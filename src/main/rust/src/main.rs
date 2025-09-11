#![feature(int_from_ascii, slice_split_once)]
use core::fmt;
use std::hash::BuildHasherDefault;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::num::ParseIntError;
use std::ops::Rem;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::TryLockError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::sleep;
use std::time::Duration;
use std::{error::Error, fs::File};

use hashbrown::HashMap;
use nohash::NoHashHasher;
use rayon::Scope;
use rayon::prelude::ParallelSliceMut;

fn main() {
    if let Err(err) = try_main() {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn try_main() -> Result<(), Box<dyn Error>> {
    let home = std::env::home_dir().expect("Could not determine HOME env var");

    let path = std::env::args()
        .skip(1)
        .next()
        .map(|arg| PathBuf::from(arg))
        .unwrap_or_else(|| home.join("Programming/1brc.data/measurements-1000000000.txt"));

    let station_map = StationMap::new(path)?;

    rayon::in_place_scope(|scope| {
        station_map.exec(scope).unwrap_or_else(|err| {
            eprintln!("{}", err);
            std::process::exit(1);
        });
    });

    station_map.read_queue_to_map();

    station_map.print_map()?;

    Ok(())
}

struct StationMap {
    path: PathBuf,
    map: Mutex<HashMap<u64, Record, BuildHasherDefault<NoHashHasher<u64>>>>,
    queue: Mutex<Vec<HashMap<u64, Record, BuildHasherDefault<NoHashHasher<u64>>>>>,
    hangup: AtomicBool,
    exclusive: AtomicBool,
}

impl StationMap {
    fn new(path: PathBuf) -> Result<Arc<Self>, Box<dyn Error>> {
        static APPROXIMATE_TOTAL_CAPACITY: usize = 512;

        Ok(Arc::new(Self {
            path,
            map: Mutex::new(HashMap::with_capacity_and_hasher(
                APPROXIMATE_TOTAL_CAPACITY,
                nohash::BuildNoHashHasher::new(),
            )),
            queue: Mutex::new(Vec::with_capacity(APPROXIMATE_TOTAL_CAPACITY)),
            hangup: AtomicBool::new(false),
            exclusive: AtomicBool::new(true),
        }))
    }

    fn exec<'a>(self: &Arc<Self>, scope: &Scope) -> Result<(), Box<dyn Error>> {
        static BUFFER_SIZE: usize = 2_097_152;

        let mut iter_count = 0;
        let mut total_bytes_read = 0u64;

        let file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        let near_eof = file_len.saturating_sub(BUFFER_SIZE as u64 * 128);

        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);

        loop {
            let mut bytes_buffer: Vec<u8> = reader.fill_buf()?.to_vec();
            reader.consume(bytes_buffer.len());
            reader.read_until(b'\n', &mut bytes_buffer)?;

            total_bytes_read += bytes_buffer.len() as u64;
            iter_count += 1;

            if bytes_buffer.is_empty() {
                break;
            }

            Self::spawn_buffer_worker(self.clone(), bytes_buffer, scope);

            if iter_count.rem(128) == 0
                && total_bytes_read < near_eof
                && self.exclusive.load(Ordering::SeqCst)
            {
                Self::spawn_queue_worker(self.clone(), scope);
            }
        }

        self.hangup.store(true, Ordering::SeqCst);

        Ok(())
    }

    fn spawn_buffer_worker(self: Arc<Self>, bytes_buffer: Vec<u8>, scope: &Scope) {
        scope.spawn(move |_| {
            let mut lock_failures = 0u32;
            let mut local_map: HashMap<u64, Record, BuildHasherDefault<NoHashHasher<u64>>> =
                HashMap::with_hasher(nohash::BuildNoHashHasher::new());

            bytes_buffer
                .split(|byte| byte == &b'\n')
                .filter_map(|line| line.split_once(|byte| byte == &b';'))
                .filter_map(|(station, temp)| {
                    parse_i32(temp).ok().map(|parsed| (station, parsed as i32))
                })
                .for_each(|(station_name, temp_int)| {
                    let uuid = Record::uuid(station_name);

                    match local_map.get_mut(&uuid) {
                        Some(station) => {
                            station.values.update(temp_int);
                        }
                        None => unsafe {
                            let item = Record::new(station_name, temp_int);

                            local_map.insert_unique_unchecked(uuid, item);
                        },
                    }
                });

            loop {
                match self.queue.try_lock() {
                    Ok(mut locked) => {
                        locked.push(local_map);
                        break;
                    }
                    Err(err) => {
                        lock_failures += 1;

                        match err {
                            TryLockError::Poisoned(_) => panic!("Thread poisoned!"),
                            TryLockError::WouldBlock => {
                                let duration = 2u64.pow(lock_failures);
                                sleep(Duration::from_millis(duration));
                                continue;
                            }
                        }
                    }
                }
            }
        });
    }

    fn spawn_queue_worker(self: Arc<Self>, scope: &Scope) {
        self.exclusive.store(false, Ordering::SeqCst);

        scope.spawn(move |_| {
            if self.hangup.load(Ordering::SeqCst) {
                return;
            }

            self.read_queue_to_map();

            self.exclusive.store(true, Ordering::SeqCst);
        });
    }

    fn read_queue_to_map(&self) {
        let mut queue_taken = Vec::new();

        let Ok(mut queue_locked) = self.queue.lock() else {
            panic!("Thread poisoned!")
        };

        queue_taken.append(&mut *queue_locked);
        drop(queue_locked);

        let Ok(mut map_locked) = self.map.lock() else {
            panic!("Thread poisoned!")
        };

        queue_taken
            .into_iter()
            .flatten()
            .for_each(|(k, v)| match map_locked.get_mut(&k) {
                Some(station) => {
                    station.merge(&v);
                }
                None => unsafe {
                    map_locked.insert_unique_unchecked(k, v);
                },
            });
    }

    fn print_map(&self) -> Result<(), Box<dyn Error>> {
        let out = std::io::stdout();
        let mut output_buf = BufWriter::new(out);
        let Ok(map_locked) = self.map.lock() else {
            panic!("Thread poisoned!")
        };

        let mut sorted: Vec<_> = map_locked.values().collect();
        sorted.par_sort_unstable_by(|a, b| a.station_name.cmp(&b.station_name));

        {
            write!(&mut output_buf, "{{")?;

            let opt_last = sorted.pop();

            sorted.into_iter().try_for_each(|record| {
                output_buf.write_all(&record.station_name)?;
                output_buf.write_fmt(format_args!("={}, ", record.values))
            })?;

            if let Some(record) = opt_last {
                output_buf.write_all(&record.station_name)?;
                output_buf.write_fmt(format_args!("={}", record.values))?;
            }

            writeln!(&mut output_buf, "}}")?;

            output_buf.flush()?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Record {
    station_name: Box<[u8]>,
    values: StationValues,
}

impl Record {
    fn new(station_name: &[u8], initial_value: i32) -> Self {
        Self {
            station_name: station_name.into(),
            values: StationValues::new(initial_value),
        }
    }

    fn merge(&mut self, other: &Self) {
        self.values.merge(&other.values)
    }

    fn uuid(station_name: &[u8]) -> u64 {
        use foldhash::quality::FixedState;
        use std::hash::{BuildHasher, Hasher};

        let s = FixedState::default();
        let mut hash = s.build_hasher();

        hash.write(station_name);
        hash.finish()
    }
}

#[derive(Clone, Debug, Copy)]
struct StationValues {
    min: i32,
    max: i32,
    sum: i32,
    count: u32,
}

impl StationValues {
    fn new(initial_value: i32) -> Self {
        Self {
            min: initial_value,
            max: initial_value,
            sum: initial_value,
            count: 1,
        }
    }

    fn update(&mut self, new_value: i32) {
        self.max = std::cmp::max(self.max, new_value);
        self.min = std::cmp::min(self.min, new_value);
        self.sum += new_value;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.max = std::cmp::max(self.max, other.max);
        self.min = std::cmp::min(self.min, other.min);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn mean(&self) -> f32 {
        self.sum as f32 / (self.count as f32 * 10.0)
    }

    fn min(&self) -> f32 {
        self.min as f32 / 10.0
    }

    fn max(&self) -> f32 {
        self.max as f32 / 10.0
    }
}

// Parses ints values between -9999 to 9999
#[inline]
fn parse_i32(value: &[u8]) -> Result<i32, ParseIntError> {
    let mut is_negative = false;

    let range = if let Some(b'-') = value.first() {
        is_negative = true;
        &value[1..]
    } else {
        &value[..]
    };

    let out = match range {
        [h2, h1, h0, b'.', l] => i32::from_ascii(&[*h2, *h1, *h0, *l])?,
        [h1, h0, b'.', l] => i32::from_ascii(&[*h1, *h0, *l])?,
        [h0, b'.', l] => i32::from_ascii(&[*h0, *l])?,
        _ => unreachable!(),
    };

    if is_negative {
        return Ok(-out);
    }

    Ok(out)
}

impl fmt::Display for StationValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min(), self.mean(), self.max())
    }
}
