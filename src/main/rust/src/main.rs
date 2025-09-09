use core::fmt;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::ops::Rem;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::TryLockError;
use std::sync::atomic::AtomicBool;
use std::thread::sleep;
use std::time::Duration;
use std::{error::Error, fs::File};

use hashbrown::HashMap;
use rayon::Scope;
use rayon::prelude::ParallelSliceMut;
use std::sync::atomic::Ordering;

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
        station_map
            .read_bytes_into_map(scope)
            .unwrap_or_else(|err| {
                eprintln!("{}", err);
                std::process::exit(1);
            });
    });

    station_map.read_queue_remainder()?;

    station_map.print_map()?;

    Ok(())
}

struct StationMap {
    path: PathBuf,
    map: Arc<Mutex<HashMap<Box<str>, StationValues>>>,
    queue: Arc<Mutex<Vec<HashMap<Box<str>, StationValues>>>>,
    hangup: Arc<AtomicBool>,
    exclusive: Arc<AtomicBool>,
}

impl StationMap {
    fn new(path: PathBuf) -> Result<Self, Box<dyn Error>> {
        static APPROXIMATE_TOTAL_CAPACITY: usize = 512;

        Ok(Self {
            path,
            map: Arc::new(Mutex::new(HashMap::with_capacity(
                APPROXIMATE_TOTAL_CAPACITY,
            ))),
            queue: Arc::new(Mutex::new(Vec::with_capacity(APPROXIMATE_TOTAL_CAPACITY))),
            hangup: Arc::new(AtomicBool::new(false)),
            exclusive: Arc::new(AtomicBool::new(true)),
        })
    }

    fn read_bytes_into_map<'a>(&self, scope: &Scope) -> Result<(), Box<dyn Error>> {
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

            self.spawn_bytes_worker(bytes_buffer, scope);

            if iter_count.rem(128) == 0
                && total_bytes_read < near_eof
                && self.exclusive.load(Ordering::SeqCst)
            {
                self.spawn_queue_reader(scope);
            }
        }

        self.hangup.store(true, Ordering::SeqCst);

        Ok(())
    }

    fn read_queue_remainder(&self) -> Result<(), Box<dyn Error>> {
        let mut queue_locked = self.queue.lock().unwrap();
        let queue_taken = std::mem::take(&mut *queue_locked);

        let mut map_locked = self.map.lock().unwrap();

        queue_taken
            .into_iter()
            .flatten()
            .for_each(|(k, v)| match map_locked.get_mut(&k) {
                Some(value) => {
                    value.merge(&v);
                }
                None => unsafe {
                    map_locked.insert_unique_unchecked(k, v);
                },
            });

        Ok(())
    }

    fn spawn_bytes_worker(&self, bytes_buffer: Vec<u8>, scope: &Scope) {
        let queue_clone = self.queue.clone();

        scope.spawn(move |_| {
            let mut lock_failures = 0u32;
            let mut local_map: HashMap<Box<str>, StationValues> = HashMap::new();

            unsafe { std::str::from_utf8_unchecked(&bytes_buffer) }
                .lines()
                .filter_map(|line| line.split_once(';'))
                .filter_map(|(station, temp)| {
                    temp.parse::<f32>()
                        .ok()
                        .map(|float| float * 10.0)
                        .map(|parsed| (station, parsed as i32))
                })
                .for_each(
                    |(station_name, temp_int)| match local_map.get_mut(station_name) {
                        Some(value) => {
                            value.update(temp_int);
                        }
                        None => unsafe {
                            local_map.insert_unique_unchecked(
                                Box::from(station_name),
                                StationValues::from(temp_int),
                            );
                        },
                    },
                );

            loop {
                match queue_clone.try_lock() {
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

    fn spawn_queue_reader(&self, scope: &Scope) {
        let queue_clone = self.queue.clone();
        let map_clone = self.map.clone();
        let hangup_clone = self.hangup.clone();
        let exclusive_clone = self.exclusive.clone();

        self.exclusive.store(false, Ordering::SeqCst);

        scope.spawn(move |_| {
            if hangup_clone.load(Ordering::SeqCst) {
                return;
            }

            let ready = match queue_clone.lock() {
                Ok(mut queue_locked) => std::mem::take(&mut *queue_locked),
                Err(_err) => {
                    panic!("Thread poisoned!")
                }
            };

            match map_clone.lock() {
                Ok(mut map_locked) => {
                    ready
                        .into_iter()
                        .flatten()
                        .for_each(|(k, v)| match map_locked.get_mut(&k) {
                            Some(value) => {
                                value.merge(&v);
                            }
                            None => unsafe {
                                map_locked.insert_unique_unchecked(k, v);
                            },
                        });
                }
                Err(_err) => {
                    panic!("Thread poisoned!")
                }
            };

            exclusive_clone.store(true, Ordering::SeqCst);
        });
    }

    fn print_map(self) -> Result<(), Box<dyn Error>> {
        let out = std::io::stdout();
        let out_locked = out.lock();
        let mut output_buf = BufWriter::new(out_locked);
        let mut map_locked = self.map.lock().unwrap();
        let map_taken = std::mem::take(&mut *map_locked);

        let mut sorted: Vec<_> = map_taken.into_iter().collect();
        sorted.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

        {
            write!(&mut output_buf, "{{")?;

            let opt_last = sorted.pop();

            sorted
                .into_iter()
                .try_for_each(|(key, value)| write!(&mut output_buf, "{}={}, ", key, value))?;

            if let Some((key, value)) = opt_last {
                write!(&mut output_buf, "{}={}", key, value)?;
            }

            writeln!(&mut output_buf, "}}")?;

            output_buf.flush()?;
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct StationValues {
    min: i32,
    max: i32,
    sum: i32,
    count: u32,
}

impl From<i32> for StationValues {
    fn from(initial_value: i32) -> Self {
        Self {
            min: initial_value,
            max: initial_value,
            sum: initial_value,
            count: 1,
        }
    }
}

impl StationValues {
    fn update(&mut self, new_value: i32) {
        if self.max.lt(&new_value) {
            self.max = new_value;
        }

        if self.min.gt(&new_value) {
            self.min = new_value
        }

        self.sum += new_value;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        if self.max.lt(&other.max) {
            self.max = other.max;
        }

        if self.min.gt(&other.min) {
            self.min = other.min;
        }

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

impl fmt::Display for StationValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min(), self.mean(), self.max())
    }
}
