use core::fmt;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::TryLockError;
use std::thread::sleep;
use std::time::Duration;
use std::{error::Error, fs::File};

use hashbrown::HashMap;
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

    let mut station_map = StationMap::new(path)?;

    rayon::scope(|scope| {
        station_map
            .read_bytes_into_map(scope)
            .unwrap_or_else(|err| {
                eprintln!("{}", err);
                std::process::exit(1);
            });
    });

    station_map.print_map()?;

    Ok(())
}

struct StationMap {
    path: PathBuf,
    map: Arc<Mutex<HashMap<Box<str>, StationValues>>>,
    queue: Arc<Mutex<Vec<HashMap<Box<str>, StationValues>>>>,
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
        })
    }

    fn read_bytes_into_map<'a>(&mut self, scope: &Scope) -> Result<(), Box<dyn Error>> {
        let file = File::open(&self.path)?;
        static BUFFER_SIZE: usize = 2_097_152;

        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);

        loop {
            let mut bytes_buffer: Vec<u8> = reader.fill_buf()?.to_vec();
            reader.consume(bytes_buffer.len());
            reader.read_until(b'\n', &mut bytes_buffer)?;

            if bytes_buffer.is_empty() {
                break;
            }

            self.spawn_bytes_worker(bytes_buffer, scope);
            self.spawn_queue_reader(scope);
        }

        let mut queue_locked = self.queue.lock().unwrap();
        let taken = std::mem::take(&mut *queue_locked);

        let mut map_locked = self.map.lock().unwrap();

        taken
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
            let mut lock_failures = 0u64;
            let mut local_map: HashMap<Box<str>, StationValues> = HashMap::new();

            unsafe { std::str::from_utf8_unchecked(&bytes_buffer) }
                .lines()
                .filter_map(|line| line.split_once(';'))
                .filter_map(|(station, temp)| {
                    temp.parse::<f32>().ok().map(|parsed| (station, parsed))
                })
                .for_each(
                    |(station_name, temp_float)| match local_map.get_mut(station_name) {
                        Some(value) => {
                            value.update(temp_float);
                        }
                        None => unsafe {
                            local_map.insert_unique_unchecked(
                                Box::from(station_name),
                                StationValues::from(temp_float),
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
                                sleep(Duration::from_millis(lock_failures));
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

        scope.spawn(move |_| {
            let mut lock_failures = 0u64;

            loop {
                let ready = match queue_clone.try_lock() {
                    Ok(mut queue_locked) => std::mem::take(&mut *queue_locked),
                    Err(err) => {
                        lock_failures += 1;

                        match err {
                            TryLockError::Poisoned(_) => panic!("Thread poisoned!"),
                            TryLockError::WouldBlock => {
                                sleep(Duration::from_millis(lock_failures));
                                continue;
                            }
                        }
                    }
                };

                match map_clone.try_lock() {
                    Ok(mut map_locked) => {
                        ready.into_iter().flatten().for_each(|(k, v)| {
                            match map_locked.get_mut(&k) {
                                Some(value) => {
                                    value.merge(&v);
                                }
                                None => unsafe {
                                    map_locked.insert_unique_unchecked(k, v);
                                },
                            }
                        });

                        break;
                    }
                    Err(err) => {
                        lock_failures += 1;

                        match err {
                            TryLockError::Poisoned(_) => panic!("Thread poisoned!"),
                            TryLockError::WouldBlock => {
                                sleep(Duration::from_millis(lock_failures));
                                continue;
                            }
                        }
                    }
                };
            }
        });
    }

    fn print_map(self) -> Result<(), Box<dyn Error>> {
        let out = std::io::stdout();
        let out_locked = out.lock();
        let mut output_buf = BufWriter::new(out_locked);
        let mut locked = self.map.lock().unwrap();
        let taken = std::mem::take(&mut *locked);

        let mut sorted: Vec<_> = taken.into_iter().collect();
        sorted.par_sort_unstable_by_key(|(k, _v)| k.clone());

        {
            write!(&mut output_buf, "{{")?;

            let opt_last = sorted.pop();

            sorted
                .into_iter()
                .try_for_each(|(key, value)| write!(&mut output_buf, "{}={}, ", key, value))?;

            if let Some(last) = opt_last {
                write!(&mut output_buf, "{}={}", last.0, last.1)?;
            }

            writeln!(&mut output_buf, "}}")?;

            output_buf.flush()?;
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct StationValues {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

impl From<f32> for StationValues {
    fn from(initial_value: f32) -> Self {
        Self {
            min: initial_value,
            max: initial_value,
            sum: initial_value,
            count: 1,
        }
    }
}

impl StationValues {
    fn update(&mut self, new_value: f32) {
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
        self.sum / (self.count as f32)
    }
}

impl fmt::Display for StationValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.mean(), self.max)
    }
}
