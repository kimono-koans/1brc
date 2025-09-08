use core::fmt;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::Scope;
use std::{error::Error, fs::File};

use hashbrown::HashMap;
use memmap2::MmapOptions;

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

    std::thread::scope(|scope| {
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
}

impl StationMap {
    fn new(path: PathBuf) -> Result<Self, Box<dyn Error>> {
        static APPROXIMATE_TOTAL_CAPACITY: usize = 512;

        Ok(Self {
            path,
            map: Arc::new(Mutex::new(HashMap::with_capacity(
                APPROXIMATE_TOTAL_CAPACITY,
            ))),
        })
    }

    fn read_bytes_into_map<'a>(&mut self, scope: &'a Scope<'a, '_>) -> Result<(), Box<dyn Error>> {
        let file = File::open(&self.path)?;
        let len = file.metadata()?.len();
        static BUFFER_SIZE: u64 = 67108864u64;
        let mut start_offset = 0u64;

        let mmap = unsafe { MmapOptions::new().populate().map(&file)? };

        let mut handles = Vec::new();

        loop {
            let mut end_offset = start_offset + BUFFER_SIZE;

            if end_offset.gt(&len) {
                end_offset = len;
            } else if end_offset.lt(&len) {
                if let Some(pos) = mmap[end_offset as usize..]
                    .iter()
                    .position(|byte| byte == &b'\n')
                {
                    end_offset += pos as u64;
                }
            }

            let bytes_buffer = mmap[start_offset as usize..end_offset as usize].to_vec();

            if bytes_buffer.is_empty() {
                break;
            }

            let handle = scope.spawn(move || {
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
                            None => {
                                let _ = local_map.insert(
                                    Box::from(station_name),
                                    StationValues::from(temp_float),
                                );
                            }
                        },
                    );

                local_map
            });

            handles.push(handle);

            let finished: Vec<_> = handles
                .extract_if(.., |item| item.is_finished())
                .map(|item| item.join().ok())
                .flatten()
                .flatten()
                .collect();

            if !finished.is_empty() {
                let map_clone = self.map.clone();

                scope.spawn(move || {
                    let mut locked = map_clone.lock().unwrap();

                    finished
                        .into_iter()
                        .for_each(|(name, values)| match locked.get_mut(&name) {
                            Some(value) => {
                                value.merge(&values);
                            }
                            None => unsafe {
                                locked.insert_unique_unchecked(name, values);
                            },
                        });
                });
            }

            start_offset = end_offset;
        }

        let mut locked = self.map.lock().unwrap();

        handles
            .into_iter()
            .filter_map(|item| item.join().ok())
            .flatten()
            .for_each(|(name, values)| match locked.get_mut(&name) {
                Some(value) => {
                    value.merge(&values);
                }
                None => unsafe {
                    locked.insert_unique_unchecked(name, values);
                },
            });

        Ok(())
    }

    fn print_map(self) -> Result<(), Box<dyn Error>> {
        let out = std::io::stdout();
        let out_locked = out.lock();
        let mut output_buf = BufWriter::new(out_locked);
        let locked = self.map.lock().unwrap();

        let mut sorted: Vec<_> = locked.iter().collect();
        sorted.sort_unstable_by_key(|(k, _v)| *k);

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
