use core::fmt;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::{error::Error, fs::File};

use dashmap::DashMap as HashMap;
use memmap2::MmapOptions;
use rayon::Scope;
use rayon::prelude::*;

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

    let map = StationMap::new(path)?;

    rayon::scope(|scope| {
        map.read_bytes_into_map(scope).unwrap_or_else(|err| {
            eprintln!("{}", err);
            std::process::exit(1);
        });
    });

    map.print_map()?;

    Ok(())
}

struct StationMap {
    path: PathBuf,
    map: HashMap<Box<str>, StationValues>,
}

impl StationMap {
    fn new(path: PathBuf) -> Result<Self, Box<dyn Error>> {
        static APPROXIMATE_TOTAL_CAPACITY: usize = 512;

        Ok(Self {
            path,
            map: HashMap::with_capacity(APPROXIMATE_TOTAL_CAPACITY),
        })
    }

    fn read_bytes_into_map<'a>(&'a self, scope: &Scope<'a>) -> Result<(), Box<dyn Error>> {
        let file = File::open(&self.path)?;
        let len = file.metadata()?.len();
        static BUFFER_SIZE: u64 = 2097152u64;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let mut start_offset = 0u64;

        loop {
            let mut end_offset = start_offset + BUFFER_SIZE;
            if end_offset.gt(&len) {
                end_offset = len;
            }

            let mut bytes_buffer = mmap[start_offset as usize..end_offset as usize].to_vec();

            let opt_next_newline_pos = mmap[end_offset as usize..]
                .iter()
                .position(|byte| byte == &b'\n');

            if let Some(pos) = opt_next_newline_pos {
                bytes_buffer.extend_from_slice(
                    &mmap[end_offset as usize..end_offset as usize + pos as usize],
                );
                end_offset += pos as u64;
            }

            if bytes_buffer.is_empty() {
                break;
            }

            scope.spawn(move |_| {
                unsafe { std::str::from_utf8_unchecked(&bytes_buffer) }
                    .lines()
                    .filter_map(|line| line.split_once(';'))
                    .filter_map(|(station, temp)| {
                        temp.parse::<f32>().ok().map(|parsed| (station, parsed))
                    })
                    .for_each(|(station, float)| match self.map.get_mut(station) {
                        Some(mut value) => {
                            value.update(float);
                        }
                        None => {
                            let _ = self
                                .map
                                .insert(Box::from(station), StationValues::from(float));
                        }
                    });

                bytes_buffer.clear();
            });

            start_offset = end_offset;
        }

        Ok(())
    }

    fn print_map(self) -> Result<(), Box<dyn Error>> {
        let out = std::io::stdout();
        let out_locked = out.lock();
        let mut output_buf = BufWriter::new(out_locked);

        let mut sorted: Vec<_> = self.map.into_iter().collect();
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

#[derive(Clone, Copy)]
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

    fn mean(&self) -> f32 {
        self.sum / (self.count as f32)
    }
}

impl fmt::Display for StationValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.mean(), self.max)
    }
}
