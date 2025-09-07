use core::fmt;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::{error::Error, fs::File};

use dashmap::DashMap as HashMap;
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
        map.read_bytes_into_map(scope).unwrap();
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

    fn optimum_buffer(file: &File) -> Result<usize, Box<dyn Error>> {
        let len = file.metadata()?.len();
        let count = std::thread::available_parallelism()?.get();
        let buf_capacity = len as usize / (count * 512);

        Ok(buf_capacity)
    }

    fn read_bytes_into_map<'a>(&'a self, scope: &Scope<'a>) -> Result<(), Box<dyn Error>> {
        let file = File::open(&self.path)?;
        let optimum_buffer_size = Self::optimum_buffer(&file)?;
        let mut reader: BufReader<File> = BufReader::with_capacity(optimum_buffer_size, file);

        loop {
            // first, read lots of bytes into the buffer
            let mut bytes_buffer = reader.fill_buf()?.to_vec();
            reader.consume(bytes_buffer.len());

            // now, keep reading to make sure we haven't stopped in the middle of a word.
            // no need to add the bytes to the total buf_len, as these bytes are auto-"consumed()",
            // and bytes_buffer will be extended from slice to accommodate the new bytes
            reader.read_until(b'\n', &mut bytes_buffer)?;

            // break when there is nothing left to read
            if bytes_buffer.is_empty() {
                break;
            }

            scope.spawn(move |_| {
                std::str::from_utf8(&bytes_buffer)
                    .expect("Input bytes are not valid UTF8")
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
            });
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
