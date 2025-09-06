use core::fmt;
use std::cmp::Ordering;
use std::io::BufRead;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::{error::Error, fs::File};

use hashbrown::HashMap;

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

    map.read_bytes_into_map()?;

    map.print_map()?;

    Ok(())
}

struct StationMap {
    path: PathBuf,
    map: Arc<Mutex<HashMap<Box<str>, StationValues>>>,
}

impl StationMap {
    fn new(path: PathBuf) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            path,
            map: Arc::new(Mutex::new(HashMap::with_capacity(1024))),
        })
    }

    fn optimum_buffer(file: &File) -> Result<usize, Box<dyn Error>> {
        let len = file.metadata()?.len();
        let count = std::thread::available_parallelism()?.get();
        let buf_capacity = len as usize / (count * 512);

        Ok(buf_capacity)
    }

    fn read_bytes_into_map(&self) -> Result<(), Box<dyn Error>> {
        let file = File::open(&self.path)?;
        let optimum_buffer_size = Self::optimum_buffer(&file)?;
        let mut reader = BufReader::with_capacity(optimum_buffer_size, file);

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

            let map_clone = self.map.clone();

            rayon::spawn(move || {
                let iter = std::str::from_utf8(&bytes_buffer)
                    .expect("Input bytes are not valid UTF8")
                    .split(|byte| byte == '\n')
                    .filter_map(|line| line.split_once(';'))
                    .filter_map(|(station, temp)| {
                        temp.parse::<f32>().ok().map(|parsed| (station, parsed))
                    });

                let mut locked = map_clone.lock().expect("Could not lock mutex");

                iter.for_each(|(station, float)| match locked.get_mut(station) {
                    Some(value) => {
                        value.update(float);
                    }
                    None => unsafe {
                        locked.insert_unique_unchecked(
                            Box::from(station),
                            StationValues::from(float),
                        );
                    },
                });
            });
        }

        Ok(())
    }

    fn print_map(self) -> Result<(), Box<dyn Error>> {
        let locked = self.map.lock().expect("Could not lock mutex");

        let last: usize = locked.len() - 1;

        print!("{}", "{");

        locked.iter().enumerate().for_each(|(idx, (key, value))| {
            if idx == 0 {
                return print!("\n\t{}={}\n", key, value);
            }

            if idx == last {
                return print!("\t{}={}\n", key, value);
            }

            print!("\t{}={},\n", key, value)
        });

        println!("{}", "}");

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
        if let Some(Ordering::Less) = f32::partial_cmp(&self.max, &new_value) {
            self.max = new_value;
        }

        if let Some(Ordering::Greater) = f32::partial_cmp(&self.min, &new_value) {
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
