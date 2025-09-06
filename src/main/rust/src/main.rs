use core::fmt;
use std::cmp::Ordering;
use std::{error::Error, fs::File};

use dashmap::DashMap as HashMap;
use memmap2::Mmap;
use memmap2::MmapOptions;
use rayon::prelude::*;

fn main() {
    if let Err(err) = try_main() {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn try_main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args().skip(1).next().unwrap_or_else(|| {
        "/Users/rswinford/Programming/1brc.data/measurements-1000000000.txt".to_owned()
    });

    let map = StationMap::new(&path)?;

    map.read_bytes_into_map()?;

    map.print_map()?;

    Ok(())
}

struct StationMap {
    bytes: Mmap,
    map: HashMap<Box<str>, StationValues>,
}

impl StationMap {
    fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        Ok(Self {
            bytes: mmap,
            map: HashMap::with_capacity(1024),
        })
    }

    fn read_bytes_into_map(&self) -> Result<(), Box<dyn Error>> {
        unsafe { std::str::from_utf8_unchecked(&self.bytes) }
            .par_lines()
            .filter_map(|line| line.split_once(';'))
            .filter_map(|(station, temp)| temp.parse::<f32>().ok().map(|parsed| (station, parsed)))
            .for_each(|(station, float)| {
                self.insert_into_map(station, float);
            });

        self.map.shrink_to_fit();

        Ok(())
    }

    fn insert_into_map(&self, station: &str, temp: f32) {
        match self.map.get_mut(station) {
            Some(mut value) => {
                value.update(temp);
            }
            None => {
                self.map
                    .insert(Box::from(station), StationValues::from(temp));
            }
        }
    }

    fn print_map(self) -> Result<(), Box<dyn Error>> {
        let last: usize = self.map.len() - 1;

        print!("{}", "{");

        self.map
            .into_iter()
            .enumerate()
            .for_each(|(idx, (station, value))| {
                if idx == 0 {
                    return print!("\n\t{}={}\n", station, value);
                }

                if idx == last {
                    return print!("\t{}={}\n", station, value);
                }

                print!("\t{}={},\n", station, value)
            });

        println!("{}", "}");

        Ok(())
    }
}

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
        write!(f, "{}/{}/{}", self.min, self.mean(), self.max)
    }
}
