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
    let map: HashMap<Box<str>, StationValues> = HashMap::with_capacity(1024);

    let path = std::env::args().skip(1).next().unwrap_or_else(|| {
        "/Users/rswinford/Programming/1brc.data/measurements-1000000000.txt".to_owned()
    });

    let bytes = mmap_file(&path)?;

    read_into_map(&bytes, &map)?;

    print_map(map)?;

    Ok(())
}

fn mmap_file(path: &str) -> Result<Mmap, Box<dyn Error>> {
    let file = File::open(path)?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };

    Ok(mmap)
}

fn read_into_map(
    bytes: &Mmap,
    map: &HashMap<Box<str>, StationValues>,
) -> Result<(), Box<dyn Error>> {
    unsafe { std::str::from_utf8_unchecked(bytes) }
        .par_lines()
        .filter_map(|word| word.split_once(';'))
        .filter_map(|(station, temp)| temp.parse::<f32>().ok().map(|parsed| (station, parsed)))
        .for_each(|(station, float)| {
            into_map(map, station, float);
        });

    map.shrink_to_fit();

    Ok(())
}

fn into_map(map: &HashMap<Box<str>, StationValues>, station: &str, temp: f32) {
    match map.get_mut(station) {
        Some(mut value) => {
            value.update(temp);
        }
        None => {
            map.insert(Box::from(station), StationValues::from(temp));
        }
    }
}

fn print_map(map: HashMap<Box<str>, StationValues>) -> Result<(), Box<dyn Error>> {
    let last: usize = map.len() - 1;

    print!("{}", "{");

    map.into_iter()
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
