use std::{cmp, collections::HashMap, env, fmt::Display, fs, thread};

use bstr::{BStr, ByteSlice};
use memchr::memchr;
use memmap2::Mmap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

const INP_PATH: &str = "data.txt";

/// Reads input and uses map/reduce pattern to compute the result. Input is
/// memory mapped to reduce copying. For more detailed justication of
/// dependencies please see comments in `Cargo.toml`.
fn main() {
    // Prepare data
    let args = env::args().nth(1);
    let path = args.unwrap_or(INP_PATH.to_string());
    let file = fs::File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    // Map
    let maps: Vec<_> = partition(&mmap)
        .par_iter()
        .map(|(start, end)| compute_part((*start, *end), &mmap))
        .collect();

    // Reduce
    let mut answer = HashMap::new();
    for m in maps {
        reduce(&mut answer, &m);
    }

    // Print
    print_result(&answer);
}

/// Use the number of CPUs to scan and partition the input data.
fn partition(data: &[u8]) -> Vec<(usize, usize)> {
    let cpus = thread::available_parallelism().unwrap().get();
    let size = data.len() / cpus;

    let mut parts = vec![];
    let mut start = 0;

    for _ in 0..cpus {
        let end = cmp::min(start + size, data.len() - 1);
        let end = match memchr(b'\n', &data[end..]) {
            Some(n) => end + n,
            None => end,
        };

        parts.push((start, end));
        start = end + 1;
    }

    parts
}

/// Scan chunk of input and record stats (i.e. map operation).
fn compute_part((start, end): (usize, usize), data: &[u8]) -> HashMap<&BStr, Stats> {
    compute(&mut data[start..=end].lines())
}

fn compute<'a>(itr: impl Iterator<Item = &'a [u8]>) -> HashMap<&'a BStr, Stats> {
    let mut map: HashMap<&'a BStr, Stats> = HashMap::new();

    for line in itr {
        let (name, temp) = line.split_once_str(";").unwrap();
        let temp = fast_float::parse(temp).unwrap();
        map.entry(name.into()).or_default().add(temp);
    }

    map
}

/// Merge the second map into the first (i.e. reduce operation).
fn reduce<'a>(a: &mut HashMap<&'a BStr, Stats>, b: &HashMap<&'a BStr, Stats>) {
    for (k, v) in b {
        a.entry(k).or_default().fold(v);
    }
}

fn print_result(answer: &HashMap<&BStr, Stats>) {
    let mut names: Vec<_> = answer.keys().collect();
    names.sort();
    let mut first = true;

    print!("{{");
    for n in names {
        if first {
            print!("{n}={}", answer[n]);
            first = false;
        } else {
            print!(", {n}={}", answer[n]);
        }
    }
    println!("}}");
}

struct Stats {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            min: f64::MAX,
            max: f64::MIN,
            sum: 0.0,
            count: 0,
        }
    }
}

impl Stats {
    fn add(&mut self, n: f64) {
        if n < self.min {
            self.min = n
        }
        if n > self.max {
            self.max = n
        }
        self.sum += n;
        self.count += 1;
    }

    fn fold(&mut self, s: &Stats) {
        if s.min < self.min {
            self.min = s.min
        }
        if s.max > self.max {
            self.max = s.max
        }
        self.sum += s.sum;
        self.count += s.count;
    }

    fn avg(&self) -> f64 {
        self.sum / self.count as f64
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}/{:.1}/{:.1}", self.min, self.avg(), self.max)
    }
}
