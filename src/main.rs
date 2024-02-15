use std::{cmp, collections::HashMap, env, fmt::Display, fs, thread};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

const INP_PATH: &str = "data.txt";

fn main() {
    // data
    let args = env::args().skip(1).next();
    let path = args.unwrap_or(INP_PATH.to_string());
    let data = fs::read_to_string(path).unwrap();

    // map
    let maps: Vec<_> = partition(&data)
        .par_iter()
        .map(|(start, end)| compute((*start, *end), &data))
        .collect();

    // reduce
    let mut answer = HashMap::new();
    for m in maps {
        reduce(&mut answer, &m);
    }

    // print
    print_result(&answer);
}

/// Use the number of CPUs to scan and partition the input data.
fn partition(data: &str) -> Vec<(usize, usize)> {
    let cpus = thread::available_parallelism().unwrap().get();
    let size = data.len() / cpus;
    let mut parts = vec![];
    let mut start = 0;

    for _ in 0..cpus {
        let end = cmp::min(start + size, data.len() - 1);
        let end = match &data[end..].find('\n') {
            Some(n) => end + n,
            None => end,
        };

        parts.push((start, end));
        start = end + 1;
    }

    parts
}

/// Scan chunk of input and record stats (i.e. map).
fn compute((start, end): (usize, usize), data: &str) -> HashMap<&str, Stats> {
    let mut map: HashMap<&str, Stats> = HashMap::new();
    let lines = data[start..=end].lines();

    for line in lines {
        let (name, temp) = line.split_once(';').unwrap();
        let temp = temp.parse::<f64>().unwrap();
        map.entry(name).or_default().add(temp);
    }

    map
}

/// Merge the second map into the first (i.e. reduce).
fn reduce<'a>(a: &mut HashMap<&'a str, Stats>, b: &HashMap<&'a str, Stats>) {
    for (k, v) in b {
        a.entry(k).or_default().fold(v);
    }
}

fn print_result(answer: &HashMap<&str, Stats>) {
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
