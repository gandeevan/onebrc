use std::{fs::{self, File}, io::{Read, Seek, SeekFrom}, thread::{self}, time::Instant};

use crate::utils::{Stat, MAX_LINE_SIZE, THREAD_COUNT, print_result_hashmap};
use log::info;

// Attempt 4 - Fast hashing
// Improvements:
// 1. Experimented with a faster hash function (GxHash and FxHash), instead of using the default SipHash
// 2. Performance: Reduced the runtime from around ~20seconds to ~17s.
fn compute(contents: String) -> gxhash::GxHashMap<String, Stat> {
    let t1: Instant = Instant::now();

    // let start_idx = contents.find('\n').unwrap();
    let start_idx = 0;

    let end_idx = contents.rfind('\n').unwrap();

    let mut table = gxhash::GxHashMap::default();
    for line in  contents[start_idx..end_idx+1].lines() {
        if let Some((station, stemp)) = line.split_once(';') {
            let temp: f32 = stemp.parse().unwrap();
            let maybe_stat = table.get_mut(station);
            match maybe_stat {
                None => {
                    table.insert(String::from(station), Stat {
                        min: temp,
                        max: temp,
                        count: 1.0,
                        sum: temp,
                    });
                },
                Some(stat) => {
                    stat.sum += temp;
                    stat.count += 1.0;
                    stat.min = temp.min(stat.min);
                    stat.max = temp.max(stat.max);
                }
            }
        }
    }
    let t2: Instant = Instant::now();
    println!("Distinct keys = {}", table.len());
    println!("Time taken to compute the stats: {} milliseconds", (t2-t1).as_millis());
    table
}

fn read_file(filepath: &str, start_offset: usize, size: usize) -> String {
    let t1: Instant = Instant::now();
    let mut file = File::open(filepath).unwrap();
    let curr_offset = file.seek(SeekFrom::Start(start_offset.try_into().unwrap())).unwrap();

    // TODO: get ris of the assert
    assert_eq!(curr_offset, start_offset.try_into().unwrap());

    let mut buf =  vec![0u8; size];
    file.read_exact(&mut buf).unwrap();
    let t2: Instant = Instant::now();
    println!("Time taken to read the file: {} milliseconds", (t2-t1).as_millis());

    // TODO: use the unchecked version for performance
    unsafe {
        String::from_utf8_unchecked(buf)
    }
}

fn thread_run(thread_id: usize, filepath: &str, start_offset: usize, size: usize) -> gxhash::GxHashMap<String, Stat> {
    let contents = read_file(filepath, start_offset, size);
    compute(contents)
}

pub fn run() {
    let path = "data/measurements.txt";

    let mut handles = Vec::with_capacity(THREAD_COUNT);
    let mut file_size: usize = fs::metadata(path).unwrap().len().try_into().unwrap();
    let file_size_per_thread = file_size/THREAD_COUNT;
    for thread_id in 0..THREAD_COUNT {
        let mut size = file_size_per_thread + MAX_LINE_SIZE;
        if thread_id == THREAD_COUNT -1 {
            size = file_size;
        }
        handles.push(thread::spawn(move || {
            thread_run(thread_id, path, file_size_per_thread * thread_id, size)
        }));
        file_size -= file_size_per_thread;
    }

    let mut result = gxhash::GxHashMap::default();
    for handle in handles {
        let partial_res = handle.join().unwrap();
        for (k, v) in partial_res {
            let maybe_stat = result.get_mut(&k);
            match maybe_stat {
                None => {
                    result.insert(k, v);
                },
                Some(stat) => {
                    stat.sum += v.sum;
                    stat.count += v.count;
                    stat.min = v.min.min(stat.min);
                    stat.max = v.max.max(stat.max);
                }
            }
        }
    }

    print_result_hashmap(&result);
}