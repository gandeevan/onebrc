use std::{cmp::{min}, collections::HashMap, fs::{self, File}, hash::Hash, sync::Arc, thread::{self, JoinHandle}, time::Instant};

use crate::utils::{Stat, LINE_COUNT, THREAD_COUNT, print_result_hashmap};

// Attempt 2 - Parallelized Stat Computation with Multi-Threading
// Drawbacks:
// 1. Single-thread file reading: The entire file is initially read into memory by one thread,
//    leading to a potential bottleneck.
// 2. Inefficient looping: Each thread processes all 1 Billion lines, but computes stats only for
//    a fraction (1/THREAD_COUNT). This results in unnecessary busy looping.
// 3. Suboptimal parsing and hashing: The default methods used for hashing and float-to-string
//    parsing may not be the most efficient choices.
// 4. Performance: Execution time ranges from approximately 50 to 60 seconds.
fn process_file_part<'a>(thread_id: usize, contents: Arc<String>) -> HashMap<String, Stat> {
    let mut table: HashMap<String, Stat> = HashMap::new();
    let lines_per_thread = LINE_COUNT.div_ceil(THREAD_COUNT);
    let start_idx = thread_id * lines_per_thread;
    let end_idx = min(start_idx+lines_per_thread, LINE_COUNT);

    for (idx, line) in  contents.lines().enumerate() {
        if idx < start_idx {
            continue;
        }

        if idx >= end_idx {
            break;
        }
        
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
    table
}

fn read_file(filepath: &str) -> String {
    let t1: Instant = Instant::now();
    let contents = fs::read_to_string(filepath).unwrap();
    let t2: Instant = Instant::now();
    println!("Time taken to read the file: {} milliseconds", (t2-t1).as_millis());
    contents
}

fn compute(thread_count: usize, contents: Arc<String>) -> HashMap<String, Stat> {
    let t1: Instant = Instant::now();

    let mut handles = Vec::with_capacity(thread_count);
    for thread_id in 0..thread_count {
        let thread_contents = contents.clone();
        handles.push(thread::spawn(move || {
            process_file_part(thread_id, thread_contents)
        }));
    }

    let mut result: HashMap<String, Stat> = HashMap::new(); 
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
    let t2: Instant = Instant::now();
    println!("Time taken to compute the stats: {} milliseconds", (t2-t1).as_millis());
    result
}

pub fn run() {
    let path = "data/measurements.txt";
    
    let contents = Arc::new(read_file(path));
    let result = compute(THREAD_COUNT, contents);
    print_result_hashmap(&result);
}