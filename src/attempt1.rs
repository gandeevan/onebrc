use std::{collections::HashMap, fs::{self}, time::Instant};

use crate::utils::Stat;

pub fn run() {
    let mut table: HashMap<String, Stat> = HashMap::new();

    let path = "data/measurements.txt";
    
    let t1 = Instant::now();
    let contents = fs::read_to_string(path).unwrap();
    let t2 = Instant::now();
    println!("Time taken to read the file: {} milliseconds", (t2-t1).as_millis());


    for line in contents.lines() {
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
    let t3 = Instant::now();
    println!("Time taken to compute the stats: {} milliseconds", (t3-t2).as_millis());

    crate::utils::print_result_hashmap(&table);
    let t4 = Instant::now();
    println!("Time taken to read the file: {} milliseconds", (t4-t3).as_millis());
}
