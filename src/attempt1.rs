use std::{collections::HashMap, fs::{self}, ptr, time::Instant};
use std::collections::BTreeMap;
use log::{debug, info};

use crate::utils::{KeyedStat, Stat};

fn read_file(path: &str) -> String {
    let start = Instant::now();
    let contents = fs::read_to_string(path).unwrap();
    let end = Instant::now();
    debug!("Time taken to read the file: {} milliseconds", (end-start).as_millis());
    contents
}

fn compute_to_hashmap(contents: String) -> HashMap<String, Stat> {
    let start = Instant::now();
    let mut table: HashMap<String, Stat> = HashMap::new();

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
    let end = Instant::now();
    debug!("Time taken to compute the stats: {} milliseconds", (end-start).as_millis());
    table
}

fn compute_to_btree_stat(contents: String) -> BTreeMap<String, Stat> {
    let start = Instant::now();
    let mut table: BTreeMap<String, Stat> = BTreeMap::new();

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
    let end = Instant::now();
    debug!("Time taken to compute the stats: {} milliseconds", (end-start).as_millis());
    table
}


fn compute_to_btree_kstat(contents: String) -> BTreeMap<String, KeyedStat> {
    let start = Instant::now();
    let mut table: BTreeMap<String, KeyedStat> = BTreeMap::new();

    for line in contents.lines() {
        if let Some((station, stemp)) = line.split_once(';') {
            let temp: f32 = stemp.parse().unwrap();
            let maybe_stat = table.get_mut(station);
            match maybe_stat {
                None => {
                    let mut ks = KeyedStat {
                        min: temp,
                        max: temp,
                        count: 1.0,
                        sum: temp,
                        station: [0; 100],
                        len: station.len(),
                    };
                    unsafe {
                        ptr::copy_nonoverlapping(station.as_ptr(), ks.station.as_mut_ptr(), station.len());
                    }
                    table.insert(String::from(station), ks);
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
    let end = Instant::now();
    debug!("Time taken to compute the stats: {} milliseconds", (end-start).as_millis());
    table
}



pub fn naive_hashmap(path: &str) -> HashMap<String, Stat> {
    let contents = read_file(path);
    let table = compute_to_hashmap(contents);
    crate::utils::print_result_hashmap(&table);
    table
}

pub fn naive_btree_stat(path: &str) -> BTreeMap<String, Stat> {
    let contents = read_file(path);
    let table = compute_to_btree_stat(contents);
    crate::utils::print_result_btreemap_stat(&table);
    table
}

pub fn naive_btree_kstat(path: &str) -> BTreeMap<String, KeyedStat> {
    let contents = read_file(path);
    let table = compute_to_btree_kstat(contents);
    crate::utils::print_result_btreemap_kstat(&table);
    table
}
