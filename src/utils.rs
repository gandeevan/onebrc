use core::fmt;
use core::hash::Hash;
use std::collections::BTreeMap;
use std::{collections::HashMap, time::Instant};
use std::hash::BuildHasherDefault;
// use gxhash;

use log::info;

pub const SIMD_WIDTH: usize = 32;
pub const LINE_COUNT: usize = 1_000_000_000;
pub const THREAD_COUNT: usize = 16;
pub const MAX_LINE_SIZE: usize = 107; //<100_BYTE_NAME><1_BYTE_SEMICOLON><6_BYTE_TEMPRATURE>, temprature is atmost 6 bytes since -99.9 >= temp <= 99.9   


#[derive(Clone, Copy, Debug, PartialEq)]
pub struct KeyedStat {
    pub station:  [u8; 100],
    pub min: f32,
    pub max: f32,
    pub sum: f32,
    pub count: f32,
    pub len: usize
}

pub struct Stat {
    pub min: f32, 
    pub max: f32, 
    pub sum: f32, 
    pub count: f32
}

pub fn print_result_hashmap<S>(table: &HashMap<String, Stat, S>) where S:  std::hash::BuildHasher  {
    let t1: Instant = Instant::now();
    let mut idx = 0;
    let size = table.len();

    let mut keys: Vec<_> = table.keys().collect();
    keys.sort();
    print!("{{");
    for key in keys {
        let stat = table.get(key).unwrap();
        if idx == size-1 {
            print!("{}:{}/{}/{:.1}", key, stat.min, stat.max, stat.sum/stat.count);
        } else {
            print!("{}:{}/{}/{:.1},", key, stat.min, stat.max, stat.sum/stat.count);
        }
        idx+=1;
    }
    print!("}}\n");
    let t2: Instant = Instant::now();
    info!("Time taken to print the results: {} milliseconds", (t2-t1).as_millis());
}

pub fn print_result_btreemap_stat(table: &BTreeMap<String, Stat>)  {
    let start: Instant = Instant::now();
    let mut idx = 0;
    let size = table.len();

    print!("{{");
    for (key, value) in table {
        let stat = table.get(key).unwrap();
        if idx == size-1 {
            print!("{}:{}/{}/{:.1}", *key, stat.min, stat.max, stat.sum/stat.count);
        } else {
            print!("{}:{}/{}/{:.1},", *key, stat.min, stat.max, stat.sum/stat.count);
        }
        idx+=1;
    }
    print!("}}\n");
    let end: Instant = Instant::now();
    info!("Time taken to print the results: {} milliseconds", (end-start).as_millis());
}

pub fn print_result_btreemap_kstat(table: &BTreeMap<String, KeyedStat>)  {
    let t1: Instant = Instant::now();
    let mut idx = 0;
    let size = table.len();

    print!("{{");
    for (key, value) in table {
        let stat = table.get(key).unwrap();
        if idx == size-1 {
            print!("{}:{}/{}/{:.1}", *key, stat.min, stat.max, stat.sum/stat.count);
        } else {
            print!("{}:{}/{}/{:.1},", *key, stat.min, stat.max, stat.sum/stat.count);
        }
        idx+=1;
    }
    print!("}}\n");
    let t2: Instant = Instant::now();
    info!("Time taken to print the results: {} milliseconds", (t2-t1).as_millis());
}