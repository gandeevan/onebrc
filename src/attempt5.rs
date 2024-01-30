use core::num;
use std::{collections::BTreeMap, fs::{self, File}, io::{Read, Seek, SeekFrom}, ptr, thread::{self}, time::Instant};

use crate::utils::{print_result_btreemap, print_result_hashmap, KeyedStat, Stat, MAX_LINE_SIZE, THREAD_COUNT};
use log::{debug, info};
use std::str;

// Attempt 5
// Commentary about attempt 4:
// 1. Even with a faster hash function, 30% (6s) of the total runtime can be attributed to hash table reads (get_mut).
//    3 out of these 6s is goes towards the hash computation and the other 3 towards finding the approprtiate slot in the hash table.
// 2. We are iterating over line twice in the compute method, once in the lines() method and another time during the hash computation
//    float parsing.

struct LPTable {
    num_slots: usize,
    min_slot_size: usize,
    table: Vec<Vec<KeyedStat>>,
    size: u32,
    occupied_slots: Vec<usize>
}


impl LPTable {
    fn new(num_slots: usize, min_slot_size: usize) -> LPTable {
        LPTable {
            num_slots,
            min_slot_size,
            table: vec![Vec::with_capacity(min_slot_size); num_slots], 
            size: 0,
            occupied_slots: Vec::with_capacity(num_slots)
        }
    }

    fn get_collision_count(&self) -> usize {
        let mut m = 0;
        let mut count = 0;
        for i in 0..self.num_slots {
            if self.table[i].len() > 1 {
                count += self.table[i].len();
            }
            m = std::cmp::max(self.table[i].len(), m);
        }
        println!("Max collisions = {}", m);
        count
    }

    fn insert_or_update(&mut self, station: &[u8; 100], len: usize, hash: usize, temp: f32) {
        let slot = hash & (self.num_slots-1);
        debug!("station={}, hash={}, temp={}", str::from_utf8(&station[0..len]).unwrap(), hash, temp);
       
        for ks in &mut self.table[slot] {
            // TODO: don't need to compare all the 100 bytes
            if &ks.station != station {
                assert_ne!(station[0..len], ks.station[0..ks.len]);
                debug!("Collision for {} and {}", str::from_utf8(&station[0..len]).unwrap(), str::from_utf8(&ks.station[0..ks.len]).unwrap());
                continue;
            }
            // println!("Found multiple entries for {}", str::from_utf8(&station[0..len]).unwrap());
            ks.sum += temp;
            ks.count += 1.0;
            ks.min = temp.min(ks.min);
            ks.max = temp.max(ks.max);
            return;
        }
        let mut ks = KeyedStat{
            min: temp,
            max: temp,
            count: 1.0,
            len: len,
            sum: temp,
            station: [0;100],
        };
        unsafe {
            ptr::copy_nonoverlapping(station.as_ptr(), ks.station.as_mut_ptr(), ks.station.len());
        }
        if self.table[slot].is_empty() {
            self.occupied_slots.push(slot);
        }
        self.table[slot].push(ks);
        self.size+=1;
    }

}

#[inline(always)]
fn reset_temp_vars(temp_int_part: &mut u8, temp_fraction_part: &mut u8, temp_multiplier: &mut f32, parsing_int_part: &mut bool) {
    *temp_int_part = 0;
    *temp_fraction_part = 0;
    *temp_multiplier = 1.0;
    *parsing_int_part = true;
}

#[inline(always)]
fn reset_station_vars(station: &mut [u8; 100], station_idx: &mut usize, hash: &mut usize, parsing_name: &mut bool) {
    *station = [0; 100];
    *station_idx = 0;
    *hash = 5381;
    *parsing_name = true;
}

#[inline(always)]
fn update_station(station: &mut [u8; 100], station_idx: &mut usize, hash: &mut usize, byte: u8) {
    station[*station_idx] = byte;
    unsafe {
        *hash = ((*hash << 5) + *hash) + byte as usize; // DJB2 hash
    }
    *station_idx += 1;
}

#[inline(always)]
fn update_temp_vars(byte: u8, temp_int_part: &mut u8, temp_fraction_part: &mut u8, parsing_int_part: bool) {
    let digit = (byte - b'0');
    if parsing_int_part {
        *temp_int_part = (*temp_int_part * 10) + digit;
    } else {
        *temp_fraction_part = (*temp_fraction_part * 10) + digit;
    }
}

fn compute(contents: &str, thread_id: usize) -> LPTable {
    let start_time = Instant::now();
    let mut table = LPTable::new(130712, 4);
    let mut row_count=0;

    let mut temp;
    let mut hash: usize = 5381;
    let mut station: [u8; 100] = [0; 100];
    let mut station_idx = 0;
    let mut parsing_name = true;

    let mut temp_int_part: u8 = 0;
    let mut temp_fraction_part: u8 = 0;
    let mut temp_multiplier: f32 = 1.0;
    let mut parsing_int_part = true;

    for &byte in contents.as_bytes() {
        match byte {
            b'\n' => {
                temp = f32::try_from(temp_int_part).unwrap() + f32::from(temp_fraction_part) / 10.0;
                table.insert_or_update(&station, station_idx, hash, temp_multiplier * temp);
                row_count+=1;    
                reset_temp_vars(&mut temp_int_part, &mut temp_fraction_part, &mut temp_multiplier, &mut parsing_int_part);
                reset_station_vars(&mut station, &mut station_idx, &mut hash, &mut parsing_name);
            },
            b';' => parsing_name = false,
            _ if parsing_name => update_station(&mut station, &mut station_idx, &mut hash, byte),
            b'-' => temp_multiplier = -1.0,
            b'.' => parsing_int_part = false,
            _ => update_temp_vars(byte, &mut temp_int_part, &mut temp_fraction_part, parsing_int_part),
        }
    }

    let end_time = Instant::now();
    debug!("Processed n row = {}", row_count);
    debug!("Distinct keys = {}", table.size);
    debug!("Collision Count = {}", table.get_collision_count());
    info!("Time taken to compute the stats: {} milliseconds", (end_time - start_time).as_millis());
    
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
    info!("Time taken to read the file: {} milliseconds", (t2-t1).as_millis());

    unsafe {
        String::from_utf8_unchecked(buf)
    }
}

fn thread_run(thread_id: usize, filepath: &str, start_offset: usize, size: usize) -> LPTable {
    let mut contents = read_file(filepath, start_offset, size);

    let mut start_idx = 0;
    if thread_id != 0 {
        start_idx = contents.find('\n').unwrap() + 1;
    }

    let end_idx = contents.rfind('\n').unwrap();
    compute(&contents[start_idx..=end_idx], thread_id)
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

    let mut result: BTreeMap<String, KeyedStat> = BTreeMap::new();
    for handle in handles {
        let lptable = handle.join().unwrap();
        for slot in lptable.occupied_slots {
            for ks in &lptable.table[slot] {
                let key: &str;
                unsafe {
                    key = str::from_utf8_unchecked(&ks.station[0..ks.len]);
                }
                let maybe_stat = result.get_mut(key);
                match maybe_stat {
                    None => {
                        result.insert(String::from(key), ks.clone());
                    },
                    Some(stat) => {
                        stat.sum += ks.sum;
                        stat.count += ks.count;
                        stat.min = stat.min.min(ks.min);
                        stat.max = stat.max.max(ks.max);
                    }
                }

            }
        }
    }

    print_result_btreemap(&result);
}