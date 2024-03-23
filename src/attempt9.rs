use core::num;
use std::{arch::asm, collections::BTreeMap, env, fs::{self, File}, io::{Read, Seek, SeekFrom}, mem, ptr, thread::{self, current, JoinHandle}, time::Instant};

use crate::{attempt1, utils::{print_result_btreemap_kstat, print_result_hashmap, KeyedStat, Stat, MAX_LINE_SIZE, THREAD_COUNT}};
use log::{debug, info};
use std::str;
use memmap2::{Mmap, MmapOptions};


struct LPTable {
    num_slots: usize,
    table: Vec<Vec<KeyedStat>>,
    size: usize,
    occupied_slots: Vec<usize>
}


impl LPTable {
    fn new(num_slots: usize, min_slot_size: usize) -> LPTable {
        LPTable {
            num_slots,
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
        count
    }

    fn len(&self) -> usize {
        self.size
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


struct Cursor {
    temp_int_part: u8,
    temp_fraction_part: u8,
    parsing_name: bool,
    parsing_int_part: bool,
    station_idx: usize,
    hash: usize,
    temp_multiplier: f32,
    station: [u8; 100],
}

impl Cursor {
    fn new() -> Cursor {
        return Cursor {
            hash: 5381,
            station: [0; 100],
            station_idx: 0,
            parsing_name: true,
            temp_int_part: 0,
            temp_fraction_part: 0,
            temp_multiplier: 1.0,
            parsing_int_part: true
        };
    }
}

fn temprature(c: &mut Cursor) -> f32 {
    c.temp_multiplier * (f32::try_from(c.temp_int_part).unwrap() + f32::from(c.temp_fraction_part) / 10.0)
}
    
fn reset(c: &mut Cursor) {
    c.station = [0; 100];
    c.station_idx = 0;
    c.hash = 5381;
    c.parsing_name = true;
    c.temp_int_part = 0;
    c.temp_fraction_part = 0;
    c.temp_multiplier = 1.0;
    c.parsing_int_part = true;
}

fn update_temprature(c: &mut Cursor, byte: u8) {
    if byte == b'.' {
        c.parsing_int_part = false;
        return;
    } else if byte == b'-' {
        c.temp_multiplier = -1.0;
        return;
    }

    let digit = byte - b'0';
    if c.parsing_int_part {
        c.temp_int_part = (c.temp_int_part * 10) + digit;
    } else {
        c.temp_fraction_part = (c.temp_fraction_part * 10) + digit;
    }
}

unsafe fn read_unaligned_u64(ptr: *const usize) -> usize {
    let mut result: usize = 1;

    asm!(
        "mov {result}, qword ptr [{ptr}]",
        ptr = in(reg) ptr,
        result = out(reg) result,
    );

    result
}

fn find_newline(data: usize) -> i32 {
    let x = data ^ 0x0A0A0A0A0A0A0A0A;
    let y = (x-0x0101010101010101) & !x;

    let z = y & (0x8080808080808080);
    if z == 0 {
        return -1;
    }
    return (y.trailing_zeros() >> 3) as i32;
}

fn find_next_newline_vectorized(ptr: *const u8,  max_offset: isize) -> isize {
    let mut offset = 0;
    let mut bytes_remaining: isize = max_offset-offset+1;
    while bytes_remaining >= 8 {
        unsafe {
            let data: usize = read_unaligned_u64(ptr.offset(offset) as *const usize);
            let newline_idx = find_newline(data);
            if newline_idx == -1 {
                offset += 8;
                bytes_remaining -= 8;
            } else {
                offset += newline_idx as isize;
                return offset;
            }
        }
    }
    
    while bytes_remaining > 0 {
        unsafe {
            if *ptr.offset(offset) == b'\n' {
                return offset;
            }
            offset +=1;
            bytes_remaining -= 1;
        }
    }

    assert!(false, "unreacheable code");
    0
}

fn find_next_newline(ptr: *const u8, max_offset: isize, c: &mut Cursor) -> isize {
    let mut offset = 0;
    let mut bytes_remaining: isize = max_offset-offset+1;
    while bytes_remaining > 0 {
        unsafe {
            let byte = *ptr.offset(offset);
            if byte == b'\n' {
                return offset;
            }
            update_temprature(c, byte);
            offset +=1;
            bytes_remaining -= 1;
        }
    }

    assert!(false, "unreacheable code");
    0
}


fn find_semicolon(data: usize) -> i32 {
    // finds the 
    let x = data ^ 0x3B3B3B3B3B3B3B3B;
    let y = (x-0x0101010101010101) & (!x) & (0x8080808080808080);
    if y == 0 {
        return -1;
    }
    return (y.trailing_zeros() >> 3) as i32;
}

fn find_next_semicolon_vectorized(ptr: *const u8, max_offset: isize, hash: &mut usize, name: &mut [u8; 100]) -> isize {
    let mut offset: isize = 0;
    let mut bytes_remaining: isize = max_offset-offset+1;
    while bytes_remaining >= 8 {
        unsafe {
            let mut data: usize = read_unaligned_u64(ptr.offset(offset) as *const usize);
            let sc_idx = find_semicolon(data);
            if sc_idx == -1 {
                ptr::copy_nonoverlapping(&data as *const usize as *const u8, name.as_mut_ptr().offset(offset), 8);
                offset += 8;
                bytes_remaining -= 8;
                *hash = (*hash << 5) + *hash + data; 
            } else {
                assert!(sc_idx >= 0 && sc_idx < 8);
                ptr::copy_nonoverlapping(&data as *const usize as *const u8, name.as_mut_ptr().offset(offset), sc_idx as usize);
                data = !(0xFFFFFFFFFFFFFFFF << 8*sc_idx) & data;
                *hash = (*hash << 5) + *hash + data; 
                offset += sc_idx as isize;
                return offset;
            }
        }
    }
    
    while bytes_remaining > 0 {
        unsafe {
            let byte = *ptr.offset(offset);
            if byte == b';' {
                return offset;
            }
            name[offset as usize] = byte;
            *hash = (*hash << 5) + *hash + byte as usize; 
            offset +=1;
            bytes_remaining -= 1;
        }
    }

    assert!(false, "unreacheable code");
    0
}


fn compute(thread_id: usize, filepath: &str, start_offset: usize, file_size_per_thread: isize, file_size: usize) -> LPTable {
    let start_time = Instant::now();

    let file = File::open(filepath).unwrap();
    let contents = unsafe { MmapOptions::new().offset(start_offset.try_into().unwrap()).map(&file).unwrap() };
    let mut table = LPTable::new(130712, 4);


    let buf = contents.as_ptr();
    let mut buf_idx: isize = 0;
    let max_buf_idx: isize = (file_size-start_offset - 1) as isize;


    let ignore_first_line = thread_id != 0;
    if ignore_first_line {  
        unsafe {    
            let offset = find_next_newline_vectorized(buf.offset(buf_idx) as *const u8, (max_buf_idx-buf_idx));
            buf_idx += offset as isize + 1;
        }
    }

    let mut c = Cursor::new();
    while true {
        if buf_idx > file_size_per_thread || buf_idx > max_buf_idx {
            break;
        }

        let mut station_name_len = 0;
        unsafe {
            station_name_len = find_next_semicolon_vectorized(buf.offset(buf_idx) as *const u8, (max_buf_idx-buf_idx), &mut c.hash, &mut c.station);
        }
        buf_idx +=  station_name_len + 1;

        unsafe {
            buf_idx += find_next_newline(buf.offset(buf_idx), (max_buf_idx-buf_idx), &mut c);
        }
        buf_idx +=1;

        let temp = temprature(&mut c);
        // println!("Found {}:{}", std::str::from_utf8(&c.station[0..station_name_len as usize]).unwrap(), temp);
        table.insert_or_update(&c.station, station_name_len as usize, c.hash, temp);  
        reset(&mut c);
    }

    let end_time = Instant::now();
    info!("Time taken to compute the stats: {} milliseconds", (end_time - start_time).as_millis());
    table
}


pub fn distribute_work(path: &'static str, thread_count: usize) -> Vec<JoinHandle<LPTable>> {
    let mut handles = Vec::with_capacity(thread_count);
    let file_size: usize = fs::metadata(path).unwrap().len().try_into().unwrap();
    let file_size_per_thread = file_size.div_ceil(thread_count);
    for thread_id in 0..thread_count {
        handles.push(thread::spawn(move || {
            compute(thread_id, path, file_size_per_thread * thread_id, file_size_per_thread as isize, file_size)
        }));
    }
    handles
}

pub fn aggregate_result(handles: Vec<JoinHandle<LPTable>>) -> BTreeMap<String, KeyedStat> {
    let mut count = 0;
    let mut result: BTreeMap<String, KeyedStat> = BTreeMap::new();
    for handle in handles {
        let lptable = handle.join().unwrap();
        for slot in lptable.occupied_slots {
            for ks in &lptable.table[slot] {
                count += 1;
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
    result
}


pub fn run(path: &'static str, thread_count: usize) -> BTreeMap<String, KeyedStat> {
    let handles = distribute_work(path, thread_count);
    let result = aggregate_result(handles);
    print_result_btreemap_kstat(&result);
    result
}


#[test]
fn test_impl() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }
    env_logger::init();

    let path = "data/test_small.csv";
    let expected = attempt1::naive_btree_kstat(path);
    let actual = run(path, 1);
    for (station, ks) in expected.into_iter() {
        let acs = actual.get(&station).unwrap();
        assert_eq!(ks.max, acs.max);
        assert_eq!(ks.min, acs.min);
        assert_eq!(ks.sum/ks.count, acs.sum/acs.count);
    }
}



#[test]
fn test_find_next_semicolon() {
    let mut station: [u8; 100] = [0;100];

    let mut sf_hash_1 = 0;
    let bytes = "san_francisco;100";
    let offset = 13;
    assert_eq!(find_next_semicolon_vectorized(bytes.as_ptr(), (bytes.len()-1) as isize, &mut sf_hash_1, &mut station), offset);
    assert_eq!(&station[0..offset as usize], "san_francisco".as_bytes());

    let mut chicago_hash: usize = 0;
    let bytes = "chicago;100";
    let offset = 7;
    assert_eq!(find_next_semicolon_vectorized(bytes.as_ptr(), (bytes.len()-1) as isize, &mut chicago_hash, &mut station), offset);
    assert_eq!(&station[0..offset as usize], "chicago".as_bytes());

    let mut sf_hash_2 = 0;
    let bytes = "san_francisco;111";
    let offset = 13;
    assert_eq!(find_next_semicolon_vectorized(bytes.as_ptr(), (bytes.len()-1) as isize, &mut sf_hash_2, &mut station), offset);
    assert_eq!(&station[0..offset as usize], "san_francisco".as_bytes());

    assert_ne!(sf_hash_1, chicago_hash);
    assert_eq!(sf_hash_1, sf_hash_2);
}


#[test]
fn test_find_next_newline() {
    let bytes = "1111111\n";
    assert_eq!(find_next_newline_vectorized(bytes.as_ptr(), (bytes.len()-1) as isize), 7);

    let bytes = "11\n11111";
    assert_eq!(find_next_newline_vectorized(bytes.as_ptr(), (bytes.len()-1) as isize), 2);

    let bytes = "0000000011\n11111";
    assert_eq!(find_next_newline_vectorized(bytes.as_ptr(), (bytes.len()-1) as isize), 10);

    let bytes = "000000001111111122\n";
    assert_eq!(find_next_newline_vectorized(bytes.as_ptr(), (bytes.len()-1) as isize), 18);
}
