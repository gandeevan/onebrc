#![feature(stdarch_x86_avx512)]
#![feature(portable_simd)]
use std::time::Instant;


mod utils;
mod attempt1;
mod attempt2;
mod attempt3;
mod attempt4;
mod attempt5;
mod attempt6;

mod attempt7;

use log::{debug, info};

use crate::utils::THREAD_COUNT;



fn main() {
    env_logger::init();

    let start_time = Instant::now();
    attempt7::run("data/measurements.txt", THREAD_COUNT);
    let end_time = Instant::now();
    info!("Runtime: {} milliseconds", (end_time-start_time).as_millis());
}
