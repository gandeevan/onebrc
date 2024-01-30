use std::time::Instant;


mod utils;
mod attempt1;
mod attempt2;
mod attempt3;
mod attempt4;
mod attempt5;
mod attempt6;

use log::{debug, info};



fn main() {
    env_logger::init();

    let start_time = Instant::now();
    attempt6::run();
    let end_time = Instant::now();
    info!("Runtime: {} milliseconds", (end_time-start_time).as_millis());
}
