use std::error::Error;

use crate::config;
use proctitle::set_title;
use sysinfo::{System, get_current_pid};

pub fn INIT() -> Result<System, Box<dyn Error>> {
    let pid = get_current_pid().expect("Failed to get PID");
    println!("PID : {pid}");
    let mut sys = System::new_all();
    let process_name = config::get_config().expect("Failed to get config").name;
    set_title(process_name);
    Ok(sys)
}
