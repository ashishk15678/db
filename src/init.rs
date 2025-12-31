use std::{
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use crate::{
    config::{self, Config},
    info,
};
use proctitle::set_title;
use sysinfo::{System, get_current_pid};

pub async fn INIT(config: Config) -> Result<System, Box<dyn Error>> {
    let pid = get_current_pid().expect("Failed to get PID");
    info!(format!("pid = {pid}"));
    let mut sys = System::new_all();
    set_title(config.name);

    let file_path = Path::new("~/data/__leader.db");
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;

    // leader's partition key will always be 1 for simplicity
    let partition_key = b"1\n";
    file.write_all(partition_key)?;

    Ok(sys)
}
