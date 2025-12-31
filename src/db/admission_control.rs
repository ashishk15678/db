use crate::{error, info};
use std::thread;
use std::time::Duration;
use sysinfo::{System, get_current_pid};

use crate::config::{Config, get_config};
pub fn can_take_task<T>(t: T, sys: &mut System) -> Result<T, Box<dyn std::error::Error>>
where
    T: Sized,
{
    let config: Config = get_config().expect("Cannot get config");
    let process_name = config.name;
    sys.refresh_all();
    thread::sleep(Duration::from_millis(200));

    sys.refresh_all();

    let current_pid = get_current_pid();
    let target_process = sys
        .processes()
        .values()
        .find(|p| p.pid() == current_pid.unwrap());
    match target_process {
        Some(process) => {
            let cpu_usage = process.cpu_usage();
            let ram_kb = process.memory();
            let ram_mb = ram_kb as f64 / 1024.0;

            info!(format!(
                "Monitoring '{}': CPU {:.2}% | RAM {:.2} MB",
                process_name, cpu_usage, ram_mb
            ));
            if cpu_usage > config.resource.max_cpu_percent {
                error!("Task rejected , exiting");
                return Err(format!(
                    "Task rejected: CPU usage ({:.2}%) exceeds limit ({:.2}%)",
                    cpu_usage, config.resource.max_cpu_percent
                )
                .into());
            }

            if ram_mb > config.resource.max_ram_usage {
                return Err(format!(
                    "Task rejected: RAM usage ({:.2} MB) exceeds limit ({:.2} MB)",
                    ram_mb, config.resource.max_ram_usage
                )
                .into());
            }
            Ok(t)
        }
        None => {
            println!(
                "Target process '{}' not found. Allowing task.",
                process_name
            );
            Ok(t)
        }
    }
}
