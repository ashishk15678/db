use crate::{
    config::{Config, get_config},
    db::{admission_control::can_take_task, partition::DataBaseClient},
    init::INIT,
};
#[allow(unused_imports)]
use std::env;
pub mod DS;
pub mod config;
pub mod db;
pub mod hashing;
pub mod init;
pub mod log;
pub mod pools;

#[tokio::main]
async fn main() {
    let config: Config = get_config().unwrap();
    let mut sys = INIT(config.clone()).await.unwrap();
    // println!("{config:?}");
    let args: Vec<String> = env::args().collect();

    can_take_task(read, &mut sys).expect("Cannot take task");
    if args.len() >= 2 {
        if args[1] == "--config" {
            info!(" Showing available config ");
            println!("{}", config);
            return;
        }

        if args[1] == "-h" || args[1] == "--host" {}
    } else {
        info!("New client initialized");
        let client = DataBaseClient::new();
        client.intialize().await;
    }
}

pub fn read() -> String {
    String::from("Hello")
}
