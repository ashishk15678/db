use sysinfo::System;

use crate::{
    config::{Config, get_config},
    db::{
        admission_control::can_take_task, http::HttpResponse, partition::DataBaseClient, sql::SQL,
    },
    init::INIT,
    log::debug,
};
#[allow(unused_imports)]
use std::env;
pub mod config;
pub mod db;
pub mod hashing;
pub mod init;
pub mod log;

#[tokio::main]
async fn main() {
    let mut sys = INIT().expect("Init failed");
    let config: Config = get_config().unwrap();
    println!("{config}");
    let args: Vec<String> = env::args().collect();

    if args[0] == "--config" {
        println!("{}", config);
    }

    can_take_task(read, &mut sys).expect("Cannot take task");
    if args[0] == "-h" || args[0] == "--host" {
    } else {
        let client = DataBaseClient::new();
        client.intialize().await;
    }
}

pub fn read() -> String {
    String::from("Hello")
}
