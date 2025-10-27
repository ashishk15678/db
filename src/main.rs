use crate::{
    config::{Config, get_config},
    db::{http::HttpResponse, partition::DataBaseClient, sql::SQL},
    log::debug,
};
#[allow(unused_imports)]
use std::env;
pub mod config;
pub mod db;
pub mod hashing;
pub mod log;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let config: Config = get_config().unwrap();
    println!("{:?}", config);
    if args[0] == "-h" || args[0] == "--host" {
    } else {
        let client = DataBaseClient::new();
        client.intialize().await;
    }
}
