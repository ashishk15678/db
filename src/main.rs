use crate::{
    db::{http::HttpResponse, partition::DataBaseClient, sql::SQL},
    log::debug,
};
#[allow(unused_imports)]
use std::env;
pub mod db;
pub mod hashing;
pub mod log;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args[0] == "-h" || args[0] == "--host" {
    } else {
        let client = DataBaseClient::new();
        client.intialize().await;
    }
}
