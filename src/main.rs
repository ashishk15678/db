use crate::{
    db::{http::HttpResponse, partition::DataBaseClient, sql::SQL},
    log::debug,
};
#[allow(unused_imports)]
pub mod db;
pub mod hashing;
pub mod log;

#[tokio::main]
async fn main() {
    let client = DataBaseClient::new();
    info!("This is another info");
    error!("Another error");
    warn!("Another warning");
    debug("This works ?");
    client.intialize().await;

    let http = HttpResponse {
        status_code: 200,
        protocol: String::from("https"),
        headers: String::from(""),
        body: String::from(""),
    };
    print!("{}", http);
}
