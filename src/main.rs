use crate::db::{http::HttpResponse, partition::DataBaseClient, sql::SQL};

#[allow(unused_imports)]
pub mod db;
pub mod hashing;

#[tokio::main]
async fn main() {
    let client = DataBaseClient::new();
    client.intialize().await;
    let http = HttpResponse {
        status_code: 200,
        protocol: String::from("https"),
        headers: String::from(""),
        body: String::from(""),
    };
    print!("{}", http);
}
