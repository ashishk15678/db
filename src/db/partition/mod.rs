use crate::{
    db::{
        http::{handleClient, HttpResponse},
        tcp::start_tcp_server,
    },
    error, info, warn,
};
use rand::Rng;
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Error,
    path::Path,
    process::exit,
    sync::{Arc, Mutex},
};
use std::{
    io::{Read, Write},
    net::SocketAddr,
    thread,
};
use tokio::{fs, task};

// ------------------------------------------------------------------------
// --------------- Structs ------------------------------------------------
#[derive(Debug, Clone)]
pub struct PartitionServer {
    port: usize,
    pub data: Option<Arc<Mutex<BTreeMap<String, String>>>>,
    leader_port: usize,
}

#[derive(Debug)]
pub struct DataBaseClient {
    pub partitions: u8,
    pub servers: Vec<PartitionServer>,
    leader: PartitionServer,
    throttle: u32,
}

impl PartitionServer {
    async fn start(&self) -> Result<(), Error> {
        let addr = format!("0.0.0.0:{}", self.port);
        let _ = start_tcp_server(addr).await;
        Ok(())
    }

    async fn initialize() -> Result<(), Error> {
        let partition_key: usize =
            rand::rng().random_range(usize::max_value() / 10..usize::max_value());

        // this redundancy of code i.e. 3 paths just to make a file is because of lifetime issue
        let f = format!("~/data/{}.db", partition_key);
        let file_path_string = f.as_str();
        let file_path = Path::new(file_path_string);

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)?;

        // leader's partition key will always be 1 for simplicity
        let partition_key = b"1\n";
        file.write_all(partition_key)?;
        Ok(())
    }
}

impl DataBaseClient {
    pub fn new() -> Self {
        // ---------------------------------------------------------------
        // This part comes from config , although for now it is hardcoded
        // will be changed in later parts
        //
        // initially will have 4 partitions only.
        // the port 1231 will be the leader
        // ---------------------------------------------------------------
        let leader = PartitionServer {
            port: 1231,
            data: None,
            leader_port: 1231,
        };
        DataBaseClient {
            partitions: 0,
            leader: leader,
            throttle: 20,
            servers: vec![],
        }
    }

    pub async fn intialize(&self) {
        // the sole reason for getting &self is to have multiple instances at once
        // and the code can be used like this
        //
        // ------------------ Wanted --------------------------------------------
        // ----------------------------------------------------------------------
        // let mut client = DataBaseClient::new();
        // client.initialize();
        // ----------------------------------------------------------------------
        // ----------------------------------------------------------------------
        //
        // what I personally don't want is this
        // ------------------------- Not wanted ----------------------------------
        // DataBaseClient::intitialize()
        // -----------------------------------------------------------------------

        // We use a Vec to hold the tasks and prevent them from being dropped
        let mut tasks = Vec::new();
        // starting leader
        let leader_clone = self.leader.clone();
        let task_handle = task::spawn(async move {
            match leader_clone.start().await {
                Ok(_) => {}
                Err(err) => {
                    error!(format!(
                        "Server at port {} failed to start or crashed. Error: {}",
                        leader_clone.port, err
                    ));
                    exit(127)
                }
            };
        });
        tasks.push(task_handle);

        for server in &self.servers {
            let server_clone = server.clone();

            // `tokio::spawn` launches each server's `start` method as a concurrent task.
            // The loop doesn't block here; it immediately continues to the next server.
            let task_handle = task::spawn(async move {
                match server_clone.start().await {
                    Ok(_) => info!(format!("Server at port {} exited.", server_clone.port)),
                    Err(err) => warn!(format!(
                        "Server at port {} failed to start or crashed. Error: {}",
                        server_clone.port, err
                    )),
                };
            });
            tasks.push(task_handle);
        }

        // Await all tasks to keep the program running indefinitely.
        // This is a blocking call, but it's essential to prevent `main` from exiting.
        for task in tasks {
            let _ = task.await;
        }
    }
}
