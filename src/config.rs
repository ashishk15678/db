use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, write},
    fs,
};
use toml;

#[derive(Debug, Deserialize, Serialize)]
pub struct NetworkConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_timeout_ms")]
    pub connection_timeout_ms: u32,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct ResourceConfig {
    #[serde(default = "default_max_io_rate")]
    pub max_disk_io_rate: u32,

    #[serde(default = "default_max_connections")]
    pub max_concurrent_connections: u32,

    #[serde(default = "default_max_cpu_percent")]
    pub max_cpu_percent: f32,

    #[serde(default)]
    pub enable_rate_limiting: bool,

    #[serde(default = "default_max_ram_usage")]
    pub max_ram_usage: f64,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct ReplicationConfig {
    #[serde(default = "default_replication_mode")]
    pub mode: String,

    #[serde(default = "default_write_quorum")]
    pub write_quorum: u8,
    #[serde(default)]
    pub auto_failover_enabled: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_name")]
    pub name: String,

    #[serde(default = "default_server_count")]
    pub server_count: u8,

    #[serde(default = "default_network")]
    pub network: NetworkConfig,

    #[serde(default = "default_replication")]
    pub replication: ReplicationConfig,

    #[serde(default = "default_resource")]
    pub resource: ResourceConfig, // Mapped to [resource] in TOML
}

pub fn get_config() -> Result<Config, Box<dyn std::error::Error>> {
    let file = fs::read_to_string("config.toml")?;
    let config: Config = toml::from_str(&file)?;
    Ok(config)
}

impl Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "App name = {}\n\n Resource config: \n Max ram (mb) : {} \n Max CPU percent : {} \n Enable rate limiting : {} \n Max concurrent connection : {}",
            self.name,
            // resource
            self.resource.max_ram_usage,
            self.resource.max_cpu_percent,
            self.resource.enable_rate_limiting,
            self.resource.max_concurrent_connections,
        )
    }
}

// --- Default Functions (Necessary for serde(default = "...")) ---
fn default_name() -> String {
    "Butterfly_DB".to_string()
}
fn default_server_count() -> u8 {
    4
}
fn default_bind_address() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    6379
}
fn default_timeout_ms() -> u32 {
    5000
} // 5 seconds

fn default_network() -> NetworkConfig {
    NetworkConfig {
        bind_address: default_bind_address(),
        port: default_port(),
        connection_timeout_ms: default_timeout_ms(),
    }
}
// Replication Defaults
fn default_replication_mode() -> String {
    "Raft".to_string()
}
fn default_write_quorum() -> u8 {
    2
}
fn default_max_io_rate() -> u32 {
    100
}
fn default_max_connections() -> u32 {
    500
} // 500 concurrent connections
fn default_max_cpu_percent() -> f32 {
    60.0
}
fn default_resource() -> ResourceConfig {
    ResourceConfig::default()
}

fn default_replication() -> ReplicationConfig {
    ReplicationConfig::default()
}

fn default_max_ram_usage() -> f64 {
    500.0
}
