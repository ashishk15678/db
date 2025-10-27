use serde::{Deserialize, Serialize};
use std::fs;
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

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_name")]
    pub name: String,

    #[serde(default = "default_server_count")]
    pub server_count: u8,
    #[serde(default = "default_network")]
    pub network: NetworkConfig,
}

pub fn get_config() -> Result<Config, Box<dyn std::error::Error>> {
    let file = fs::read_to_string("config.toml")?;
    let config: Config = toml::from_str(&file)?;
    Ok(config)
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
