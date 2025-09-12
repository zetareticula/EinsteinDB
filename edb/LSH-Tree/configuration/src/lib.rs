// Minimal stub for edb/LSH-Tree/configuration
#![allow(unused)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub name: String,
    pub value: String,
}

impl Config {
    pub fn new(name: String, value: String) -> Self {
        Self { name, value }
    }
}

pub fn load_config() -> Config {
    Config::new("default".to_string(), "value".to_string())
}
