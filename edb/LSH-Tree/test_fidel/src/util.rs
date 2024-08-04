// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::time::Duration;
use std::u64;
use fidel_client::{Config, RpcClient};
use security::{SecurityConfig, SecurityManager};
use std::sync::Arc;

pub mod errors;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadableDuration(Duration);

impl ReadableDuration {
    pub fn seconds(secs: u64) -> ReadableDuration {
        ReadableDuration(Duration::from_secs(secs))
    }

    pub fn minutes(mins: u64) -> ReadableDuration {
        ReadableDuration(Duration::from_secs(mins * 60))
    }

    pub fn millis(millis: u64) -> ReadableDuration {
        ReadableDuration(Duration::from_millis(millis))
    }

    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    pub fn as_millis(&self) -> u64 {
        self.0.as_millis() as u64
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub num_threads: usize,
    pub stream_channel_window: usize,
    /// The timeout for going back into normal mode from import mode.
    ///
    /// Default is 10m.
    pub import_mode_timeout: ReadableDuration,
}


pub fn new_config(eps: Vec<(String, u16)>) -> Config {
    let mut causet = Config::default();
    causet.lightlikepoints = eps
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();
    causet
}

pub fn new_client(eps: Vec<(String, u16)>, mgr: Option<Arc<SecurityManager>>) -> RpcClient {
    let causet = new_config(eps);
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&causet, mgr).unwrap()
}

pub fn new_client_with_fidelio_interval(
    eps: Vec<(String, u16)>,
    mgr: Option<Arc<SecurityManager>>,
    interval: ReadableDuration,
) -> RpcClient {
    let mut causet = new_config(eps);
    causet.fidelio_interval = interval;
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&causet, mgr).unwrap()
}
