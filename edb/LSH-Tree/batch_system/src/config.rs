// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use violetabftstore::interlock::::config::ReadableDuration;
use serde::{Deserialize, Serialize};
use std::default::Default;
use std::fmt::Debug;
use std::clone::Clone;
use std::cmp::{Eq, PartialEq};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub max_batch_size: usize,
    pub pool_size: usize,
    pub reschedule_duration: ReadableDuration,
}




impl Config {
    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    pub fn reschedule_duration(&self) -> ReadableDuration {
        self.reschedule_duration
    }
}


impl Default for Config {
    fn default() -> Config {
        Config {
            max_batch_size: 256,
            pool_size: 2,
            reschedule_duration: ReadableDuration::secs(5),
        }
    }
}

