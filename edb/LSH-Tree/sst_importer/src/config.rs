//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::error::Error;
use std::result::Result;
use std::time::Duration;
use std::u64;
use std::usize;


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

impl Default for Config {
    fn default() -> Config {
        Config {
            num_threads: 8,
            stream_channel_window: 128,
            import_mode_timeout: ReadableDuration::minutes(10),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.num_threads == 0 {
            return Err("import.num_threads can not be 0".into());
        }
        if self.stream_channel_window == 0 {
            return Err("import.stream_channel_window can not be 0".into());
        }
        Ok(())
    }
}
