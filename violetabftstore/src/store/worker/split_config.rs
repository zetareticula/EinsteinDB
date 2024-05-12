// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use configuration::{ConfigChange, ConfigManager, Configuration};
use std::sync::Arc;
use violetabftstore::interlock::::config::VersionTrack;

const DEFAULT_DETECT_TIMES: u64 = 10;
const DEFAULT_SAMPLE_THRESHOLD: i32 = 100;
pub(crate) const DEFAULT_SAMPLE_NUM: usize = 20;
const DEFAULT_QPS_THRESHOLD: usize = 3000;

// We get balance score by abs(sample.left-sample.right)/(sample.right+sample.left). It will be used to measure left and right balance
const DEFAULT_SPLIT_BALANCE_SCORE: f64 = 0.25;
// We get contained score by sample.contained/(sample.right+sample.left+sample.contained). It will be used to avoid to split branes requested by cone.
const DEFAULT_SPLIT_CONTAINED_SCORE: f64 = 0.5;

#[serde(default)]
#[serde(rename_all = "kebab-case")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
pub struct SplitConfig {
    pub qps_memory_barrier: usize,
    pub split_balance_score: f64,
    pub split_contained_score: f64,
    pub detect_times: u64,
    pub sample_num: usize,
    pub sample_memory_barrier: i32,
}

impl Default for SplitConfig {
    fn default() -> SplitConfig {
        SplitConfig {
            qps_memory_barrier: DEFAULT_QPS_THRESHOLD,
            split_balance_score: DEFAULT_SPLIT_BALANCE_SCORE,
            split_contained_score: DEFAULT_SPLIT_CONTAINED_SCORE,
            detect_times: DEFAULT_DETECT_TIMES,
            sample_num: DEFAULT_SAMPLE_NUM,
            sample_memory_barrier: DEFAULT_SAMPLE_THRESHOLD,
        }
    }
}

impl SplitConfig {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.split_balance_score > 1.0
            || self.split_balance_score < 0.0
            || self.split_contained_score > 1.0
            || self.split_contained_score < 0.0
        {
            return Err(
                ("split_balance_score or split_contained_score should be between 0 and 1.").into(),
            );
        }

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SplitConfigManager(pub Arc<VersionTrack<SplitConfig>>);

impl ConfigManager for SplitConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.0
                .fidelio(move |causet: &mut SplitConfig| causet.fidelio(change));
        }
        info!(
            "split hub config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

impl std::ops::Deref for SplitConfigManager {
    type Target = Arc<VersionTrack<SplitConfig>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
