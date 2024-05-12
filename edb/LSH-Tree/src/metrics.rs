// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref BACKUP_REQUEST_HISTOGRAM: Histogram = register_histogram!(
        "edb_backup_request_duration_seconds",
        "Bucketed histogram of backup requests duration"
    )
    .unwrap();
    pub static ref BACKUP_RANGE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "edb_backup_cone_duration_seconds",
        "Bucketed histogram of backup cone duration",
        &["type"]
    )
    .unwrap();
    pub static ref BACKUP_RANGE_SIZE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "edb_backup_cone_size_bytes",
        "Bucketed histogram of backup cone size",
        &["causet"],
        exponential_buckets(32.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref BACKUP_THREAD_POOL_SIZE_GAUGE: IntGauge = register_int_gauge!(
        "edb_backup_thread_pool_size",
        "Total size of backup thread pool"
    )
    .unwrap();
    pub static ref BACKUP_RANGE_ERROR_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_backup_error_counter",
        "Total number of backup errors",
        &["error"]
    )
    .unwrap();
}
