// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::make_static_metric;

make_static_metric! {
    pub struct FIDelLocalMetrics: IntCounter {
        "type" => {
            heartbeat,
            validate_peer,
        },
    }
}





lazy_static! {
    pub static ref FIDel_REQUEST_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "edb_fidel_request_duration_seconds",
        "Bucketed histogram of FIDel requests duration",
        &["type"]
    )
    .unwrap();
    pub static ref FIDel_HEARTBEAT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_fidel_heartbeat_message_total",
        "Total number of FIDel heartbeat messages.",
        &["type"]
    )
    .unwrap();
    pub static ref FIDel_VALIDATE_PEER_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "edb_fidel_validate_peer_total",
        "Total number of fidel worker validate peer task.",
        &["type"]
    )
    .unwrap();
    pub static ref STORE_SIZE_GAUGE_VEC: IntGaugeVec =
        register_int_gauge_vec!("edb_store_size_bytes", "Size of causet_storage.", &["type"]).unwrap();
    pub static ref REGION_READ_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "edb_brane_read_tuplespaceInstanton",
        "Histogram of tuplespaceInstanton written for branes",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_READ_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "edb_brane_read_bytes",
        "Histogram of bytes written for branes",
        exponential_buckets(256.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_WRITTEN_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "edb_brane_written_bytes",
        "Histogram of bytes written for branes",
        exponential_buckets(256.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_WRITTEN_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "edb_brane_written_tuplespaceInstanton",
        "Histogram of tuplespaceInstanton written for branes",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
}

pub fn monitor_fidel_request_duration(type_str: &str, observe_duration: f64) {
    FIDel_REQUEST_HISTOGRAM_VEC.with_label_values(&[type_str]).observe(observe_duration);
}

pub fn monitor_fidel_heartbeat_count(type_str: &str, observe_count: i64) {
    FIDel_HEARTBEAT_COUNTER_VEC.with_label_values(&[type_str]).inc_by(observe_count);
}

pub fn monitor_fidel_validate_peer_count(type_str: &str, observe_count: i64) {
    FIDel_VALIDATE_PEER_COUNTER_VEC.with_label_values(&[type_str]).inc_by(observe_count);
}


