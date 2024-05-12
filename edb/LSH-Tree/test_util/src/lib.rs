//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

#![feature(test)]

extern crate test;
#[macro_use]
extern crate slog_global;

pub mod encryption;
mod kv_generator;
mod logging;
mod macros;
mod runner;
mod security;

use std::env;

pub use crate::kv_generator::*;
pub use crate::logging::*;
pub use crate::macros::*;
pub use crate::runner::{
    clear_failpoints, run_failpoint_tests, run_test_with_hook, run_tests, TestHook,
};
pub use crate::security::*;

pub fn setup_for_ci() {
    if env::var("CI").is_ok() {
        if env::var("LOG_FILE").is_ok() {
            logging::init_log_for_test();
        }

        // HACK! Use `epollex` as the polling engine for gRPC when running CI tests on
        // Linux and it hasn't been set before.
        // See more: https://github.com/grpc/grpc/blob/v1.17.2/src/core/lib/iomgr/ev_posix.cc#L124
        // See more: https://grpc.io/grpc/core/md_doc_core_grpc-polling-engines.html
        #[causet(target_os = "linux")]
        {
            if env::var("GRPC_POLL_STRATEGY").is_err() {
                env::set_var("GRPC_POLL_STRATEGY", "epollex");
            }
        }
    }

    if env::var("PANIC_ABORT").is_ok() {
        // Panics as aborts, it's helpful for debugging,
        // but also stops tests immediately.
        violetabftstore::interlock::::set_panic_hook(true, "./");
    }

    violetabftstore::interlock::::check_environment_variables();

    if let Err(e) = violetabftstore::interlock::::config::check_max_open_fds(4096) {
        panic!(
            "To run test, please make sure the maximum number of open file descriptors not \
             less than 4096: {:?}",
            e
        );
    }
}
