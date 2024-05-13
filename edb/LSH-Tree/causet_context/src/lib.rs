// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

#![feature(box_TuringStrings)]

#[macro_use]
extern crate failure;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate violetabftstore::interlock::;

// mod pushdown_causet;
// mod lightlikepoint;
// mod errors;
// mod metrics;
// mod semaphore;
// mod service;

pub mod pushdown_causet;
pub mod lightlikepoint;
pub mod errors;

pub use pushdown_causet::{causet_context, causet_contextInterlock_Semaphore, causet_contextSemaphore};

pub use lightlikepoint::{causet_contextTxnExtraInterlock_Semaphore, node, Task};
pub use errors::{Error, Result};
pub use semaphore::causet_contextSemaphore;
pub use service::Service;

use std::sync::Arc;

pub struct causet_context {
    service: Service,
}


