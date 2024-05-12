//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

//! Importing Lmdb SST files into EinsteinDB
#![feature(min_specialization)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate violetabftstore::interlock::;
#[allow(unused_extern_crates)]
extern crate edb_alloc;

mod config;
mod errors;
pub mod metrics;
mod util;
#[macro_use]
pub mod service;
pub mod import_mode;
pub mod sst_importer;

pub use self::config::Config;
pub use self::errors::{error_inc, Error, Result};
pub use self::sst_importer::{SSTImporter, SSTWriter};
pub use self::util::prepare_sst_for_ingestion;
