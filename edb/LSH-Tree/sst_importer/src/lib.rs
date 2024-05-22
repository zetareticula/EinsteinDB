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
extern crate prometheus;
#[allow(unused_extern_crates)]
extern crate edb_alloc;

pub use self::config::Config;
pub use self::errors::{error_inc, Error, Result};
pub use self::sst_importer::{SSTImporter, SSTWriter};
pub use self::util::prepare_sst_for_ingestion;


pub mod config;

mod errors;

mod metrics;

mod sst_importer;

mod util;


