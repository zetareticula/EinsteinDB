//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

//! This mod contains components to support rapid data import with the project
//! `milevadb-lightning`.
//!
//! It mainly exposes one service:
//!
//! The `ImportSSTService` is used to ingest the generated SST files into EinsteinDB's
//! Lmdb instance. The ingesting process: `milevadb-lightning` first uploads SST
//! files to the host where EinsteinDB is located, and then calls the `Ingest` RPC.
//! After `ImportSSTService` receives the RPC, it lightlikes a message to violetabftstore
//! thread to notify it of the ingesting operation.  This service is running
//! inside EinsteinDB because it needs to interact with violetabftstore.

mod sst_service;

pub use self::sst_service::ImportSSTService;
pub use sst_importer::Config;
pub use sst_importer::{Error, Result};
pub use sst_importer::{SSTImporter, SSTWriter};
