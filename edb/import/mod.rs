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

#[causet(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;

    use edb::causet_storage::kv::TestEngineBuilder;
    use edb::causet_storage::tail_pointer::Config as TailPointerConfig;
    use edb::causet_storage::tail_pointer::Result as TailPointerResult;
    use edb::causet_storage::tail_pointer::TailPointer;
    use edb::causet_storage::tail_pointer::TailPointerBatchSystem;
    use edb::causet_storage::tail_pointer::TailPointerFactoryBuilder;
    use edb::causet_storage::tail_pointer::TailPointerSender;
    use edb::causet_storage::tail_pointer::TestEngine;
    use edb::causet_storage::tail_pointer::TestEngineBuilder as TestEngineBuilderForTest;
    use edb::causet_storage::tail_pointer::TestEngineBuilderWrapper;
    use edb::causet_storage::tail_pointer::TestEngineWrapper;
    use edb::causet_storage::tail_pointer::TestImportSSTService;
    use edb::causet_storage::tail_pointer::TestSSTImporter;
    use edb::causet_storage::tail_pointer::TestSSTWriter;
    use edb::causet_storage::tail_pointer::TestServiceBuilder;
    use edb::causet_storage::tail_pointer::TestServiceBuilderWrapper;
    use edb::causet_storage::tail_pointer::TestServiceWrapper;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapper;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapperBuilder;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapperBuilderWrapper;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapperWrapper;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapperWrapperBuilder;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapperWrapperBuilderWrapper;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapperWrapperWrapper;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapperWrapperWrapperBuilder;
    use edb::causet_storage::tail_pointer::TestSstWriterWrapperWrapperWrapperBuilderWrapper;
}










