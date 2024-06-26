// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::causet_defs::CfName;
use crate::errors::Result;
use crate::iterable::Iterable;
use std::path::PathBuf;

/// SstExt is a trait that provides the ability to read and write SST files.
/// It is used to abstract the underlying storage engine.
/// For example, in RocksDB, it is implemented for DB.
/// In EinsteinDB, it is implemented for EinsteinDB.
/// In tests, it is implemented for MemSST.

pub trait SstExt: Sized {
    type SstReader: SstReader;
    type SstWriter: SstWriter;
    type SstWriterBuilder: SstWriterBuilder<Self>;
}

/// SstReader is used to read an SST file.
pub trait SstReader: Iterable + Sized {
    fn open(path: &str) -> Result<Self>;
    fn verify_checksum(&self) -> Result<()>;
    // FIXME: Shouldn't this me a method on Iterable?
    fn iter(&self) -> Self::Iteron;
}

/// SstWriter is used to create sst files that can be added to database later.
pub trait SstWriter {
    type ExternalSstFileInfo: ExternalSstFileInfo;
    type ExternalSstFileReader: std::io::Read;

    /// Add key, value to currently opened file
    /// REQUIRES: key is after any previously added key according to comparator.
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;

    /// Add a deletion key to currently opened file
    /// REQUIRES: key is after any previously added key according to comparator.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Return the current file size.
    fn file_size(&mut self) -> u64;

    /// Finalize writing to sst file and close file.
    fn finish(self) -> Result<Self::ExternalSstFileInfo>;

    /// Finalize writing to sst file and read the contents into the buffer.
    fn finish_read(self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)>;
}

// compression type used for write sst file
#[derive(Copy, Clone)]
pub enum SstCompressionType {
    Lz4,
    Snappy,
    Zstd,
}

/// A builder builds a SstWriter.
pub trait SstWriterBuilder<E>
where
    E: SstExt,
{
    /// Create a new SstWriterBuilder.
    fn new() -> Self;

    /// Set DB for the builder. The builder may need some config from the DB.
    fn set_db(self, db: &E) -> Self;

    /// Set Causet for the builder. The builder may need some config from the Causet.
    fn set_causet(self, causet: CfName) -> Self;

    /// Set it to true, the builder builds a in-memory SST builder.
    fn set_in_memory(self, in_memory: bool) -> Self;

    /// set other config specified by writer
    fn set_compression_type(self, compression: Option<SstCompressionType>) -> Self;

    fn set_compression_level(self, level: i32) -> Self;

    /// Builder a SstWriter.
    fn build(self, path: &str) -> Result<E::SstWriter>;
}

pub trait ExternalSstFileInfo {
    fn new() -> Self;
    fn file_path(&self) -> PathBuf;
    fn smallest_key(&self) -> &[u8];
    fn largest_key(&self) -> &[u8];
    fn sequence_number(&self) -> u64;
    fn file_size(&self) -> u64;
    fn num_entries(&self) -> u64;
}
