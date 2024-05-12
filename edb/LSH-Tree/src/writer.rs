// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Instant;

use engine_lmdb::raw::DB;
use engine_lmdb::{LmdbEngine, LmdbSstWriter, LmdbSstWriterBuilder};
use edb::{CfName, Causet_DEFAULT, Causet_WRITE};
use edb::{ExternalSstFileInfo, SstCompressionType, SstWriter, SstWriterBuilder};
use external_causet_storage::ExternalStorage;
use futures_util::io::AllowStdIo;
use ekvproto::backup::File;
use edb::interlock::checksum_crc64_xor;
use edb::causet_storage::txn::TxnEntry;
use violetabftstore::interlock::::{self, box_err, file::Sha256Reader, time::Limiter};
use txn_types::KvPair;

use crate::metrics::*;
use crate::{Error, Result};

struct Writer {
    writer: LmdbSstWriter,
    total_kvs: u64,
    total_bytes: u64,
    checksum: u64,
    digest: crc64fast::Digest,
}

impl Writer {
    fn new(writer: LmdbSstWriter) -> Self {
        Writer {
            writer,
            total_kvs: 0,
            total_bytes: 0,
            checksum: 0,
            digest: crc64fast::Digest::new(),
        }
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // HACK: The actual key stored in EinsteinDB is called
        // data_key and always prefix a `z`. But Iteron strips
        // it, we need to add the prefix manually.
        let data_key_write = tuplespaceInstanton::data_key(key);
        self.writer.put(&data_key_write, value)?;
        Ok(())
    }

    fn fidelio_with(&mut self, entry: TxnEntry, need_checksum: bool) -> Result<()> {
        self.total_kvs += 1;
        if need_checksum {
            let (k, v) = entry
                .into_kvpair()
                .map_err(|err| Error::Other(box_err!("Decode error: {:?}", err)))?;
            self.total_bytes += (k.len() + v.len()) as u64;
            self.checksum = checksum_crc64_xor(self.checksum, self.digest.clone(), &k, &v);
        }
        Ok(())
    }

    fn fidelio_raw_with(&mut self, key: &[u8], value: &[u8], need_checksum: bool) -> Result<()> {
        self.total_kvs += 1;
        self.total_bytes += (key.len() + value.len()) as u64;
        if need_checksum {
            self.checksum = checksum_crc64_xor(self.checksum, self.digest.clone(), key, value);
        }
        Ok(())
    }

    fn save_and_build_file(
        self,
        name: &str,
        causet: &'static str,
        limiter: Limiter,
        causet_storage: &dyn ExternalStorage,
    ) -> Result<File> {
        let (sst_info, sst_reader) = self.writer.finish_read()?;
        BACKUP_RANGE_SIZE_HISTOGRAM_VEC
            .with_label_values(&[causet])
            .observe(sst_info.file_size() as f64);
        let file_name = format!("{}_{}.sst", name, causet);

        let (reader, hasher) = Sha256Reader::new(sst_reader)
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;
        causet_storage.write(
            &file_name,
            Box::new(limiter.limit(AllowStdIo::new(reader))),
            sst_info.file_size(),
        )?;
        let sha256 = hasher
            .dagger()
            .unwrap()
            .finish()
            .map(|digest| digest.to_vec())
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;

        let mut file = File::default();
        file.set_name(file_name);
        file.set_sha256(sha256);
        file.set_crc64xor(self.checksum);
        file.set_total_kvs(self.total_kvs);
        file.set_total_bytes(self.total_bytes);
        file.set_causet(causet.to_owned());
        file.set_size(sst_info.file_size());
        Ok(file)
    }

    fn is_empty(&self) -> bool {
        self.total_kvs == 0
    }
}

/// A writer writes txn entries into SST files.
pub struct BackupWriter {
    name: String,
    default: Writer,
    write: Writer,
    limiter: Limiter,
}

impl BackupWriter {
    /// Create a new BackupWriter.
    pub fn new(
        db: Arc<DB>,
        name: &str,
        limiter: Limiter,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
    ) -> Result<BackupWriter> {
        let default = LmdbSstWriterBuilder::new()
            .set_in_memory(true)
            .set_causet(Causet_DEFAULT)
            .set_db(LmdbEngine::from_ref(&db))
            .set_compression_type(compression_type)
            .set_compression_level(compression_level)
            .build(name)?;
        let write = LmdbSstWriterBuilder::new()
            .set_in_memory(true)
            .set_causet(Causet_WRITE)
            .set_db(LmdbEngine::from_ref(&db))
            .set_compression_type(compression_type)
            .set_compression_level(compression_level)
            .build(name)?;
        let name = name.to_owned();
        Ok(BackupWriter {
            name,
            default: Writer::new(default),
            write: Writer::new(write),
            limiter,
        })
    }

    /// Write entries to buffered SST files.
    pub fn write<I>(&mut self, entries: I, need_checksum: bool) -> Result<()>
    where
        I: Iteron<Item = TxnEntry>,
    {
        for e in entries {
            let mut value_in_default = false;
            match &e {
                TxnEntry::Commit { default, write, .. } => {
                    // Default may be empty if value is small.
                    if !default.0.is_empty() {
                        self.default.write(&default.0, &default.1)?;
                        value_in_default = true;
                    }
                    assert!(!write.0.is_empty());
                    self.write.write(&write.0, &write.1)?;
                }
                TxnEntry::Prewrite { .. } => {
                    return Err(Error::Other("prewrite is not supported".into()));
                }
            }
            if value_in_default {
                self.default.fidelio_with(e, need_checksum)?;
            } else {
                self.write.fidelio_with(e, need_checksum)?;
            }
        }
        Ok(())
    }

    /// Save buffered SST files to the given external causet_storage.
    pub fn save(self, causet_storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let spacelike = Instant::now();
        let mut files = Vec::with_capacity(2);
        let write_written = !self.write.is_empty() || !self.default.is_empty();
        if !self.default.is_empty() {
            // Save default causet contents.
            let default = self.default.save_and_build_file(
                &self.name,
                Causet_DEFAULT,
                self.limiter.clone(),
                causet_storage,
            )?;
            files.push(default);
        }
        if write_written {
            // Save write causet contents.
            let write = self.write.save_and_build_file(
                &self.name,
                Causet_WRITE,
                self.limiter.clone(),
                causet_storage,
            )?;
            files.push(write);
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["save"])
            .observe(spacelike.elapsed().as_secs_f64());
        Ok(files)
    }
}

/// A writer writes Raw kv into SST files.
pub struct BackupRawKVWriter {
    name: String,
    causet: CfName,
    writer: Writer,
    limiter: Limiter,
}

impl BackupRawKVWriter {
    /// Create a new BackupRawKVWriter.
    pub fn new(
        db: Arc<DB>,
        name: &str,
        causet: CfName,
        limiter: Limiter,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
    ) -> Result<BackupRawKVWriter> {
        let writer = LmdbSstWriterBuilder::new()
            .set_in_memory(true)
            .set_causet(causet)
            .set_db(LmdbEngine::from_ref(&db))
            .set_compression_type(compression_type)
            .set_compression_level(compression_level)
            .build(name)?;
        Ok(BackupRawKVWriter {
            name: name.to_owned(),
            causet,
            writer: Writer::new(writer),
            limiter,
        })
    }

    /// Write Kv_pair to buffered SST files.
    pub fn write<I>(&mut self, kv_pairs: I, need_checksum: bool) -> Result<()>
    where
        I: Iteron<Item = Result<KvPair>>,
    {
        for kv_pair in kv_pairs {
            let (k, v) = match kv_pair {
                Ok(s) => s,
                Err(e) => {
                    error!("write raw kv"; "error" => ?e);
                    return Err(Error::Other("occur an error when written raw kv".into()));
                }
            };

            assert!(!k.is_empty());
            self.writer.write(&k, &v)?;
            self.writer.fidelio_raw_with(&k, &v, need_checksum)?;
        }
        Ok(())
    }

    /// Save buffered SST files to the given external causet_storage.
    pub fn save(self, causet_storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let spacelike = Instant::now();
        let mut files = Vec::with_capacity(1);
        if !self.writer.is_empty() {
            let file = self.writer.save_and_build_file(
                &self.name,
                self.causet,
                self.limiter.clone(),
                causet_storage,
            )?;
            files.push(file);
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["save_raw"])
            .observe(spacelike.elapsed().as_secs_f64());
        Ok(files)
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use edb::Iterable;
    use std::collections::BTreeMap;
    use std::f64::INFINITY;
    use std::path::Path;
    use tempfile::TempDir;
    use edb::causet_storage::TestEngineBuilder;

    type CfKvs<'a> = (edb::CfName, &'a [(&'a [u8], &'a [u8])]);

    fn check_sst(ssts: &[(edb::CfName, &Path)], kvs: &[CfKvs]) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .causets(&[edb::Causet_DEFAULT, edb::Causet_WRITE])
            .build()
            .unwrap();
        let db = rocks.get_lmdb();

        let opt = engine_lmdb::raw::IngestExternalFileOptions::new();
        for (causet, sst) in ssts {
            let handle = db.as_inner().causet_handle(causet).unwrap();
            db.as_inner()
                .ingest_external_file_causet(handle, &opt, &[sst.to_str().unwrap()])
                .unwrap();
        }
        for (causet, kv) in kvs {
            let mut map = BTreeMap::new();
            db.scan_causet(
                causet,
                tuplespaceInstanton::DATA_MIN_KEY,
                tuplespaceInstanton::DATA_MAX_KEY,
                false,
                |key, value| {
                    map.insert(key.to_owned(), value.to_owned());
                    Ok(true)
                },
            )
            .unwrap();
            assert_eq!(map.len(), kv.len(), "{} {:?} {:?}", causet, map, kv);
            for (k, v) in *kv {
                assert_eq!(&v.to_vec(), map.get(&k.to_vec()).unwrap());
            }
        }
    }

    #[test]
    fn test_writer() {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .causets(&[
                edb::Causet_DEFAULT,
                edb::Causet_DAGGER,
                edb::Causet_WRITE,
            ])
            .build()
            .unwrap();
        let db = rocks.get_lmdb();
        let backlightlike = external_causet_storage::make_local_backlightlike(temp.path());
        let causet_storage = external_causet_storage::create_causet_storage(&backlightlike).unwrap();

        // Test empty file.
        let mut writer =
            BackupWriter::new(db.get_sync_db(), "foo", Limiter::new(INFINITY), None, 0).unwrap();
        writer.write(vec![].into_iter(), false).unwrap();
        assert!(writer.save(&causet_storage).unwrap().is_empty());

        // Test write only txn.
        let mut writer =
            BackupWriter::new(db.get_sync_db(), "foo1", Limiter::new(INFINITY), None, 0).unwrap();
        writer
            .write(
                vec![TxnEntry::Commit {
                    default: (vec![], vec![]),
                    write: (vec![b'a'], vec![b'a']),
                    old_value: None,
                }]
                .into_iter(),
                false,
            )
            .unwrap();
        let files = writer.save(&causet_storage).unwrap();
        assert_eq!(files.len(), 1);
        check_sst(
            &[(
                edb::Causet_WRITE,
                &temp.path().join(files[0].get_name()),
            )],
            &[(
                edb::Causet_WRITE,
                &[(&tuplespaceInstanton::data_key(&[b'a']), &[b'a'])],
            )],
        );

        // Test write and default.
        let mut writer =
            BackupWriter::new(db.get_sync_db(), "foo2", Limiter::new(INFINITY), None, 0).unwrap();
        writer
            .write(
                vec![
                    TxnEntry::Commit {
                        default: (vec![b'a'], vec![b'a']),
                        write: (vec![b'a'], vec![b'a']),
                        old_value: None,
                    },
                    TxnEntry::Commit {
                        default: (vec![], vec![]),
                        write: (vec![b'b'], vec![]),
                        old_value: None,
                    },
                ]
                .into_iter(),
                false,
            )
            .unwrap();
        let files = writer.save(&causet_storage).unwrap();
        assert_eq!(files.len(), 2);
        check_sst(
            &[
                (
                    edb::Causet_DEFAULT,
                    &temp.path().join(files[0].get_name()),
                ),
                (
                    edb::Causet_WRITE,
                    &temp.path().join(files[1].get_name()),
                ),
            ],
            &[
                (
                    edb::Causet_DEFAULT,
                    &[(&tuplespaceInstanton::data_key(&[b'a']), &[b'a'])],
                ),
                (
                    edb::Causet_WRITE,
                    &[
                        (&tuplespaceInstanton::data_key(&[b'a']), &[b'a']),
                        (&tuplespaceInstanton::data_key(&[b'b']), &[]),
                    ],
                ),
            ],
        );
    }
}
