// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::sync::Arc;
use std::{fs, usize};

use encryption::{
    encryption_method_from_db_encryption_method, DataKeyManager, DecrypterReader, EncrypterWriter,
    Iv,
};
use edb::{
    CfName, EncryptionKeyManager, Error as EngineError, ImportExt, IngestExternalFileOptions,
    Iterable, CausetEngine, MuBlock, SstWriter, SstWriterBuilder,
};
use ekvproto::encryption_timeshare::EncryptionMethod;
use violetabftstore::interlock::::codec::bytes::{BytesEncoder, CompactBytesFromFileDecoder};
use violetabftstore::interlock::::time::Limiter;

use super::Error;

/// Used to check a procedure is stale or not.
pub trait StaleDetector {
    fn is_stale(&self) -> bool;
}

#[derive(Clone, Copy, Default)]
pub struct BuildStatistics {
    pub key_count: usize,
    pub total_size: usize,
}

/// Build a snapshot file for the given PrimaryCauset family in plain format.
/// If there are no key-value pairs fetched, no files will be created at `path`,
/// otherwise the file will be created and synchronized.
pub fn build_plain_causet_file<E>(
    path: &str,
    key_mgr: Option<&Arc<DataKeyManager>>,
    snap: &E::Snapshot,
    causet: &str,
    spacelike_key: &[u8],
    lightlike_key: &[u8],
) -> Result<BuildStatistics, Error>
where
    E: CausetEngine,
{
    let mut file = Some(box_try!(OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)));
    let mut encrypted_file: Option<EncrypterWriter<File>> = None;
    let mut should_encrypt = false;

    if let Some(key_mgr) = key_mgr {
        let enc_info = box_try!(key_mgr.new_file(path));
        let mthd = encryption_method_from_db_encryption_method(enc_info.method);
        if mthd != EncryptionMethod::Plaintext {
            let writer = box_try!(EncrypterWriter::new(
                file.take().unwrap(),
                mthd,
                &enc_info.key,
                box_try!(Iv::from_slice(&enc_info.iv)),
            ));
            encrypted_file = Some(writer);
            should_encrypt = true;
        }
    }

    let mut writer = if !should_encrypt {
        file.as_mut().unwrap() as &mut dyn Write
    } else {
        encrypted_file.as_mut().unwrap() as &mut dyn Write
    };

    let mut stats = BuildStatistics::default();
    box_try!(snap.scan_causet(causet, spacelike_key, lightlike_key, false, |key, value| {
        stats.key_count += 1;
        stats.total_size += key.len() + value.len();
        box_try!(BytesEncoder::encode_compact_bytes(&mut writer, key));
        box_try!(BytesEncoder::encode_compact_bytes(&mut writer, value));
        Ok(true)
    }));

    if stats.key_count > 0 {
        box_try!(BytesEncoder::encode_compact_bytes(&mut writer, b""));
        let file = if !should_encrypt {
            file.unwrap()
        } else {
            encrypted_file.unwrap().finalize()
        };
        box_try!(file.sync_all());
    } else {
        drop(file);
        box_try!(fs::remove_file(path));
    }

    Ok(stats)
}

/// Build a snapshot file for the given PrimaryCauset family in sst format.
/// If there are no key-value pairs fetched, no files will be created at `path`,
/// otherwise the file will be created and synchronized.
pub fn build_sst_causet_file<E>(
    path: &str,
    engine: &E,
    snap: &E::Snapshot,
    causet: CfName,
    spacelike_key: &[u8],
    lightlike_key: &[u8],
    io_limiter: &Limiter,
) -> Result<BuildStatistics, Error>
where
    E: CausetEngine,
{
    let mut sst_writer = create_sst_file_writer::<E>(engine, causet, path)?;
    let mut stats = BuildStatistics::default();
    box_try!(snap.scan_causet(causet, spacelike_key, lightlike_key, false, |key, value| {
        let entry_len = key.len() + value.len();
        io_limiter.blocking_consume(entry_len);
        stats.key_count += 1;
        stats.total_size += entry_len;
        if let Err(e) = sst_writer.put(key, value) {
            let io_error = io::Error::new(io::ErrorKind::Other, e);
            return Err(io_error.into());
        }
        Ok(true)
    }));
    if stats.key_count > 0 {
        box_try!(sst_writer.finish());
        box_try!(File::open(path).and_then(|f| f.sync_all()));
    } else {
        box_try!(fs::remove_file(path));
    }
    Ok(stats)
}

/// Apply the given snapshot file into a PrimaryCauset family. `callback` will be invoked after each batch of
/// key value pairs written to db.
pub fn apply_plain_causet_file<E, F>(
    path: &str,
    key_mgr: Option<&Arc<DataKeyManager>>,
    stale_detector: &impl StaleDetector,
    db: &E,
    causet: &str,
    batch_size: usize,
    mut callback: F,
) -> Result<(), Error>
where
    E: CausetEngine,
    F: for<'r> FnMut(&'r [(Vec<u8>, Vec<u8>)]),
{
    let file = box_try!(File::open(path));
    let mut decoder = if let Some(key_mgr) = key_mgr {
        let reader = get_decrypter_reader(path, key_mgr)?;
        BufReader::new(reader)
    } else {
        BufReader::new(Box::new(file) as Box<dyn Read + lightlike>)
    };

    let mut wb = db.write_batch();
    let mut write_to_db =
        |db: &E, batch: &mut Vec<(Vec<u8>, Vec<u8>)>| -> Result<(), EngineError> {
            batch.iter().try_for_each(|(k, v)| wb.put_causet(causet, &k, &v))?;
            db.write(&wb)?;
            wb.clear();
            callback(batch);
            batch.clear();
            Ok(())
        };

    // Collect tuplespaceInstanton to a vec rather than wb so that we can invoke the callback less times.
    let mut batch = Vec::with_capacity(1024);
    let mut batch_data_size = 0;

    loop {
        if stale_detector.is_stale() {
            return Err(Error::Abort);
        }
        let key = box_try!(decoder.decode_compact_bytes());
        if key.is_empty() {
            if !batch.is_empty() {
                box_try!(write_to_db(db, &mut batch));
            }
            return Ok(());
        }
        let value = box_try!(decoder.decode_compact_bytes());
        batch_data_size += key.len() + value.len();
        batch.push((key, value));
        if batch_data_size >= batch_size {
            box_try!(write_to_db(db, &mut batch));
            batch_data_size = 0;
        }
    }
}

pub fn apply_sst_causet_file<E>(path: &str, db: &E, causet: &str) -> Result<(), Error>
where
    E: CausetEngine,
{
    let causet_handle = box_try!(db.causet_handle(causet));
    let mut ingest_opt = <E as ImportExt>::IngestExternalFileOptions::new();
    ingest_opt.move_files(true);
    box_try!(db.ingest_external_file_causet(causet_handle, &ingest_opt, &[path]));
    Ok(())
}

fn create_sst_file_writer<E>(engine: &E, causet: CfName, path: &str) -> Result<E::SstWriter, Error>
where
    E: CausetEngine,
{
    let builder = E::SstWriterBuilder::new().set_db(&engine).set_causet(causet);
    let writer = box_try!(builder.build(path));
    Ok(writer)
}

pub fn get_decrypter_reader(
    file: &str,
    encryption_key_manager: &DataKeyManager,
) -> Result<Box<dyn Read + lightlike>, Error> {
    let enc_info = box_try!(encryption_key_manager.get_file(file));
    let mthd = encryption_method_from_db_encryption_method(enc_info.method);
    debug!(
        "get_decrypter_reader gets enc_info for {:?}, method: {:?}",
        file, mthd
    );
    if mthd == EncryptionMethod::Plaintext {
        let f = box_try!(File::open(file));
        return Ok(Box::new(f) as Box<dyn Read + lightlike>);
    }
    let iv = box_try!(Iv::from_slice(&enc_info.iv));
    let f = box_try!(File::open(file));
    let r = box_try!(DecrypterReader::new(f, mthd, &enc_info.key, iv));
    Ok(Box::new(r) as Box<dyn Read + lightlike>)
}

#[causet(test)]
mod tests {
    use std::collections::HashMap;
    use std::f64::INFINITY;
    use std::sync::Arc;

    use super::*;
    use crate::store::snap::tests::*;
    use crate::store::snap::SNAPSHOT_CausetS;
    use engine_lmdb::{Compat, LmdbEngine, LmdbSnapshot};
    use edb::Causet_DEFAULT;
    use tempfile::Builder;
    use violetabftstore::interlock::::time::Limiter;

    struct TestStaleDetector;
    impl StaleDetector for TestStaleDetector {
        fn is_stale(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_causet_build_and_apply_plain_files() {
        let db_creaters = &[open_test_empty_db, open_test_db];
        for db_creater in db_creaters {
            for db_opt in vec![None, Some(gen_db_options_with_encryption())] {
                let dir = Builder::new().prefix("test-snap-causet-db").temfidelir().unwrap();
                let db = db_creater(&dir.path(), db_opt.clone(), None).unwrap();
                // Collect tuplespaceInstanton via the key_callback into a collection.
                let mut applied_tuplespaceInstanton: HashMap<_, Vec<_>> = HashMap::new();
                let dir1 = Builder::new()
                    .prefix("test-snap-causet-db-apply")
                    .temfidelir()
                    .unwrap();
                let db1 = open_test_empty_db(&dir1.path(), db_opt, None).unwrap();

                let snap = LmdbSnapshot::new(Arc::clone(&db));
                for causet in SNAPSHOT_CausetS {
                    let snap_causet_dir = Builder::new().prefix("test-snap-causet").temfidelir().unwrap();
                    let plain_file_path = snap_causet_dir.path().join("plain");
                    let stats = build_plain_causet_file::<LmdbEngine>(
                        &plain_file_path.to_str().unwrap(),
                        None,
                        &snap,
                        causet,
                        &tuplespaceInstanton::data_key(b"a"),
                        &tuplespaceInstanton::data_lightlike_key(b"z"),
                    )
                    .unwrap();
                    if stats.key_count == 0 {
                        assert_eq!(
                            fs::metadata(&plain_file_path).unwrap_err().kind(),
                            io::ErrorKind::NotFound
                        );
                        continue;
                    }

                    let detector = TestStaleDetector {};
                    apply_plain_causet_file(
                        &plain_file_path.to_str().unwrap(),
                        None,
                        &detector,
                        db1.c(),
                        causet,
                        16,
                        |v| {
                            v.to_owned()
                                .into_iter()
                                .for_each(|pair| applied_tuplespaceInstanton.entry(causet).or_default().push(pair))
                        },
                    )
                    .unwrap();
                }

                assert_eq_db(&db, &db1);

                // Scan tuplespaceInstanton from db
                let mut tuplespaceInstanton_in_db: HashMap<_, Vec<_>> = HashMap::new();
                for causet in SNAPSHOT_CausetS {
                    snap.scan_causet(
                        causet,
                        &tuplespaceInstanton::data_key(b"a"),
                        &tuplespaceInstanton::data_lightlike_key(b"z"),
                        true,
                        |k, v| {
                            tuplespaceInstanton_in_db
                                .entry(causet)
                                .or_default()
                                .push((k.to_owned(), v.to_owned()));
                            Ok(true)
                        },
                    )
                    .unwrap();
                }
                assert_eq!(applied_tuplespaceInstanton, tuplespaceInstanton_in_db);
            }
        }
    }

    #[test]
    fn test_causet_build_and_apply_sst_files() {
        let db_creaters = &[open_test_empty_db, open_test_db];
        let limiter = Limiter::new(INFINITY);
        for db_creater in db_creaters {
            for db_opt in vec![None, Some(gen_db_options_with_encryption())] {
                let dir = Builder::new().prefix("test-snap-causet-db").temfidelir().unwrap();
                let db = db_creater(&dir.path(), db_opt.clone(), None).unwrap();

                let snap_causet_dir = Builder::new().prefix("test-snap-causet").temfidelir().unwrap();
                let sst_file_path = snap_causet_dir.path().join("sst");
                let engine = db.c();
                let stats = build_sst_causet_file::<LmdbEngine>(
                    &sst_file_path.to_str().unwrap(),
                    engine,
                    &engine.snapshot(),
                    Causet_DEFAULT,
                    b"a",
                    b"z",
                    &limiter,
                )
                .unwrap();
                if stats.key_count == 0 {
                    assert_eq!(
                        fs::metadata(&sst_file_path).unwrap_err().kind(),
                        io::ErrorKind::NotFound
                    );
                    continue;
                }

                let dir1 = Builder::new()
                    .prefix("test-snap-causet-db-apply")
                    .temfidelir()
                    .unwrap();
                let db1 = open_test_empty_db(&dir1.path(), db_opt, None).unwrap();
                apply_sst_causet_file(&sst_file_path.to_str().unwrap(), db1.c(), Causet_DEFAULT).unwrap();
                assert_eq_db(&db, &db1);
            }
        }
    }
}
