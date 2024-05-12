// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::{
    fs::{self, File},
    io,
    path::Path,
    sync::Arc,
};

use encryption::DataKeyManager;
use edb::EncryptionKeyManager;

use super::Result;

/// Prepares the SST file for ingestion.
/// The purpose is to make the ingestion retryable when using the `move_files` option.
/// Things we need to consider here:
/// 1. We need to access the original file on retry, so we should make a clone
///    before ingestion.
/// 2. `Lmdb` will modified the global seqno of the ingested file, so we need
///    to modified the global seqno back to 0 so that we can pass the checksum
///    validation.
/// 3. If the file has been ingested to `Lmdb`, we should not modified the
///    global seqno directly, because that may corrupt Lmdb's data.
pub fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
    path: P,
    clone: Q,
    encryption_key_manager: Option<&Arc<DataKeyManager>>,
) -> Result<()> {
    #[causet(unix)]
    use std::os::unix::fs::MetadataExt;

    let path = path.as_ref().to_str().unwrap();
    let clone = clone.as_ref().to_str().unwrap();

    if Path::new(clone).exists() {
        if let Some(key_manager) = encryption_key_manager {
            key_manager.delete_file(clone)?;
        }
        fs::remove_file(clone).map_err(|e| format!("remove {}: {:?}", clone, e))?;
    }

    #[causet(unix)]
    let nlink = fs::metadata(path)
        .map_err(|e| format!("read metadata from {}: {:?}", path, e))?
        .nlink();
    #[causet(not(unix))]
    let nlink = 0;

    if nlink == 1 {
        // Lmdb must not have this file, we can make a hard link.
        fs::hard_link(path, clone)
            .map_err(|e| format!("link from {} to {}: {:?}", path, clone, e))?;
    } else {
        // Lmdb may have this file, we should make a copy.
        copy_and_sync(path, clone)
            .map_err(|e| format!("copy from {} to {}: {:?}", path, clone, e))?;
    }
    if let Some(key_manager) = encryption_key_manager {
        key_manager.link_file(path, clone)?;
    }
    Ok(())
}

/// Copies the source file to a newly created file.
fn copy_and_sync<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> Result<()> {
    if !from.as_ref().is_file() {
        return Err(format!("{:?} is not an existing regular file", from.as_ref()).into());
    }

    let mut reader = File::open(from)?;
    let mut writer = File::create(to)?;

    io::copy(&mut reader, &mut writer)?;
    writer.sync_all()?;
    Ok(())
}

#[causet(test)]
mod tests {
    use super::prepare_sst_for_ingestion;

    use encryption::DataKeyManager;
    use engine_lmdb::{
        util::{new_engine, LmdbCausetOptions},
        LmdbPrimaryCausetNetworkOptions, LmdbDBOptions, LmdbEngine, LmdbIngestExternalFileOptions,
        LmdbSstWriterBuilder, LmdbNoetherDBOptions,
    };
    use edb::{
        CausetHandleExt, CfName, PrimaryCausetNetworkOptions, DBOptions, EncryptionKeyManager, ImportExt,
        IngestExternalFileOptions, Peekable, SstWriter, SstWriterBuilder, NoetherDBOptions,
    };
    use std::{fs, path::Path, sync::Arc};
    use tempfile::Builder;
    use test_util::encryption::new_test_key_manager;
    use violetabftstore::interlock::::file::calc_crc32;

    #[causet(unix)]
    fn check_hard_link<P: AsRef<Path>>(path: P, nlink: u64) {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(fs::metadata(path).unwrap().nlink(), nlink);
    }

    #[causet(not(unix))]
    fn check_hard_link<P: AsRef<Path>>(_: P, _: u64) {
        // Just do nothing
    }

    fn check_db_with_kvs(db: &LmdbEngine, causet: &str, kvs: &[(&str, &str)]) {
        for &(k, v) in kvs {
            assert_eq!(
                db.get_value_causet(causet, k.as_bytes()).unwrap().unwrap(),
                v.as_bytes()
            );
        }
    }

    fn gen_sst_with_kvs(db: &LmdbEngine, causet: CfName, path: &str, kvs: &[(&str, &str)]) {
        let mut writer = LmdbSstWriterBuilder::new()
            .set_db(db)
            .set_causet(causet)
            .build(path)
            .unwrap();
        for &(k, v) in kvs {
            writer.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
    }

    fn check_prepare_sst_for_ingestion(
        db_opts: Option<LmdbDBOptions>,
        causet_opts: Option<Vec<LmdbCausetOptions>>,
        key_manager: Option<&Arc<DataKeyManager>>,
        was_encrypted: bool,
    ) {
        let path = Builder::new()
            .prefix("_util_lmdb_test_prepare_sst_for_ingestion")
            .temfidelir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let sst_dir = Builder::new()
            .prefix("_util_lmdb_test_prepare_sst_for_ingestion_sst")
            .temfidelir()
            .unwrap();
        let sst_path = sst_dir.path().join("abc.sst");
        let sst_clone = sst_dir.path().join("abc.sst.clone");

        let kvs = [("k1", "v1"), ("k2", "v2"), ("k3", "v3")];

        let causet_name = "default";
        let db = new_engine(path_str, db_opts, &[causet_name], causet_opts).unwrap();
        let causet = db.causet_handle(causet_name).unwrap();
        let mut ingest_opts = LmdbIngestExternalFileOptions::new();
        ingest_opts.move_files(true);

        gen_sst_with_kvs(&db, causet_name, sst_path.to_str().unwrap(), &kvs);
        let size = fs::metadata(&sst_path).unwrap().len();
        let checksum = calc_crc32(&sst_path).unwrap();

        if was_encrypted {
            // Add the file to key_manager to simulate an encrypted file.
            if let Some(manager) = key_manager {
                manager.new_file(sst_path.to_str().unwrap()).unwrap();
            }
        }

        // The first ingestion will hard link sst_path to sst_clone.
        check_hard_link(&sst_path, 1);
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        db.validate_sst_for_ingestion(causet, &sst_clone, size, checksum)
            .unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        // If we prepare again, it will use hard link too.
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        db.validate_sst_for_ingestion(causet, &sst_clone, size, checksum)
            .unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        db.ingest_external_file_causet(causet, &ingest_opts, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, causet_name, &kvs);
        assert!(!sst_clone.exists());
        // Since we are not using key_manager in db, simulate the db deleting the file from
        // key_manager.
        if let Some(manager) = key_manager {
            manager.delete_file(sst_clone.to_str().unwrap()).unwrap();
        }

        // The second ingestion will copy sst_path to sst_clone.
        check_hard_link(&sst_path, 2);
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        db.validate_sst_for_ingestion(causet, &sst_clone, size, checksum)
            .unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 1);
        db.ingest_external_file_causet(causet, &ingest_opts, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, causet_name, &kvs);
        assert!(!sst_clone.exists());
    }

    #[test]
    fn test_prepare_sst_for_ingestion() {
        check_prepare_sst_for_ingestion(
            None, None, None,  /*key_manager*/
            false, /* was encrypted*/
        );
    }

    #[test]
    fn test_prepare_sst_for_ingestion_titan() {
        let mut db_opts = LmdbDBOptions::new();
        let mut titan_opts = LmdbNoetherDBOptions::new();
        // Force all values write out to blob files.
        titan_opts.set_min_blob_size(0);
        db_opts.tenancy_launched_for_einsteindb(&titan_opts);
        let mut causet_opts = LmdbPrimaryCausetNetworkOptions::new();
        causet_opts.tenancy_launched_for_einsteindb(&titan_opts);
        check_prepare_sst_for_ingestion(
            Some(db_opts),
            Some(vec![LmdbCausetOptions::new("default", causet_opts)]),
            None,  /*key_manager*/
            false, /*was_encrypted*/
        );
    }

    #[test]
    fn test_prepare_sst_for_ingestion_with_key_manager_plaintext() {
        let (_tmp_dir, key_manager) = new_test_key_manager(None, None, None, None);
        let manager = Arc::new(key_manager.unwrap().unwrap());
        check_prepare_sst_for_ingestion(None, None, Some(&manager), false /*was_encrypted*/);
    }

    #[test]
    fn test_prepare_sst_for_ingestion_with_key_manager_encrypted() {
        let (_tmp_dir, key_manager) = new_test_key_manager(None, None, None, None);
        let manager = Arc::new(key_manager.unwrap().unwrap());
        check_prepare_sst_for_ingestion(None, None, Some(&manager), true /*was_encrypted*/);
    }
}
