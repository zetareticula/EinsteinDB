// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use edb::{EncryptionKeyManager, FileEncryptionInfo};
use ekvproto::encryption_timeshare::{DataKey, EncryptionMethod, FileDictionary, FileInfo, KeyDictionary};
use protobuf::Message;

use crate::config::{EncryptionConfig, MasterKeyConfig};
use crate::crypter::{self, compat, Iv};
use crate::encrypted_file::EncryptedFile;
use crate::io::EncrypterWriter;
use crate::master_key::{create_backlightlike, Backlightlike, PlaintextBacklightlike};
use crate::metrics::*;
use crate::{Error, Result};

const KEY_DICT_NAME: &str = "key.dict";
const FILE_DICT_NAME: &str = "file.dict";

struct Dicts {
    file_dict: FileDictionary,
    key_dict: KeyDictionary,
    rotation_period: Duration,
    base: PathBuf,
}

impl Dicts {
    fn new(path: &str, rotation_period: Duration) -> Dicts {
        Dicts {
            file_dict: FileDictionary::default(),
            key_dict: KeyDictionary {
                current_key_id: 0,
                ..Default::default()
            },
            rotation_period,
            base: Path::new(path).to_owned(),
        }
    }

    fn open(
        path: &str,
        rotation_period: Duration,
        master_key: &dyn Backlightlike,
    ) -> Result<Option<Dicts>> {
        let base = Path::new(path);

        // File dict is saved in plaintext.
        let file_file = EncryptedFile::new(base, FILE_DICT_NAME);
        let plaintext_config = MasterKeyConfig::Plaintext;
        let plaintext = create_backlightlike(&plaintext_config)?;
        let file_bytes = file_file.read(plaintext.as_ref());

        let key_file = EncryptedFile::new(base, KEY_DICT_NAME);
        let key_bytes = key_file.read(master_key);

        match (file_bytes, key_bytes) {
            // Both files are found.
            (Ok(file_bytes), Ok(key_bytes)) => {
                info!("encryption: found both of key dictionary and file dictionary.");
                let mut file_dict = FileDictionary::default();
                file_dict.merge_from_bytes(&file_bytes)?;
                let mut key_dict = KeyDictionary::default();
                key_dict.merge_from_bytes(&key_bytes)?;

                ENCRYPTION_DATA_KEY_GAUGE.set(key_dict.tuplespaceInstanton.len() as _);
                ENCRYPTION_FILE_NUM_GAUGE.set(file_dict.files.len() as _);

                Ok(Some(Dicts {
                    file_dict,
                    key_dict,
                    rotation_period,
                    base: base.to_owned(),
                }))
            }
            // If neither files are found, encryption was never enabled.
            (Err(Error::Io(file_err)), Err(Error::Io(key_err)))
                if file_err.kind() == ErrorKind::NotFound
                    && key_err.kind() == ErrorKind::NotFound =>
            {
                info!("encryption: none of key dictionary and file dictionary are found.");
                Ok(None)
            }
            // ...else, return either error.
            (file_bytes, key_bytes) => {
                if let Err(key_err) = key_bytes {
                    error!("encryption: failed to load key dictionary.");
                    Err(key_err)
                } else {
                    error!("encryption: failed to load file dictionary.");
                    Err(file_bytes.unwrap_err())
                }
            }
        }
    }

    fn save_key_dict(&mut self, master_key: &dyn Backlightlike) -> Result<()> {
        let file = EncryptedFile::new(&self.base, KEY_DICT_NAME);
        if !master_key.is_secure() {
            for value in self.key_dict.tuplespaceInstanton.values_mut() {
                value.was_exposed = true
            }
        }
        let key_bytes = self.key_dict.write_to_bytes()?;
        file.write(&key_bytes, master_key)?;

        ENCRYPTION_FILE_SIZE_GAUGE
            .with_label_values(&["key_dictionary"])
            .set(key_bytes.len() as _);
        ENCRYPTION_DATA_KEY_GAUGE.set(self.key_dict.tuplespaceInstanton.len() as _);

        Ok(())
    }

    fn save_file_dict(&self) -> Result<()> {
        let file = EncryptedFile::new(&self.base, FILE_DICT_NAME);
        let file_bytes = self.file_dict.write_to_bytes()?;
        // File dict is saved in plaintext.
        file.write(&file_bytes, &PlaintextBacklightlike::default())?;

        ENCRYPTION_FILE_SIZE_GAUGE
            .with_label_values(&["file_dictionary"])
            .set(file_bytes.len() as _);
        ENCRYPTION_FILE_NUM_GAUGE.set(self.file_dict.files.len() as _);

        Ok(())
    }

    fn get_key(&self, key_id: u64) -> Option<&DataKey> {
        self.key_dict.tuplespaceInstanton.get(&key_id)
    }

    fn current_data_key(&self) -> (u64, &DataKey) {
        (
            self.key_dict.current_key_id,
            self.key_dict
                .tuplespaceInstanton
                .get(&self.key_dict.current_key_id)
                .unwrap_or_else(|| {
                    panic!(
                        "current data key not found! Number of tuplespaceInstanton {}",
                        self.key_dict.tuplespaceInstanton.len()
                    );
                }),
        )
    }

    fn get_file(&mut self, fname: &str) -> &FileInfo {
        if self.file_dict.files.get(fname).is_none() {
            // Return Plaintext if file not found
            let mut file = FileInfo::default();
            file.method = compat(EncryptionMethod::Plaintext);
            self.file_dict.files.insert(fname.to_owned(), file);
        }
        self.file_dict.files.get(fname).unwrap()
    }

    fn new_file(&mut self, fname: &str, method: EncryptionMethod) -> Result<&FileInfo> {
        let iv = Iv::new_ctr();
        let mut file = FileInfo::default();
        file.iv = iv.as_slice().to_vec();
        file.key_id = self.key_dict.current_key_id;
        file.method = compat(method);
        self.file_dict.files.insert(fname.to_owned(), file);
        self.save_file_dict()?;
        if method != EncryptionMethod::Plaintext {
            info!("new encrypted file"; 
                  "fname" => fname, 
                  "method" => format!("{:?}", method), 
                  "iv" => hex::encode(iv.as_slice()));
        } else {
            info!("new plaintext file"; "fname" => fname);
        }
        Ok(self.file_dict.files.get(fname).unwrap())
    }

    fn delete_file(&mut self, fname: &str) -> Result<()> {
        let file = match self.file_dict.files.remove(fname) {
            Some(file_info) => file_info,
            None => {
                // Could be a plaintext file not tracked by file dictionary.
                info!("delete untracked plaintext file"; "fname" => fname);
                return Ok(());
            }
        };

        // TOOD GC unused data tuplespaceInstanton.
        self.save_file_dict()?;
        if file.method != compat(EncryptionMethod::Plaintext) {
            info!("delete encrypted file"; "fname" => fname);
        } else {
            info!("delete plaintext file"; "fname" => fname);
        }
        Ok(())
    }

    fn link_file(&mut self, src_fname: &str, dst_fname: &str) -> Result<()> {
        let file = match self.file_dict.files.get(src_fname) {
            Some(file_info) => file_info.clone(),
            None => {
                // Could be a plaintext file not tracked by file dictionary.
                info!("link untracked plaintext file"; "src" => src_fname, "dst" => dst_fname);
                return Ok(());
            }
        };
        if self.file_dict.files.get(dst_fname).is_some() {
            return Err(Error::Io(IoError::new(
                ErrorKind::AlreadyExists,
                format!("file already exists, {}", dst_fname),
            )));
        }
        let method = file.method;
        self.file_dict.files.insert(dst_fname.to_owned(), file);
        self.save_file_dict()?;
        if method != compat(EncryptionMethod::Plaintext) {
            info!("link encrypted file"; "src" => src_fname, "dst" => dst_fname);
        } else {
            info!("link plaintext file"; "src" => src_fname, "dst" => dst_fname);
        }
        Ok(())
    }

    fn rename_file(&mut self, src_fname: &str, dst_fname: &str) -> Result<()> {
        let file = match self.file_dict.files.remove(src_fname) {
            Some(file_info) => file_info,
            None => {
                // Could be a plaintext file not tracked by file dictionary.
                info!("rename untracked plaintext file"; "src" => src_fname, "dst" => dst_fname);
                return Ok(());
            }
        };
        let method = file.method;
        self.file_dict.files.insert(dst_fname.to_owned(), file);
        self.save_file_dict()?;
        if method != compat(EncryptionMethod::Plaintext) {
            info!("rename encrypted file"; "src" => src_fname, "dst" => dst_fname);
        } else {
            info!("rename plaintext file"; "src" => src_fname, "dst" => dst_fname);
        }
        Ok(())
    }

    fn rotate_key(&mut self, key_id: u64, key: DataKey, master_key: &dyn Backlightlike) -> Result<bool> {
        info!("encryption: rotate data key."; "key_id" => key_id);
        match self.key_dict.tuplespaceInstanton.entry(key_id) {
            // key id collides
            Entry::Occupied(_) => return Ok(false),
            Entry::Vacant(e) => e.insert(key),
        };
        // fidelio current data key id.
        self.key_dict.current_key_id = key_id;
        // re-encrypt key dict file.
        self.save_key_dict(master_key).map(|()| true)
    }

    fn maybe_rotate_data_key(
        &mut self,
        method: EncryptionMethod,
        master_key: &dyn Backlightlike,
    ) -> Result<()> {
        let now = SystemTime::now();
        // 0 means it's a new key dict
        if self.key_dict.current_key_id != 0 {
            // It's not a new dict, then check current data key
            // creation time.
            let (_, key) = self.current_data_key();

            // Generate a new data key if
            //   1. encryption method is not the same, or
            //   2. the current data key was exposed and the new master key is secure.
            if compat(method) == key.method && !(key.was_exposed && master_key.is_secure()) {
                let creation_time = UNIX_EPOCH + Duration::from_secs(key.creation_time);
                match now.duration_since(creation_time) {
                    Ok(duration) => {
                        if self.rotation_period > duration {
                            debug!("current data key creation time is within rotation period";
                                "now" => ?now, "creation_time" => ?creation_time);
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        warn!("data key rotate duraion overflow, generate a new data key";
                            "now" => ?now, "creation_time" => ?creation_time, "error" => ?e);
                    }
                }
            }
        }

        let duration = now.duration_since(UNIX_EPOCH).unwrap();
        let creation_time = duration.as_secs();

        // Generate new data key.
        let generate_limit = 10;
        for _ in 0..generate_limit {
            let (key_id, key) = generate_data_key(method);
            if key_id == 0 {
                // 0 is invalid
                continue;
            }
            let mut data_key = DataKey::default();
            data_key.key = key;
            data_key.method = compat(method);
            data_key.creation_time = creation_time;
            data_key.was_exposed = false;

            let ok = self.rotate_key(key_id, data_key, master_key)?;
            if !ok {
                // key id collides, retry
                continue;
            }
            return Ok(());
        }
        Err(box_err!("key id collides {} times!", generate_limit))
    }
}

fn generate_data_key(method: EncryptionMethod) -> (u64, Vec<u8>) {
    use rand::{rngs::OsRng, RngCore};

    let key_id = OsRng.next_u64();
    let key_length = crypter::get_method_key_length(method);
    let mut key = vec![0; key_length];
    OsRng.fill_bytes(&mut key);
    (key_id, key)
}

#[derive(Clone)]
pub struct DataKeyManager {
    master_key: Arc<dyn Backlightlike>,
    dicts: Arc<RwLock<Dicts>>,
    method: EncryptionMethod,
}

impl DataKeyManager {
    pub fn from_config(
        config: &EncryptionConfig,
        dict_path: &str,
    ) -> Result<Option<DataKeyManager>> {
        Self::new(
            &config.master_key,
            &config.previous_master_key,
            config.data_encryption_method,
            config.data_key_rotation_period.into(),
            dict_path,
        )
    }

    pub fn new(
        master_key_config: &MasterKeyConfig,
        previous_master_key_config: &MasterKeyConfig,
        method: EncryptionMethod,
        rotation_period: Duration,
        dict_path: &str,
    ) -> Result<Option<DataKeyManager>> {
        let master_key = create_backlightlike(master_key_config).map_err(|e| {
            error!("failed to access master key, {}", e);
            e
        })?;
        if method != EncryptionMethod::Plaintext && !master_key.is_secure() {
            return Err(box_err!(
                "encryption is to enable but master key is either absent or insecure."
            ));
        }
        let mut dicts = match (
            Dicts::open(dict_path, rotation_period, master_key.as_ref()),
            method,
        ) {
            // Encryption is disabled.
            (Ok(None), EncryptionMethod::Plaintext) => {
                info!("encryption is disabled.");
                return Ok(None);
            }
            // Encryption is being enabled.
            (Ok(None), _) => {
                info!("encryption is being enabled. method = {:?}", method);
                Dicts::new(dict_path, rotation_period)
            }
            // Encryption was enabled and master key didn't change.
            (Ok(Some(dicts)), _) => {
                info!("encryption is enabled. method = {:?}", method);
                dicts
            }
            // Failed to decrypt the dictionaries using master key. Could be master key being
            // rotated. Try the previous master key.
            (Err(Error::WrongMasterKey(e_current)), _) => {
                warn!(
                    "failed to open encryption metadata using master key. \
                      could be master key being rotated. \
                      current master key: {:?}, previous master key: {:?}",
                    master_key_config, previous_master_key_config
                );
                let previous_master_key = create_backlightlike(previous_master_key_config)?;
                let mut dicts =
                    Dicts::open(dict_path, rotation_period, previous_master_key.as_ref())
                        .map_err(|e| {
                            if let Error::WrongMasterKey(e_previous) = e {
                                Error::BothMasterKeyFail(e_current, e_previous)
                            } else {
                                e
                            }
                        })?
                        .ok_or_else(|| {
                            Error::Other(box_err!(
                            "Fallback to previous master key but find dictionaries to be empty."))
                        })?;
                // Rewrite key_dict after replace master key.
                dicts.save_key_dict(master_key.as_ref())?;
                info!("encryption: persisted result after replace master key.");

                dicts
            }
            // Error.
            (Err(e), _) => return Err(e),
        };
        dicts.maybe_rotate_data_key(method, master_key.as_ref())?;

        ENCRYPTION_INITIALIZED_GAUGE.set(1);

        Ok(Some(DataKeyManager {
            dicts: Arc::new(RwLock::new(dicts)),
            master_key,
            method,
        }))
    }

    pub fn create_file<P: AsRef<Path>>(&self, path: P) -> Result<EncrypterWriter<File>> {
        let fname = path.as_ref().to_str().ok_or_else(|| {
            Error::Other(box_err!(
                "failed to convert path to string {:?}",
                path.as_ref()
            ))
        })?;
        let file = self.new_file(fname)?;
        let file_writer = File::create(path)?;
        EncrypterWriter::new(
            file_writer,
            crypter::encryption_method_from_db_encryption_method(file.method),
            &file.key,
            Iv::from_slice(&file.iv)?,
        )
    }

    pub fn dump_key_dict(
        config: &EncryptionConfig,
        dict_path: &str,
        key_ids: Option<Vec<u64>>,
    ) -> Result<()> {
        let dict_file = EncryptedFile::new(Path::new(dict_path), KEY_DICT_NAME);
        // Here we don't trigger master key rotation and don't care about
        // config.previous_master_key.
        let backlightlike = create_backlightlike(&config.master_key)?;
        let dict_bytes = dict_file.read(backlightlike.as_ref())?;
        let mut dict = KeyDictionary::default();
        dict.merge_from_bytes(&dict_bytes)?;
        if let Some(key_ids) = key_ids {
            for key_id in key_ids {
                if let Some(key) = dict.tuplespaceInstanton.get(&key_id) {
                    println!("{}: {:?}", key_id, key);
                }
            }
        } else {
            println!("current key id: {}", dict.current_key_id);
            for (key_id, key) in dict.tuplespaceInstanton.iter() {
                println!("{}: {:?}", key_id, key);
            }
        }
        Ok(())
    }

    pub fn dump_file_dict(dict_path: &str, file_path: Option<&str>) -> Result<()> {
        let dict_file = EncryptedFile::new(Path::new(dict_path), FILE_DICT_NAME);
        let config = MasterKeyConfig::Plaintext;
        let backlightlike = create_backlightlike(&config)?;
        let dict_bytes = dict_file.read(backlightlike.as_ref())?;
        let mut dict = FileDictionary::default();
        dict.merge_from_bytes(&dict_bytes)?;
        if let Some(file_path) = file_path {
            if let Some(info) = dict.files.get(file_path) {
                println!("{}: {:?}", file_path, info);
            }
        } else {
            for (path, info) in dict.files.iter() {
                println!("{}: {:?}", path, info);
            }
        }
        Ok(())
    }
}

impl EncryptionKeyManager for DataKeyManager {
    // Get key to open existing file.
    fn get_file(&self, fname: &str) -> IoResult<FileEncryptionInfo> {
        let mut dicts = self.dicts.write().unwrap();
        let (method, key_id, iv) = {
            let file = dicts.get_file(fname);
            (file.method, file.key_id, file.iv.to_owned())
        };
        // Fail if key is specified but not found.
        let key = if method as i32 == EncryptionMethod::Plaintext as i32 {
            vec![]
        } else {
            match dicts.get_key(key_id) {
                Some(k) => k.key.clone(),
                None => {
                    return Err(IoError::new(
                        ErrorKind::Other,
                        format!("key not found for id {}", key_id),
                    ));
                }
            }
        };
        let encrypted_file = FileEncryptionInfo {
            key,
            method: crypter::encryption_method_to_db_encryption_method(method),
            iv,
        };
        Ok(encrypted_file)
    }

    fn new_file(&self, fname: &str) -> IoResult<FileEncryptionInfo> {
        let mut dicts = self.dicts.write().unwrap();
        // Rotate data key if necessary.
        dicts.maybe_rotate_data_key(self.method, self.master_key.as_ref())?;
        let (_, data_key) = dicts.current_data_key();
        let key = data_key.get_key().to_owned();
        let file = dicts.new_file(fname, self.method)?;
        let encrypted_file = FileEncryptionInfo {
            key,
            method: crypter::encryption_method_to_db_encryption_method(file.method),
            iv: file.get_iv().to_owned(),
        };
        Ok(encrypted_file)
    }

    fn delete_file(&self, fname: &str) -> IoResult<()> {
        self.dicts.write().unwrap().delete_file(fname)?;
        Ok(())
    }

    fn link_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        self.dicts
            .write()
            .unwrap()
            .link_file(src_fname, dst_fname)?;
        Ok(())
    }

    fn rename_file(&self, src_fname: &str, dst_fname: &str) -> IoResult<()> {
        self.dicts
            .write()
            .unwrap()
            .rename_file(src_fname, dst_fname)?;
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::*;
    use crate::config::{FileConfig, Mock};
    use crate::master_key::tests::MockBacklightlike;

    use edb::EncryptionMethod as DBEncryptionMethod;
    use matches::assert_matches;
    use std::{
        fs::{remove_file, File},
        io::Write,
        sync::{Arc, Mutex},
        thread::sleep,
    };
    use tempfile::TempDir;

    // TODO(yiwu): use the similar method in test_util crate instead.
    fn new_tmp_key_manager(
        temp: Option<tempfile::TempDir>,
        method: Option<EncryptionMethod>,
        master_key: Option<MasterKeyConfig>,
        previous_master_key: Option<MasterKeyConfig>,
    ) -> (tempfile::TempDir, Result<Option<DataKeyManager>>) {
        let tmp = temp.unwrap_or_else(|| tempfile::TempDir::new().unwrap());
        let mock_config = MasterKeyConfig::Mock(Mock(Arc::new(Mutex::new(MockBacklightlike::default()))));
        let master_key = master_key.unwrap_or_else(|| mock_config.clone());
        let previous_master_key = previous_master_key.unwrap_or(mock_config);
        let manager = DataKeyManager::new(
            &master_key,
            &previous_master_key,
            method.unwrap_or(EncryptionMethod::Aes256Ctr),
            Duration::from_secs(60),
            tmp.path().as_os_str().to_str().unwrap(),
        );
        (tmp, manager)
    }

    // TODO(yiwu): use the similar method in test_util crate instead.
    fn create_key_file(name: &str) -> (PathBuf, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join(name);
        let mut file = File::create(path.clone()).unwrap();
        file.write_all(b"603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4\n")
            .unwrap();
        (path, tmp_dir)
    }

    #[test]
    fn test_key_manager_encryption_enable_disable() {
        // encryption not enabled.
        let (tmp, manager) =
            new_tmp_key_manager(None, Some(EncryptionMethod::Plaintext), None, None);
        assert!(manager.unwrap().is_none());

        // encryption being enabled.
        let (tmp, manager) =
            new_tmp_key_manager(Some(tmp), Some(EncryptionMethod::Aes256Ctr), None, None);
        let manager = manager.unwrap().unwrap();
        let foo1 = manager.new_file("foo").unwrap();
        drop(manager);

        // encryption was enabled. reopen.
        let (tmp, manager) =
            new_tmp_key_manager(Some(tmp), Some(EncryptionMethod::Aes256Ctr), None, None);
        let manager = manager.unwrap().unwrap();
        let foo2 = manager.get_file("foo").unwrap();
        assert_eq!(foo1, foo2);
        drop(manager);

        // disable encryption.
        let (_tmp, manager) =
            new_tmp_key_manager(Some(tmp), Some(EncryptionMethod::Plaintext), None, None);
        let manager = manager.unwrap().unwrap();
        let foo3 = manager.get_file("foo").unwrap();
        assert_eq!(foo1, foo3);
        let bar = manager.new_file("bar").unwrap();
        assert_eq!(bar.method, DBEncryptionMethod::Plaintext);
    }

    // When enabling encryption, using insecure master key is not allowed.
    #[test]
    fn test_key_manager_disallow_plaintext_metadata() {
        let (_tmp, manager) = new_tmp_key_manager(
            None,
            Some(EncryptionMethod::Aes256Ctr),
            Some(MasterKeyConfig::Plaintext),
            None,
        );
        manager.err().unwrap();
    }

    // If master_key is the wrong key, fallback to previous_master_key.
    #[test]
    fn test_key_manager_rotate_master_key() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        let info1 = manager.new_file("foo").unwrap();
        drop(manager);

        let current_key = Arc::new(Mutex::new(MockBacklightlike {
            is_wrong_master_key: true,
            ..Default::default()
        }));
        let previous_key = Arc::new(Mutex::new(MockBacklightlike::default()));
        let (_tmp, manager) = new_tmp_key_manager(
            Some(tmp),
            None,
            Some(MasterKeyConfig::Mock(Mock(current_key.clone()))),
            Some(MasterKeyConfig::Mock(Mock(previous_key.clone()))),
        );
        let manager = manager.unwrap().unwrap();
        let info2 = manager.get_file("foo").unwrap();
        assert_eq!(info1, info2);
        assert_eq!(1, current_key.dagger().unwrap().encrypt_called);
        assert_eq!(1, current_key.dagger().unwrap().decrypt_called);
        assert_eq!(1, previous_key.dagger().unwrap().decrypt_called);
    }

    #[test]
    fn test_key_manager_rotate_master_key_rewrite_failure() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        let current_key = Arc::new(Mutex::new(MockBacklightlike {
            is_wrong_master_key: true,
            encrypt_fail: true,
            ..Default::default()
        }));
        let previous_key = Arc::new(Mutex::new(MockBacklightlike::default()));
        let (_tmp, manager) = new_tmp_key_manager(
            Some(tmp),
            None,
            Some(MasterKeyConfig::Mock(Mock(current_key.clone()))),
            Some(MasterKeyConfig::Mock(Mock(previous_key.clone()))),
        );
        assert!(manager.is_err());
        assert_eq!(1, current_key.dagger().unwrap().encrypt_called);
        assert_eq!(1, current_key.dagger().unwrap().decrypt_called);
        assert_eq!(1, previous_key.dagger().unwrap().decrypt_called);
    }

    #[test]
    fn test_key_manager_both_master_key_fail() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        let master_key = Arc::new(Mutex::new(MockBacklightlike {
            is_wrong_master_key: true,
            ..Default::default()
        }));
        let (_tmp, manager) = new_tmp_key_manager(
            Some(tmp),
            None,
            Some(MasterKeyConfig::Mock(Mock(master_key.clone()))),
            Some(MasterKeyConfig::Mock(Mock(master_key))),
        );
        assert_matches!(manager.err(), Some(Error::BothMasterKeyFail(_, _)));
    }

    #[test]
    fn test_key_manager_key_dict_missing() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        remove_file(tmp.path().join(KEY_DICT_NAME)).unwrap();
        let (_tmp, manager) = new_tmp_key_manager(Some(tmp), None, None, None);
        assert!(manager.is_err());
    }

    #[test]
    fn test_key_manager_file_dict_missing() {
        // create initial dictionaries.
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        manager.new_file("foo").unwrap();
        drop(manager);

        remove_file(tmp.path().join(FILE_DICT_NAME)).unwrap();
        let (_tmp, manager) = new_tmp_key_manager(Some(tmp), None, None, None);
        assert!(manager.is_err());
    }

    #[test]
    fn test_key_manager_create_get_delete() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let mut manager = manager.unwrap().unwrap();

        let new_file = manager.new_file("foo").unwrap();
        let get_file = manager.get_file("foo").unwrap();
        assert_eq!(new_file, get_file);
        manager.delete_file("foo").unwrap();
        manager.delete_file("foo").unwrap();
        manager.delete_file("foo1").unwrap();

        // Must be plaintext if file not found.
        let file = manager.get_file("foo").unwrap();
        assert_eq!(file.method, DBEncryptionMethod::Plaintext);

        manager.method = EncryptionMethod::Aes192Ctr;
        let file1 = manager.new_file("foo").unwrap();
        assert_ne!(file, file1);

        // Must fail if key is specified but not found.
        let mut file = FileInfo::default();
        file.method = EncryptionMethod::Aes192Ctr as _;
        file.key_id = 7; // Not exists.
        manager
            .dicts
            .write()
            .unwrap()
            .file_dict
            .files
            .insert("foo".to_owned(), file);
        manager.get_file("foo").unwrap_err();
    }

    #[test]
    fn test_key_manager_link() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();

        let file = manager.new_file("foo").unwrap();
        manager.link_file("foo", "foo1").unwrap();

        // Must be the same.
        let file1 = manager.get_file("foo1").unwrap();
        assert_eq!(file1, file);

        // Source file not exists.
        manager.link_file("not exists", "not exists1").unwrap();
        // Target file already exists.
        manager.new_file("foo2").unwrap();
        manager.link_file("foo2", "foo1").unwrap_err();
    }

    #[test]
    fn test_key_manager_rename() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let mut manager = manager.unwrap().unwrap();

        manager.method = EncryptionMethod::Aes192Ctr;
        let file = manager.new_file("foo").unwrap();
        manager.rename_file("foo", "foo1").unwrap();

        // Must be the same.
        let file1 = manager.get_file("foo1").unwrap();
        assert_eq!(file1, file);

        // foo must not exist (should be plaintext)
        manager.rename_file("foo", "foo2").unwrap();
        let file_foo = manager.get_file("foo").unwrap();
        assert_ne!(file_foo, file);
        assert_eq!(file_foo.method, DBEncryptionMethod::Plaintext);
        let file_foo2 = manager.get_file("foo2").unwrap();
        assert_ne!(file_foo2, file);
        assert_eq!(file_foo2.method, DBEncryptionMethod::Plaintext);
    }

    #[test]
    fn test_key_manager_rotate() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();

        let (key_id, key) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };

        // Do not rotate.
        manager
            .dicts
            .write()
            .unwrap()
            .maybe_rotate_data_key(manager.method, manager.master_key.as_ref())
            .unwrap();
        let (current_key_id1, current_key1) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_eq!(current_key_id1, key_id);
        assert_eq!(current_key1, key);

        // Change rotateion period to a smaller value, must rotate.
        manager.dicts.write().unwrap().rotation_period = Duration::from_millis(1);
        sleep(Duration::from_secs(1));
        manager
            .dicts
            .write()
            .unwrap()
            .maybe_rotate_data_key(manager.method, manager.master_key.as_ref())
            .unwrap();
        let (current_key_id2, current_key2) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_ne!(current_key_id2, key_id);
        assert_ne!(current_key2, key);

        // Sleep and must rotate when new a file.
        sleep(Duration::from_secs(1));
        manager.new_file("foo").unwrap();
        let (current_key_id3, current_key3) = {
            let dicts = manager.dicts.read().unwrap();
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_ne!(current_key_id3, current_key_id2);
        assert_ne!(current_key3, current_key2);
    }

    #[test]
    fn test_key_manager_persistence() {
        let (tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();

        // Create a file and a datakey.
        manager.new_file("foo").unwrap();

        let (files, tuplespaceInstanton) = {
            let dicts = manager.dicts.read().unwrap();
            (dicts.file_dict.clone(), dicts.key_dict.clone())
        };

        // Close and re-open.
        drop(manager);
        let (_tmp, manager1) = new_tmp_key_manager(Some(tmp), None, None, None);
        let manager1 = manager1.unwrap().unwrap();

        let dicts = manager1.dicts.read().unwrap();
        assert_eq!(files, dicts.file_dict);
        assert_eq!(tuplespaceInstanton, dicts.key_dict);
    }

    #[test]
    fn test_dcit_rotate_key_collides() {
        let (_tmp, manager) = new_tmp_key_manager(None, None, None, None);
        let manager = manager.unwrap().unwrap();
        let mut dict = manager.dicts.write().unwrap();

        let ok = dict
            .rotate_key(1, DataKey::default(), manager.master_key.as_ref())
            .unwrap();
        assert!(ok);

        let ok = dict
            .rotate_key(1, DataKey::default(), manager.master_key.as_ref())
            .unwrap();
        assert!(!ok);
    }

    #[test]
    fn test_key_manager_rotate_on_key_expose() {
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key = MasterKeyConfig::File {
            config: FileConfig {
                path: key_path.to_str().unwrap().to_owned(),
            },
        };
        let (_tmp_data_dir, manager) = new_tmp_key_manager(None, None, Some(master_key), None);
        let manager = manager.unwrap().unwrap();
        let mut dicts = manager.dicts.write().unwrap();

        let (key_id, key) = {
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };

        // Do not rotate.
        dicts
            .maybe_rotate_data_key(manager.method, manager.master_key.as_ref())
            .unwrap();
        let (current_key_id1, current_key1) = {
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_eq!(current_key_id1, key_id);
        assert_eq!(current_key1, key);

        // Expose the current data key and must rotate.
        dicts
            .key_dict
            .tuplespaceInstanton
            .get_mut(&current_key_id1)
            .unwrap()
            .was_exposed = true;
        dicts
            .maybe_rotate_data_key(manager.method, manager.master_key.as_ref())
            .unwrap();
        let (current_key_id2, current_key2) = {
            let (id, k) = dicts.current_data_key();
            (id, k.clone())
        };
        assert_ne!(current_key_id2, key_id);
        assert_ne!(current_key2, key);
    }

    #[test]
    fn test_expose_tuplespaceInstanton_on_insecure_backlightlike() {
        let (key_path, _tmp_key_dir) = create_key_file("key");
        let master_key = MasterKeyConfig::File {
            config: FileConfig {
                path: key_path.to_str().unwrap().to_owned(),
            },
        };

        let (_tmp_data_dir, manager) = new_tmp_key_manager(None, None, Some(master_key), None);
        let manager = manager.unwrap().unwrap();
        let mut dicts = manager.dicts.write().unwrap();

        for i in 0..100 {
            let ok = dicts
                .rotate_key(i, DataKey::default(), manager.master_key.as_ref())
                .unwrap();
            assert!(ok);
        }
        for value in dicts.key_dict.tuplespaceInstanton.values() {
            assert!(!value.was_exposed);
        }

        // Change it insecure backlightlike and save dicts,
        // must set expose for all tuplespaceInstanton.
        let insecure = Arc::new(PlaintextBacklightlike::default());
        let ok = dicts
            .rotate_key(100, DataKey::default(), insecure.as_ref())
            .unwrap();
        assert!(ok);

        let mut count = 0;
        for value in dicts.key_dict.tuplespaceInstanton.values() {
            count += 1;
            assert!(value.was_exposed);
        }
        assert!(count >= 101);
    }
}
