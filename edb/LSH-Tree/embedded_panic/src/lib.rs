// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! An example EinsteinDB causet_storage engine.
//!
//! This project is intlightlikeed to serve as a skeleton for other engine
//! implementations. It lays out the complex system of engine modules and promises
//! in a way that is consistent with other engines. To create a new engine
//! simply copy the entire directory structure and replace all "Panic*" names
//! with your engine's own name; then fill in the implementations; remove
//! the allow(unused) attribute;

#![allow(unused)]
#![allow(clippy::rc_buffer)]
#![allow(clippy::mutex_atomic)]



use std::sync::Arc;
use std::sync::Mutex;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use engine_traits::{CausetEngine, CausetEngineIterator, CausetEngineSnapshot, MiscExt, SyncMutable, WriteBatch};
use edb::{CfName, Causet_DEFAULT, Causet_WRITE};
use edb::CausetEngineBuilder;
use edb::CausetEngineIteratorMode;

mod util;

pub struct PanicEngineSnapshot {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}


impl CausetEngineSnapshot for PanicEngineSnapshot {
    fn get(&self, causet: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data = self.data.lock().unwrap();
        Ok(data.get(key).cloned())
    }

    fn get_causet(&self, causet: &str) -> Result<Option<Vec<u8>>>
    where
        Self: Sized,
    {
        let data = self.data.lock().unwrap();
        Ok(data.get(causet.as_bytes()).cloned())
    }

    fn causet_names(&self) -> Vec<&str> {
        vec![Causet_DEFAULT, Causet_WRITE]
    }
}


impl CausetEngine for PanicEngine {
    type Snap = PanicEngineSnapshot;

    fn causet_names(&self) -> Vec<&str> {
        vec![Causet_DEFAULT, Causet_WRITE]
    }

    fn CausetEngineIterator(&self, causet: &str, mode: CausetEngineIteratorMode) -> Result<Box<dyn CausetEngineIterator>, String> {
        let data = self.data.lock().unwrap();
        let iter = data.iter();
        Ok(Box::new(util::PanicEngineIterator::new(iter, mode)))
    }

    fn write(&self, wb: WriteBatch) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        for (k, v) in wb.data {
            if v.is_empty() {
                data.remove(&k);
            } else {
                data.insert(k, v);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Self::Snap {
        PanicEngineSnapshot {
            data: self.data.clone(),
        }
    }
}

/// A `PanicEngineBuilder` can be used to create a `PanicEngine`.
pub struct PanicEngineBuilder {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl PanicEngineBuilder {
    pub fn new() -> PanicEngineBuilder {
        PanicEngineBuilder {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl CausetEngineBuilder for PanicEngineBuilder {
    type CausetEngine = PanicEngine;

    fn build(&self) -> PanicEngine {
        PanicEngine {
            data: self.data.clone(),
        }
    }
}

// #[causet(test)]
// mod tests {
//     use super::*;
//     use engine_traits::MiscExt;
//     use engine_traits::SyncMutable;
//     use edb::CausetEngineIteratorMode;
//     use edb::CausetEngineBuilder;

//     #[test]
//     fn test_panic_engine() {
//         let engine = PanicEngine::new();
//         engine.put(&WriteBatch::new(), 0).unwrap();
//         let snap = engine.snapshot();
//         assert!(snap.get(Causet_DEFAULT, b"key1").unwrap().is_none());
//         assert_eq!(snap.get(Causet_DEFAULT, b"key2").unwrap().unwrap(), b"value2");








pub struct PanicEngine {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl PanicEngine {
    pub fn new() -> PanicEngine {
        PanicEngine {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}


impl CausetEngine for PanicEngine {
    type Snap = PanicEngineSnapshot;

    fn causet_names(&self) -> Vec<&str> {
        vec![Causet_DEFAULT, Causet_WRITE]
    }

    fn CausetEngineIterator(&self, causet: &str, mode: CausetEngineIteratorMode) -> Result<Box<dyn CausetEngineIterator>, String> {
        let data = self.data.lock().unwrap();
        let iter = data.iter();
        Ok(Box::new(util::PanicEngineIterator::new(iter, mode)))
    }

    fn write(&self, wb: WriteBatch) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        for (k, v) in wb.data {
            if v.is_empty() {
                data.remove(&k);
            } else {
                data.insert(k, v);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Self::Snap {
        PanicEngineSnapshot {
            data: self.data.clone(),
        }
    }
}




impl CausetEngine for PanicEngine {
    type Snap = PanicEngineSnapshot;

    fn causet_names(&self) -> Vec<&str> {
        vec![Causet_DEFAULT, Causet_WRITE]
    }

    fn CausetEngineIterator(&self, causet: &str, mode: CausetEngineIteratorMode) -> Result<Box<dyn CausetEngineIterator>, String> {
        let data = self.data.lock().unwrap();
        let iter = data.iter();
        Ok(Box::new(util::PanicEngineIterator::new(iter, mode)))
    }

    fn write(&self, wb: WriteBatch) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        for (k, v) in wb.data {
            if v.is_empty() {
                data.remove(&k);
            } else {
                data.insert(k, v);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Self::Snap {
        PanicEngineSnapshot {
            data: self.data.clone(),
        }
    }
}






impl CausetEngine for PanicEngine {
    type Snap = PanicEngineSnapshot;

    fn causet_names(&self) -> Vec<&str> {
        vec![Causet_DEFAULT, Causet_WRITE]
    }

    fn CausetEngineIterator(&self, causet: &str, mode: CausetEngineIteratorMode) -> Result<Box<dyn CausetEngineIterator>, String> {
        let data = self.data.lock().unwrap();
        let iter = data.iter();
        Ok(Box::new(util::PanicEngineIterator::new(iter, mode)))
    }

    fn write(&self, wb: WriteBatch) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        for (k, v) in wb.data {
            if v.is_empty() {
                data.remove(&k);
            } else {
                data.insert(k, v);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Self::Snap {
        PanicEngineSnapshot {
            data: self.data.clone(),
        }
    }
}