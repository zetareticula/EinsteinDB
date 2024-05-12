// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::edb::LmdbEngine;
use crate::options::LmdbWriteOptions;
use crate::util::get_causet_handle;
use edb::{self, Error, MuBlock, Result, WriteBatchExt, WriteOptions};
use lmdb::{WriBlock, WriteBatch as RawWriteBatch, DB};

const WRITE_BATCH_MAX_BATCH: usize = 16;
const WRITE_BATCH_LIMIT: usize = 16;

impl WriteBatchExt for LmdbEngine {
    type WriteBatch = LmdbWriteBatch;
    type WriteBatchVec = LmdbWriteBatchVec;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_opt(&self, wb: &Self::WriteBatch, opts: &WriteOptions) -> Result<()> {
        debug_assert_eq!(
            wb.get_db().path(),
            self.as_inner().path(),
            "mismatched db path"
        );
        let opt: LmdbWriteOptions = opts.into();
        self.as_inner()
            .write_opt(wb.as_inner(), &opt.into_raw())
            .map_err(Error::Engine)
    }

    fn write_vec_opt(&self, wb: &LmdbWriteBatchVec, opts: &WriteOptions) -> Result<()> {
        let opt: LmdbWriteOptions = opts.into();
        if wb.index > 0 {
            self.as_inner()
                .multi_batch_write(wb.as_inner(), &opt.into_raw())
                .map_err(Error::Engine)
        } else {
            self.as_inner()
                .write_opt(&wb.wbs[0], &opt.into_raw())
                .map_err(Error::Engine)
        }
    }

    fn support_write_batch_vec(&self) -> bool {
        let options = self.as_inner().get_db_options();
        options.is_enable_multi_batch_write()
    }

    fn write_batch(&self) -> Self::WriteBatch {
        Self::WriteBatch::new(Arc::clone(&self.as_inner()))
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        Self::WriteBatch::with_capacity(Arc::clone(&self.as_inner()), cap)
    }
}

pub struct LmdbWriteBatch {
    db: Arc<DB>,
    wb: RawWriteBatch,
}

impl LmdbWriteBatch {
    pub fn new(db: Arc<DB>) -> LmdbWriteBatch {
        LmdbWriteBatch {
            db,
            wb: RawWriteBatch::default(),
        }
    }

    pub fn as_inner(&self) -> &RawWriteBatch {
        &self.wb
    }

    pub fn with_capacity(db: Arc<DB>, cap: usize) -> LmdbWriteBatch {
        let wb = if cap == 0 {
            RawWriteBatch::default()
        } else {
            RawWriteBatch::with_capacity(cap)
        };
        LmdbWriteBatch { db, wb }
    }

    pub fn from_raw(db: Arc<DB>, wb: RawWriteBatch) -> LmdbWriteBatch {
        LmdbWriteBatch { db, wb }
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }
}

impl edb::WriteBatch<LmdbEngine> for LmdbWriteBatch {
    fn with_capacity(e: &LmdbEngine, cap: usize) -> LmdbWriteBatch {
        e.write_batch_with_cap(cap)
    }

    fn write_to_engine(&self, e: &LmdbEngine, opts: &WriteOptions) -> Result<()> {
        e.write_opt(self, opts)
    }
}

impl MuBlock for LmdbWriteBatch {
    fn data_size(&self) -> usize {
        self.wb.data_size()
    }

    fn count(&self) -> usize {
        self.wb.count()
    }

    fn is_empty(&self) -> bool {
        self.wb.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.wb.count() > LmdbEngine::WRITE_BATCH_MAX_KEYS
    }

    fn clear(&mut self) {
        self.wb.clear();
    }

    fn set_save_point(&mut self) {
        self.wb.set_save_point();
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.wb.pop_save_point().map_err(Error::Engine)
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.wb.rollback_to_save_point().map_err(Error::Engine)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wb.put(key, value).map_err(Error::Engine)
    }

    fn put_causet(&mut self, causet: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_causet_handle(self.db.as_ref(), causet)?;
        self.wb.put_causet(handle, key, value).map_err(Error::Engine)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.wb.delete(key).map_err(Error::Engine)
    }

    fn delete_causet(&mut self, causet: &str, key: &[u8]) -> Result<()> {
        let handle = get_causet_handle(self.db.as_ref(), causet)?;
        self.wb.delete_causet(handle, key).map_err(Error::Engine)
    }

    fn delete_cone_causet(&mut self, causet: &str, begin_key: &[u8], lightlike_key: &[u8]) -> Result<()> {
        let handle = get_causet_handle(self.db.as_ref(), causet)?;
        self.wb
            .delete_cone_causet(handle, begin_key, lightlike_key)
            .map_err(Error::Engine)
    }
}

/// `LmdbWriteBatchVec` is for method `multi_batch_write` of Lmdb, which splits a large WriteBatch
/// into many smaller ones and then any thread could help to deal with these small WriteBatch when it
/// is calling `AwaitState` and wait to become leader of WriteGroup. `multi_batch_write` will perform
/// much better than traditional `pipelined_write` when EinsteinDB writes very large data into Lmdb. We
/// will remove this feature when `unordered_write` of Lmdb becomes more sBlock and becomes compatible
/// with Noether.
pub struct LmdbWriteBatchVec {
    db: Arc<DB>,
    wbs: Vec<RawWriteBatch>,
    save_points: Vec<usize>,
    index: usize,
    cur_batch_size: usize,
    batch_size_limit: usize,
}

impl LmdbWriteBatchVec {
    pub fn new(db: Arc<DB>, batch_size_limit: usize, cap: usize) -> LmdbWriteBatchVec {
        let wb = RawWriteBatch::with_capacity(cap);
        LmdbWriteBatchVec {
            db,
            wbs: vec![wb],
            save_points: vec![],
            index: 0,
            cur_batch_size: 0,
            batch_size_limit,
        }
    }

    pub fn as_inner(&self) -> &[RawWriteBatch] {
        &self.wbs[0..=self.index]
    }

    pub fn as_raw(&self) -> &RawWriteBatch {
        &self.wbs[0]
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }

    /// `check_switch_batch` will split a large WriteBatch into many smaller ones. This is to avoid
    /// a large WriteBatch blocking write_thread too long.
    fn check_switch_batch(&mut self) {
        if self.batch_size_limit > 0 && self.cur_batch_size >= self.batch_size_limit {
            self.index += 1;
            self.cur_batch_size = 0;
            if self.index >= self.wbs.len() {
                self.wbs.push(RawWriteBatch::default());
            }
        }
        self.cur_batch_size += 1;
    }
}

impl edb::WriteBatch<LmdbEngine> for LmdbWriteBatchVec {
    fn with_capacity(e: &LmdbEngine, cap: usize) -> LmdbWriteBatchVec {
        LmdbWriteBatchVec::new(e.as_inner().clone(), WRITE_BATCH_LIMIT, cap)
    }

    fn write_to_engine(&self, e: &LmdbEngine, opts: &WriteOptions) -> Result<()> {
        e.write_vec_opt(self, opts)
    }
}

impl MuBlock for LmdbWriteBatchVec {
    fn data_size(&self) -> usize {
        self.wbs.iter().fold(0, |a, b| a + b.data_size())
    }

    fn count(&self) -> usize {
        self.cur_batch_size + self.index * self.batch_size_limit
    }

    fn is_empty(&self) -> bool {
        self.wbs[0].is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.index >= WRITE_BATCH_MAX_BATCH
    }

    fn clear(&mut self) {
        for i in 0..=self.index {
            self.wbs[i].clear();
        }
        self.save_points.clear();
        self.index = 0;
        self.cur_batch_size = 0;
    }

    fn set_save_point(&mut self) {
        self.wbs[self.index].set_save_point();
        self.save_points.push(self.index);
    }

    fn pop_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            return self.wbs[x].pop_save_point().map_err(Error::Engine);
        }
        Err(Error::Engine("no save point".into()))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            for i in x + 1..=self.index {
                self.wbs[i].clear();
            }
            self.index = x;
            return self.wbs[x].rollback_to_save_point().map_err(Error::Engine);
        }
        Err(Error::Engine("no save point".into()))
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].put(key, value).map_err(Error::Engine)
    }

    fn put_causet(&mut self, causet: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_causet_handle(self.db.as_ref(), causet)?;
        self.wbs[self.index]
            .put_causet(handle, key, value)
            .map_err(Error::Engine)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].delete(key).map_err(Error::Engine)
    }

    fn delete_causet(&mut self, causet: &str, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_causet_handle(self.db.as_ref(), causet)?;
        self.wbs[self.index]
            .delete_causet(handle, key)
            .map_err(Error::Engine)
    }

    fn delete_cone_causet(&mut self, causet: &str, begin_key: &[u8], lightlike_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_causet_handle(self.db.as_ref(), causet)?;
        self.wbs[self.index]
            .delete_cone_causet(handle, begin_key, lightlike_key)
            .map_err(Error::Engine)
    }
}

#[causet(test)]
mod tests {
    use super::super::util::new_engine_opt;
    use super::super::LmdbDBOptions;
    use super::*;
    use edb::WriteBatch;
    use lmdb::DBOptions as RawDBOptions;
    use tempfile::Builder;

    #[test]
    fn test_should_write_to_engine() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .temfidelir()
            .unwrap();
        let opt = RawDBOptions::default();
        opt.enable_multi_batch_write(true);
        opt.enable_unordered_write(false);
        opt.enable_pipelined_write(true);
        let engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            LmdbDBOptions::from_raw(opt),
            vec![],
        )
        .unwrap();
        assert!(engine.support_write_batch_vec());
        let mut wb = engine.write_batch();
        for _i in 0..LmdbEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        let mut wb = LmdbWriteBatchVec::with_capacity(&engine, 1024);
        for _i in 0..WRITE_BATCH_MAX_BATCH * WRITE_BATCH_LIMIT {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        wb.clear();
        assert!(!wb.should_write_to_engine());
    }
}
