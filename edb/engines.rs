// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::edb::CausetEngine;
use crate::errors::Result;
use crate::options::WriteOptions;
use crate::violetabft_engine::VioletaBftEngine;

#[derive(Clone, Debug)]
pub struct Engines<K, R> {
    pub kv: K,
    pub violetabft: R,
}

impl<K: CausetEngine, R: VioletaBftEngine> Engines<K, R> {
    pub fn new(kv_engine: K, violetabft_engine: R) -> Self {
        Engines {
            kv: kv_engine,
            violetabft: violetabft_engine,
        }
    }

    pub fn write_kv(&self, wb: &K::WriteBatch) -> Result<()> {
        self.kv.write(wb)
    }

    pub fn write_kv_opt(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(wb, opts)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }
}
