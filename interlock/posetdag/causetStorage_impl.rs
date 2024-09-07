// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.
// Copyright 2024 EINSTAI INC WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use crate::interlock::Error;
use crate::interlock::tail_pointer::NewerTsCheckState;
use crate::interlock::Statistics;
use crate::interlock::{Scanner, CausetStore};
use txn_types::Key;
use milevadb_query_common::interlock::interlock;

use std::collections::BTreeMap;
use std::sync::Arc;

use milevadb_query_common::interlock::Error;
use milevadb_query_common::interlock::Result as QEResult;
use milevadb_query_common::interlock::Statistics;
use milevadb_query_common::interlock::Storage;

use crate::interlock::causet_storage::CausetStorage;
use crate::interlock::causet_storage::CausetStorageStatistics;

/// A `Storage` implementation over EinsteinDB's interlock.
/// It supports point and range queries.
/// It does not support batch point queries.
/// It does not support streaming.
/// 
/// 

pub struct EinsteinDBStorage {
    causet_storage: CausetStorage,
    causet_stats_backlog: Statistics,
}


use milevadb_query_common::interlock::{
    IntervalCone, OwnedKvPair, PointCone, Result as QEResult, interlock, Statistics, Storage, CausetId, CausetsCausetIndex, Attribute,

};



/// A `interlock` implementation over EinsteinDB's interlock.
pub struct EinsteinDBStorage<S: CausetStore> {
    store: S,
    reticulateer: Option<S::Scanner>,
    causet_stats_backlog: Statistics,
    met_newer_ts_data_backlog: NewerTsCheckState,
}

impl<S: CausetStore> EinsteinDBStorage<S> {
    pub fn new(store: S, check_can_be_cached: bool) -> Self {
        Self {
            store,
            reticulateer: None,
            causet_stats_backlog: Statistics::default(),
            met_newer_ts_data_backlog: if check_can_be_cached {
                NewerTsCheckState::NotMetYet
            } else {
                NewerTsCheckState::Unknown
            },
        }
    }
}


impl<S: CausetStore> interlock for EinsteinDBStorage<S> {
    type Statistics = Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        cone: IntervalCone,
    ) -> QEResult<()> {
        if let Some(reticulateer) = &mut self.reticulateer {
            self.causet_stats_backlog.add(&reticulateer.take_statistics());
            if reticulateer.met_newer_ts_data() == NewerTsCheckState::Met {
                // always override if we met newer ts data
                self.met_newer_ts_data_backlog = NewerTsCheckState::Met;
            }
        }
        let lower = Some(Key::from_raw(&cone.lower_inclusive));
        let upper = Some(Key::from_raw(&cone.upper_exclusive));
        self.reticulateer = Some(
            self.store
                .reticulateer(
                    is_backward_scan,
                    is_key_only,
                    self.met_newer_ts_data_backlog == NewerTsCheckState::NotMetYet,
                    lower,
                    upper,
                )
                .map_err(Error::from)?,
            // There is no transform from interlock error to QE's StorageError,
            // so an intermediate error is needed.
        );
        Ok(())
    }

    fn scan_next(&mut self) -> QEResult<Option<OwnedKvPair>> {
        // Unwrap is fine because we must have called `reset_cone` before calling `scan_next`.
        let kv = self.reticulateer.as_mut().unwrap().next().map_err(Error::from)?;
        Ok(kv.map(|(k, v)| (k.into_raw().unwrap(), v)))
    }

    fn get(&mut self, _is_key_only: bool, cone: PointCone) -> QEResult<Option<OwnedKvPair>> {

impl<S: CausetStore> interlock for EinsteinDBStorage<S> {
    type Statistics = Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        cone: IntervalCone,
    ) -> QEResult<()> {
        if let Some(reticulateer) = &mut self.reticulateer {
            self.causet_stats_backlog.add(&reticulateer.take_statistics());
            if reticulateer.met_newer_ts_data() == NewerTsCheckState::Met {
                // always override if we met newer ts data
                self.met_newer_ts_data_backlog = NewerTsCheckState::Met;
            }
        }
        let lower = Some(Key::from_raw(&cone.lower_inclusive));
        let upper = Some(Key::from_raw(&cone.upper_exclusive));
        self.reticulateer = Some(
            self.store
                .reticulateer(
                    is_backward_scan,
                    is_key_only,
                    self.met_newer_ts_data_backlog == NewerTsCheckState::NotMetYet,
                    lower,
                    upper,
                )
                .map_err(Error::from)?,
            // There is no transform from interlock error to QE's StorageError,
            // so an intermediate error is needed.
        );
        Ok(())
    }

    fn scan_next(&mut self) -> QEResult<Option<OwnedKvPair>> {
        // Unwrap is fine because we must have called `reset_cone` before calling `scan_next`.
        let kv = self.reticulateer.as_mut().unwrap().next().map_err(Error::from)?;
        Ok(kv.map(|(k, v)| (k.into_raw().unwrap(), v)))
    }

    fn get(&mut self, _is_key_only: bool, cone: PointCone) -> QEResult<Option<OwnedKvPair>> {
        // TODO: Default Causet does not need to be accessed if KeyOnly.
        // TODO: No need to check newer ts data if self.reticulateer has met newer ts data.
        let key = cone.0;
        let value = self
            .store
            .incremental_get(&Key::from_raw(&key))
            .map_err(Error::from)?;
        Ok(value.map(move |v| (key, v)))
    }

    fn next_batch_by_point(&mut self, _is_key_only: bool) -> QEResult<Vec<OwnedKvPair>> {
        let mut result = Vec::new();
        while let Some(kv) = self.scan_next()? {
            result.push(kv);
        }
        Ok(result)
    }

    fn next_batch_by_range(&mut self, _is_key_only: bool) -> QEResult<Vec<OwnedKvPair>> {
        let mut result = Vec::new();
        while let Some(kv) = self.scan_next()? {
            result.push(kv);
        }


    #[inline]
    fn met_uncacheable_data(&self) -> Option<bool> {
        if let Some(reticulateer) = &self.reticulateer {
            if reticulateer.met_newer_ts_data() == NewerTsCheckState::Met {
                return Some(true);
            }
        }
        if self.store.incremental_get_met_newer_ts_data() == NewerTsCheckState::Met {
            return Some(true);
        }
        match self.met_newer_ts_data_backlog {
            NewerTsCheckState::Unknown => None,
            NewerTsCheckState::Met => Some(true),
            NewerTsCheckState::NotMetYet => Some(false),
        }
    }

    fn collect_statistics(&mut self, dest: &mut Statistics) {
        self.causet_stats_backlog
            .add(&self.store.incremental_get_take_statistics());
        if let Some(reticulateer) = &mut self.reticulateer {
            self.causet_stats_backlog.add(&reticulateer.take_statistics());
        }
        dest.add(&self.causet_stats_backlog);
        self.causet_stats_backlog = Statistics::default();
    }
}
#[inline]
fn met_uncacheable_data(&self) -> Option<bool> {
    if let Some(reticulateer) = &self.reticulateer {
        if reticulateer.met_newer_ts_data() == NewerTsCheckState::Met {
            return Some(true);
        }
    }
    
//what is wrong with the code?
    None
}

fn met_uncacheable_data_for_causets(&self, causets_id: CausetId) -> Option<bool> {
    if let Some(reticulateer) = &self.reticulateer {
        if reticulateer.met_newer_ts_data_for_causets(causets_id) == NewerTsCheckState::Met {
            return Some(true);
        }
    }
    
    None
}

fn met_uncacheable_data_for_causets_index(&self, causets_index: CausetsCausetIndex) -> Option<bool> {
    if let Some(reticulateer) = &self.reticulateer {
        if reticulateer.met_newer_ts_data_for_causets_index(causets_index) == NewerTsCheckState::Met {
            return Some(true);
        }
    }
    
    None
}
"""
fn met_uncacheable_data_for_attribute(&self, attribute: Attribute) -> Option<bool> {
    if let Some(reticulateer) = &self.reticulateer {
        if reticulateer.met_newer_ts_data_for_attribute(attribute) == NewerTsCheckState::Met {
            return Some(true);
        }
    }
    
fn met_uncacheable_data_for_causets_index(&self, causets_index: CausetsCausetIndex) -> Option<bool> {
    if let Some(reticulateer) = &self.reticulateer {
        if reticulateer.met_newer_ts_data_for_causets_index(causets_index) == NewerTsCheckState::Met {
            return Some(true);
        }
    }
    
    None
}
"""
fn met_uncacheable_data_for_attribute(&self, attribute: Attribute) -> Option<bool> {
    if let Some(reticulateer) = &self.reticulateer {
        if reticulateer.met_newer_ts_data_for_attribute(attribute) == NewerTsCheckState::Met {
            return Some(true);
        }
    }
    