// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;
use criterion::measurement::Measurement;

use ekvproto::interlock::KeyCone;
use fidel_timeshare::FreeDaemon as PbFreeDaemon;

use test_interlock::*;
use milevadb_query_common::execute_stats::ExecSummaryCollectorDisabled;
use milevadb_query_datatype::expr::EvalConfig;
use edb::interlock::posetdag::EinsteinDBStorage;
use edb::causet_storage::{LmdbEngine, CausetStore as TxnStore};

use crate::util::bencher::Bencher;
use crate::util::store::StoreDescriber;

pub trait IntegratedBencher<M>
where
    M: Measurement,
{
    fn name(&self) -> String;

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        executors: &[PbFreeDaemon],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
    );

    fn box_clone(&self) -> Box<dyn IntegratedBencher<M>>;
}

impl<M> Clone for Box<dyn IntegratedBencher<M>>
where
    M: Measurement,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal executor to execute the given request.
pub struct NormalBencher<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> NormalBencher<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T, M> IntegratedBencher<M> for NormalBencher<T>
where
    T: TxnStore + 'static,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/normal", <T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        executors: &[PbFreeDaemon],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
    ) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            milevadb_query_normal_executors::runner::build_executors::<_, ExecSummaryCollectorDisabled>(
                black_box(executors.to_vec()),
                black_box(EinsteinDBStorage::new(ToTxnStore::<T>::to_store(store), false)),
                black_box(cones.to_vec()),
                black_box(Arc::new(EvalConfig::default())),
                black_box(false),
            )
            .unwrap()
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn IntegratedBencher<M>> {
        Box::new(Self::new())
    }
}

/// A bencher that will use batch executor to execute the given request.
pub struct BatchBencher<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> BatchBencher<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T, M> IntegratedBencher<M> for BatchBencher<T>
where
    T: TxnStore + 'static,
    M: Measurement,
{
    fn name(&self) -> String {
        format!("{}/batch", <T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        executors: &[PbFreeDaemon],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
    ) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            milevadb_query_vec_executors::runner::build_executors(
                black_box(executors.to_vec()),
                black_box(EinsteinDBStorage::new(ToTxnStore::<T>::to_store(store), false)),
                black_box(cones.to_vec()),
                black_box(Arc::new(EvalConfig::default())),
                black_box(false),
            )
            .unwrap()
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn IntegratedBencher<M>> {
        Box::new(Self::new())
    }
}

pub struct DAGBencher<T: TxnStore + 'static> {
    pub batch: bool,
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> DAGBencher<T> {
    pub fn new(batch: bool) -> Self {
        Self {
            batch,
            _phantom: PhantomData,
        }
    }
}

impl<T, M> IntegratedBencher<M> for DAGBencher<T>
where
    T: TxnStore + 'static,
    M: Measurement,
{
    fn name(&self) -> String {
        let tag = if self.batch { "batch" } else { "normal" };
        format!("{}/{}/with_dag", <T as StoreDescriber>::name(), tag)
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher<M>,
        executors: &[PbFreeDaemon],
        cones: &[KeyCone],
        store: &CausetStore<LmdbEngine>,
    ) {
        crate::util::bencher::DAGHandleBencher::new(|| {
            crate::util::build_dag_handler::<T>(executors, cones, store)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn IntegratedBencher<M>> {
        Box::new(Self::new(self.batch))
    }
}
