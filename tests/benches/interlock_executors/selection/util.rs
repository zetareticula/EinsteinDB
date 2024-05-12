// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use criterion::black_box;
use criterion::measurement::Measurement;

use fidel_timeshare::Expr;

use milevadb_query_datatype::expr::EvalConfig;
use milevadb_query_normal_executors::{FreeDaemon, SelectionFreeDaemon};
use milevadb_query_vec_executors::interface::BatchFreeDaemon;
use milevadb_query_vec_executors::BatchSelectionFreeDaemon;
use edb::causet_storage::Statistics;

use crate::util::bencher::Bencher;
use crate::util::executor_descriptor::selection;
use crate::util::FixtureBuilder;

pub trait SelectionBencher<M>
where
    M: Measurement,
{
    fn name(&self) -> &'static str;

    fn bench(&self, b: &mut criterion::Bencher<M>, fb: &FixtureBuilder, exprs: &[Expr]);

    fn box_clone(&self) -> Box<dyn SelectionBencher<M>>;
}

impl<M> Clone for Box<dyn SelectionBencher<M>>
where
    M: Measurement,
{
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

/// A bencher that will use normal selection executor to bench the giving expressions.
pub struct NormalBencher;

impl<M> SelectionBencher<M> for NormalBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "normal"
    }

    fn bench(&self, b: &mut criterion::Bencher<M>, fb: &FixtureBuilder, exprs: &[Expr]) {
        crate::util::bencher::NormalNextAllBencher::new(|| {
            let meta = selection(exprs).take_selection();
            let src = fb.clone().build_normal_fixture_executor();
            Box::new(
                SelectionFreeDaemon::new(
                    black_box(meta),
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                )
                .unwrap(),
            ) as Box<dyn FreeDaemon<StorageStats = Statistics>>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SelectionBencher<M>> {
        Box::new(Self)
    }
}

/// A bencher that will use batch selection aggregation executor to bench the giving expressions.
pub struct BatchBencher;

impl<M> SelectionBencher<M> for BatchBencher
where
    M: Measurement,
{
    fn name(&self) -> &'static str {
        "batch"
    }

    fn bench(&self, b: &mut criterion::Bencher<M>, fb: &FixtureBuilder, exprs: &[Expr]) {
        crate::util::bencher::BatchNextAllBencher::new(|| {
            let src = fb.clone().build_batch_fixture_executor();
            Box::new(
                BatchSelectionFreeDaemon::new(
                    black_box(Arc::new(EvalConfig::default())),
                    black_box(Box::new(src)),
                    black_box(exprs.to_vec()),
                )
                .unwrap(),
            ) as Box<dyn BatchFreeDaemon<StorageStats = Statistics>>
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn SelectionBencher<M>> {
        Box::new(Self)
    }
}
