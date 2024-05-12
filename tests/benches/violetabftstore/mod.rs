//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;

use criterion::{Bencher, Criterion};
use engine_lmdb::raw::DB;
use engine_lmdb::Compat;
use edb::{MuBlock, WriteBatchExt};
use test_violetabftstore::*;
use test_util::*;

const DEFAULT_DATA_SIZE: usize = 100_000;

fn enc_write_kvs(db: &Arc<DB>, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let mut wb = db.c().write_batch();
    for &(ref k, ref v) in kvs {
        wb.put(&tuplespaceInstanton::data_key(k), v).unwrap();
    }
    db.c().write(&wb).unwrap();
}

fn prepare_cluster<T: Simulator>(cluster: &mut Cluster<T>, initial_kvs: &[(Vec<u8>, Vec<u8>)]) {
    cluster.run();
    for engines in cluster.engines.values() {
        enc_write_kvs(engines.kv.as_inner(), initial_kvs);
    }
    cluster.leader_of_brane(1).unwrap();
}

#[derive(Debug)]
struct SetConfig<F> {
    factory: F,
    nodes: usize,
    value_size: usize,
}

fn bench_set<T, F>(b: &mut Bencher, input: &SetConfig<F>)
where
    T: Simulator,
    F: ClusterFactory<T>,
{
    let mut cluster = input.factory.build(input.nodes);
    prepare_cluster(&mut cluster, &[]);

    let mut kvs = KvGenerator::new(100, input.value_size);

    b.iter(|| {
        let (k, v) = kvs.next().unwrap();
        cluster.must_put(&k, &v)
    });
}

#[derive(Debug)]
struct GetConfig<F> {
    factory: F,
    nodes: usize,
}

fn bench_get<T, F>(b: &mut Bencher, input: &GetConfig<F>)
where
    T: Simulator,
    F: ClusterFactory<T>,
{
    let mut cluster = input.factory.build(input.nodes);
    let mut kvs = KvGenerator::new(100, 128).generate(DEFAULT_DATA_SIZE);
    prepare_cluster(&mut cluster, &kvs);

    let mut tuplespaceInstanton = kvs
        .drain(..)
        .take(DEFAULT_DATA_SIZE / 10)
        .map(|i| i.0)
        .chain(KvGenerator::new(100, 0).map(|i| i.0));

    b.iter(|| {
        let k = tuplespaceInstanton.next().unwrap();
        cluster.get(&k)
    });
}

#[derive(Debug)]
struct DeleteConfig<F> {
    factory: F,
    nodes: usize,
}

fn bench_delete<T, F>(b: &mut Bencher, input: &DeleteConfig<F>)
where
    T: Simulator,
    F: ClusterFactory<T>,
{
    let mut cluster = input.factory.build(input.nodes);
    let mut kvs = KvGenerator::new(100, 128).generate(DEFAULT_DATA_SIZE);
    prepare_cluster(&mut cluster, &kvs);

    let mut tuplespaceInstanton = kvs
        .drain(..)
        .take(DEFAULT_DATA_SIZE / 10)
        .map(|i| i.0)
        .chain(KvGenerator::new(100, 0).map(|i| i.0));

    b.iter(|| {
        let k = tuplespaceInstanton.next().unwrap();
        cluster.must_delete(&k)
    });
}

fn bench_violetabft_cluster<T, F>(c: &mut Criterion, factory: F, label: &str)
where
    T: Simulator + 'static,
    F: ClusterFactory<T>,
{
    let nodes_coll = vec![1, 3, 5];
    let value_size_coll = vec![8, 128, 1024, 4096];

    let mut set_inputs = vec![];
    let mut get_inputs = vec![];
    let mut delete_inputs = vec![];
    for nodes in nodes_coll {
        for &value_size in &value_size_coll {
            set_inputs.push(SetConfig {
                factory: factory.clone(),
                nodes,
                value_size,
            });
        }
        get_inputs.push(GetConfig {
            factory: factory.clone(),
            nodes,
        });
        delete_inputs.push(DeleteConfig {
            factory: factory.clone(),
            nodes,
        });
    }
    c.bench_function_over_inputs(&format!("{}/bench_set", label), bench_set, set_inputs);
    c.bench_function_over_inputs(&format!("{}/bench_get", label), bench_get, get_inputs);
    c.bench_function_over_inputs(
        &format!("{}/bench_delete", label),
        bench_delete,
        delete_inputs,
    );
}

trait ClusterFactory<T: Simulator>: Clone + fmt::Debug + 'static {
    fn build(&self, nodes: usize) -> Cluster<T>;
}

#[derive(Clone)]
struct NodeClusterFactory;

impl ClusterFactory<NodeCluster> for NodeClusterFactory {
    fn build(&self, nodes: usize) -> Cluster<NodeCluster> {
        new_node_cluster(1, nodes)
    }
}

impl fmt::Debug for NodeClusterFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node")
    }
}

#[derive(Clone)]
struct ServerClusterFactory;

impl ClusterFactory<ServerCluster> for ServerClusterFactory {
    fn build(&self, nodes: usize) -> Cluster<ServerCluster> {
        new_server_cluster(1, nodes)
    }
}

impl fmt::Debug for ServerClusterFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Server")
    }
}

fn main() {
    violetabftstore::interlock::::config::check_max_open_fds(4096).unwrap();

    let mut criterion = Criterion::default().configure_from_args().sample_size(10);
    bench_violetabft_cluster(&mut criterion, NodeClusterFactory {}, "violetabftstore::node");
    bench_violetabft_cluster(&mut criterion, ServerClusterFactory {}, "violetabftstore::server");

    criterion.final_summary();
}
