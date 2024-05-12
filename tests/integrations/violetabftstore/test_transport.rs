// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use test_violetabftstore::*;
use test_util;

fn test_partition_write<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    must_get_equal(&cluster.engines[&1].kv.as_inner(), key, value);

    let brane_id = cluster.get_brane_id(key);

    // transfer leader to (1, 1)
    cluster.must_transfer_leader(brane_id, new_peer(1, 1));

    // leader in majority, partition doesn't affect write/read
    cluster.partition(vec![1, 2, 3], vec![4, 5]);
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    cluster.must_transfer_leader(brane_id, new_peer(1, 1));
    cluster.clear_lightlike_filters();

    // leader in minority, new leader should be elected
    cluster.partition(vec![1, 2], vec![3, 4, 5]);
    assert_eq!(cluster.must_get(key), Some(value.to_vec()));
    assert_ne!(cluster.leader_of_brane(brane_id).unwrap().get_id(), 1);
    assert_ne!(cluster.leader_of_brane(brane_id).unwrap().get_id(), 2);
    cluster.must_put(key, b"changed");
    cluster.clear_lightlike_filters();

    // when network recover, old leader should sync data
    cluster.reset_leader_of_brane(brane_id);
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), key, b"changed");
}

#[test]
fn test_node_partition_write() {
    let mut cluster = new_node_cluster(0, 5);
    test_partition_write(&mut cluster);
}

#[test]
fn test_server_partition_write() {
    let mut cluster = new_server_cluster(0, 5);
    test_partition_write(&mut cluster);
}

#[test]
fn test_secure_connect() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.causet.security = test_util::new_security_causet(None);
    cluster.run_conf_change();

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);

    for id in 1..4 {
        must_get_equal(&cluster.get_engine(id), key, value);
    }
}
