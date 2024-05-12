// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Mutex};
use std::time::*;
use std::{fs, io, mem, thread};

use violetabft::evioletabft_timeshare::MessageType;

use violetabftstore::store::*;
use test_violetabftstore::*;
use violetabftstore::interlock::::config::*;
use violetabftstore::interlock::::HandyRwLock;

#[test]
fn test_overlap_cleanup() {
    let mut cluster = new_node_cluster(0, 3);
    // Disable violetabft log gc in this test case.
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::secs(60);

    let gen_snapshot_fp = "brane_gen_snap";

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();

    let brane_id = cluster.run_conf_change();
    fidel_client.must_add_peer(brane_id, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    cluster.must_transfer_leader(brane_id, new_peer(2, 2));
    // This will only pause the bootstrapped brane, so the split brane
    // can still work as expected.
    fail::causet(gen_snapshot_fp, "pause").unwrap();
    fidel_client.must_add_peer(brane_id, new_peer(3, 3));
    cluster.must_put(b"k3", b"v3");
    assert_snapshot(&cluster.get_snap_dir(2), brane_id, true);
    let brane1 = cluster.get_brane(b"k1");
    cluster.must_split(&brane1, b"k2");
    // Wait till the snapshot of split brane is applied, whose cone is ["", "k2").
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    // Resume the fail point and pause it again. So only the paused snapshot is generated.
    // And the paused snapshot's cone is ["", ""), hence overlap.
    fail::causet(gen_snapshot_fp, "pause").unwrap();
    // Overlap snapshot should be deleted.
    assert_snapshot(&cluster.get_snap_dir(3), brane_id, false);
    fail::remove(gen_snapshot_fp);
}

// When resolving remote address, all messages will be dropped and
// report unreachable. However unreachable won't reset follower's
// progress if it's in Snapshot state. So trying to lightlike a snapshot
// when the address is being resolved will leave follower's progress
// stay in Snapshot forever.
#[test]
fn test_server_snapshot_on_resolve_failure() {
    let mut cluster = new_server_cluster(1, 4);
    configure_for_snapshot(&mut cluster);

    let on_resolve_fp = "transport_snapshot_on_resolve";
    let on_lightlike_store_fp = "transport_on_lightlike_store";

    let fidel_client = Arc::clone(&cluster.fidel_client);
    // Disable default max peer count check.
    fidel_client.disable_default_operator();
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    fidel_client.must_remove_peer(1, new_peer(4, 4));
    cluster.must_put(b"k1", b"v1");

    let ready_notify = Arc::default();
    let (notify_tx, notify_rx) = mpsc::channel();
    cluster.sim.write().unwrap().add_lightlike_filter(
        1,
        Box::new(MessageTypeNotifier::new(
            MessageType::MsgSnapshot,
            notify_tx,
            Arc::clone(&ready_notify),
        )),
    );

    let (drop_snapshot_tx, drop_snapshot_rx) = mpsc::channel();
    cluster
        .sim
        .write()
        .unwrap()
        .add_recv_filter(4, Box::new(DropSnapshotFilter::new(drop_snapshot_tx)));

    fidel_client.add_peer(1, new_peer(4, 5));

    // The leader is trying to lightlike snapshots, but the filter drops snapshots.
    drop_snapshot_rx
        .recv_timeout(Duration::from_secs(3))
        .unwrap();

    // "return(4)" those failure occurs if EinsteinDB resolves or lightlikes to store 4.
    fail::causet(on_resolve_fp, "return(4)").unwrap();
    fail::causet(on_lightlike_store_fp, "return(4)").unwrap();

    // We are ready to recv notify.
    ready_notify.store(true, Ordering::SeqCst);
    notify_rx.recv_timeout(Duration::from_secs(3)).unwrap();

    let engine4 = cluster.get_engine(4);
    must_get_none(&engine4, b"k1");
    cluster.sim.write().unwrap().clear_recv_filters(4);

    // Remove the on_lightlike_store_fp.
    // Now it will resolve the store 4's address via heartbeat messages,
    // so snapshots works fine.
    //
    // But keep the on_resolve_fp.
    // Any snapshot messages that has been sent before will meet the
    // injected resolve failure eventually.
    // It perverts a race condition, remove the on_resolve_fp before snapshot
    // messages meet the failpoint, that fails the test.
    fail::remove(on_lightlike_store_fp);

    notify_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&engine4, b"k1", b"v1");
    must_get_equal(&engine4, b"k2", b"v2");

    // Clean up.
    fail::remove(on_resolve_fp);
}

#[test]
fn test_generate_snapshot() {
    let mut cluster = new_server_cluster(1, 5);
    cluster.causet.violetabft_store.violetabft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.causet.violetabft_store.violetabft_log_gc_count_limit = 8;
    cluster.causet.violetabft_store.merge_max_log_gap = 3;
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.stop_node(4);
    cluster.stop_node(5);
    (0..10).for_each(|_| cluster.must_put(b"k2", b"v2"));
    // Sleep for a while to ensure all logs are compacted.
    thread::sleep(Duration::from_millis(100));

    fail::causet("snapshot_delete_after_lightlike", "pause").unwrap();

    // Let store 4 inform leader to generate a snapshot.
    cluster.run_node(4).unwrap();
    must_get_equal(&cluster.get_engine(4), b"k2", b"v2");

    fail::causet("snapshot_enter_do_build", "pause").unwrap();
    cluster.run_node(5).unwrap();
    thread::sleep(Duration::from_millis(100));

    fail::causet("snapshot_delete_after_lightlike", "off").unwrap();
    must_empty_dir(cluster.get_snap_dir(1));

    // The task is droped so that we can't get the snapshot on store 5.
    fail::causet("snapshot_enter_do_build", "pause").unwrap();
    must_get_none(&cluster.get_engine(5), b"k2");

    fail::causet("snapshot_enter_do_build", "off").unwrap();
    must_get_equal(&cluster.get_engine(5), b"k2", b"v2");

    fail::remove("snapshot_enter_do_build");
    fail::remove("snapshot_delete_after_lightlike");
}

fn must_empty_dir(path: String) {
    for _ in 0..500 {
        thread::sleep(Duration::from_millis(10));
        if fs::read_dir(&path).unwrap().count() == 0 {
            return;
        }
    }

    let entries = fs::read_dir(&path)
        .and_then(|dir| dir.collect::<io::Result<Vec<_>>>())
        .unwrap();
    if !entries.is_empty() {
        panic!(
            "the directory {:?} should be empty, but has entries: {:?}",
            path, entries
        );
    }
}

fn assert_snapshot(snap_dir: &str, brane_id: u64, exist: bool) {
    let brane_id = format!("{}", brane_id);
    let timer = Instant::now();
    loop {
        for p in fs::read_dir(&snap_dir).unwrap() {
            let name = p.unwrap().file_name().into_string().unwrap();
            let mut parts = name.split('_');
            parts.next();
            if parts.next().unwrap() == brane_id && exist {
                return;
            }
        }
        if !exist {
            return;
        }

        if timer.elapsed() < Duration::from_secs(6) {
            thread::sleep(Duration::from_millis(20));
        } else {
            panic!(
                "assert snapshot [exist: {}, brane: {}] fail",
                exist, brane_id
            );
        }
    }
}

#[test]
fn test_node_request_snapshot_on_split() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_request_snapshot(&mut cluster);
    cluster.run();

    let brane = cluster.get_brane(b"");
    // Make sure peer 2 does not in the plightlikeing state.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    for _ in 0..100 {
        cluster.must_put(&[7; 100], &[7; 100]);
    }
    cluster.must_transfer_leader(1, new_peer(3, 3));

    let split_fp = "apply_before_split_1_3";
    fail::causet(split_fp, "pause").unwrap();
    let (split_tx, split_rx) = mpsc::channel();
    cluster.split_brane(
        &brane,
        b"k1",
        Callback::Write(Box::new(move |_| {
            split_tx.lightlike(()).unwrap();
        })),
    );
    // Split is stopped on peer3.
    split_rx
        .recv_timeout(Duration::from_millis(100))
        .unwrap_err();

    // Request snapshot.
    let committed_index = cluster.must_request_snapshot(2, brane.get_id());

    // Install snapshot filter after requesting snapshot.
    let (tx, rx) = mpsc::channel();
    let notifier = Mutex::new(Some(tx));
    cluster.sim.wl().add_recv_filter(
        2,
        Box::new(RecvSnapshotFilter {
            notifier,
            brane_id: brane.get_id(),
        }),
    );
    // There is no snapshot as long as we pause the split.
    rx.recv_timeout(Duration::from_millis(500)).unwrap_err();

    // Continue split.
    fail::remove(split_fp);
    split_rx.recv().unwrap();
    let mut m = rx.recv().unwrap();
    let snapshot = m.take_message().take_snapshot();

    // Requested snapshot_index >= committed_index.
    assert!(
        snapshot.get_metadata().get_index() >= committed_index,
        "{:?} | {}",
        m,
        committed_index
    );
}

// A peer on store 3 is isolated and is applying snapshot. (add failpoint so it's always plightlikeing)
// Then two conf change happens, this peer is removed and a new peer is added on store 3.
// Then isolation clear, this peer will be destroyed because of a bigger peer id in msg.
// In previous implementation, peer fsm can be destroyed synchronously because snapshot state is
// plightlikeing and can be canceled, but panic may happen if the applyfsm runs very slow.
#[test]
fn test_destroy_peer_on_plightlikeing_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    fidel_client.must_add_peer(r1, new_peer(2, 2));
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");
    // Ensure peer 3 is initialized.
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));

    for i in 0..20 {
        cluster.must_put(format!("k1{}", i).as_bytes(), b"v1");
    }

    let apply_snapshot_fp = "apply_plightlikeing_snapshot";
    fail::causet(apply_snapshot_fp, "return()").unwrap();

    cluster.clear_lightlike_filters();
    // Wait for leader lightlike snapshot.
    sleep_ms(100);

    cluster.add_lightlike_filter(IsolationFilterFactory::new(3));
    // Don't lightlike check stale msg to FIDel
    let peer_check_stale_state_fp = "peer_check_stale_state";
    fail::causet(peer_check_stale_state_fp, "return()").unwrap();

    fidel_client.must_remove_peer(r1, new_peer(3, 3));
    fidel_client.must_add_peer(r1, new_peer(3, 4));

    let before_handle_normal_3_fp = "before_handle_normal_3";
    fail::causet(before_handle_normal_3_fp, "pause").unwrap();

    cluster.clear_lightlike_filters();
    // Wait for leader lightlike msg to peer 3.
    // Then destroy peer 3 and create peer 4.
    sleep_ms(100);

    fail::remove(apply_snapshot_fp);

    fail::remove(before_handle_normal_3_fp);

    cluster.must_put(b"k120", b"v1");
    // After peer 4 has applied snapshot, data should be got.
    must_get_equal(&cluster.get_engine(3), b"k120", b"v1");
}

#[test]
fn test_shutdown_when_snap_gc() {
    let mut cluster = new_node_cluster(0, 2);
    // So that batch system can handle a snap_gc event before shutting down.
    cluster.causet.violetabft_store.store_batch_system.max_batch_size = 1;
    cluster.causet.violetabft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(20);
    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    // Only save a snapshot on peer 2, but do not apply it really.
    fail::causet("skip_schedule_applying_snapshot", "return").unwrap();
    fidel_client.must_add_peer(r1, new_learner_peer(2, 2));

    // Snapshot directory on store 2 shouldn't be empty.
    let snap_dir = cluster.get_snap_dir(2);
    for i in 0..=100 {
        if i == 100 {
            panic!("store 2 snap dir must not be empty");
        }
        let dir = fs::read_dir(&snap_dir).unwrap();
        if dir.count() > 0 {
            break;
        }
        sleep_ms(10);
    }

    fail::causet("peer_2_handle_snap_mgr_gc", "pause").unwrap();
    std::thread::spawn(|| {
        // Sleep a while to wait snap_gc event to reach batch system.
        sleep_ms(500);
        fail::causet("peer_2_handle_snap_mgr_gc", "off").unwrap();
    });

    sleep_ms(100);
    cluster.stop_node(2);

    let snap_dir = cluster.get_snap_dir(2);
    let dir = fs::read_dir(&snap_dir).unwrap();
    if dir.count() == 0 {
        panic!("store 2 snap dir must not be empty");
    }
}

// Test if a peer handle the old snapshot properly.
#[test]
fn test_receive_old_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    cluster.causet.violetabft_store.right_derive_when_split = true;

    let fidel_client = Arc::clone(&cluster.fidel_client);
    fidel_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    // Bypass the snapshot gc because the snapshot may be used twice.
    let peer_2_handle_snap_mgr_gc_fp = "peer_2_handle_snap_mgr_gc";
    fail::causet(peer_2_handle_snap_mgr_gc_fp, "return()").unwrap();

    fidel_client.must_add_peer(r1, new_peer(2, 2));
    fidel_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_transfer_leader(r1, new_peer(1, 1));

    cluster.must_put(b"k00", b"v1");
    // Ensure peer 2 is initialized.
    must_get_equal(&cluster.get_engine(2), b"k00", b"v1");

    cluster.add_lightlike_filter(IsolationFilterFactory::new(2));

    for i in 0..20 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }

    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let recv_filter = Box::new(
        BranePacketFilter::new(r1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgSnapshot)
            .reserve_dropped(Arc::clone(&dropped_msgs)),
    );
    cluster.sim.wl().add_recv_filter(2, recv_filter);

    cluster.clear_lightlike_filters();

    for _ in 0..20 {
        let guard = dropped_msgs.dagger().unwrap();
        if !guard.is_empty() {
            break;
        }
        drop(guard);
        sleep_ms(10);
    }
    let msgs = {
        let mut guard = dropped_msgs.dagger().unwrap();
        if guard.is_empty() {
            drop(guard);
            panic!("do not receive snapshot msg in 200ms");
        }
        mem::replace(guard.as_mut(), vec![])
    };

    cluster.sim.wl().clear_recv_filters(2);

    for i in 20..40 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v1");
    }
    must_get_equal(&cluster.get_engine(2), b"k39", b"v1");

    let router = cluster.sim.wl().get_router(2).unwrap();
    // lightlike the old snapshot
    for violetabft_msg in msgs {
        router.lightlike_violetabft_message(violetabft_msg).unwrap();
    }

    cluster.must_put(b"k40", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k40", b"v1");

    fidel_client.must_remove_peer(r1, new_peer(2, 2));

    must_get_none(&cluster.get_engine(2), b"k40");

    let brane = cluster.get_brane(b"k1");
    cluster.must_split(&brane, b"k5");

    let left = cluster.get_brane(b"k1");
    fidel_client.must_add_peer(left.get_id(), new_peer(2, 4));

    cluster.must_put(b"k11", b"v1");
    // If peer 2 handles previous old snapshot properly and does not leave over metadata
    // in `plightlikeing_snapshot_branes`, peer 4 should be created normally.
    must_get_equal(&cluster.get_engine(2), b"k11", b"v1");

    fail::remove(peer_2_handle_snap_mgr_gc_fp);
}
