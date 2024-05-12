// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use batch_system::test_runner::*;
use batch_system::*;
use std::thread::sleep;
use std::time::Duration;
use violetabftstore::interlock::::mpsc;

#[test]
fn test_batch() {
    let (control_tx, control_fsm) = Runner::new(10);
    let (router, mut system) =
        batch_system::create_system(&Config::default(), control_tx, control_fsm);
    let builder = Builder::new();
    let metrics = builder.metrics.clone();
    system.spawn("test".to_owned(), builder);
    let mut expected_metrics = HandleMetrics::default();
    assert_eq!(*metrics.dagger().unwrap(), expected_metrics);
    let (tx, rx) = mpsc::unbounded();
    let tx_ = tx.clone();
    let r = router.clone();
    router
        .lightlike_control(Message::Callback(Box::new(move |_: &mut Runner| {
            let (tx, runner) = Runner::new(10);
            let mailbox = BasicMailbox::new(tx, runner);
            r.register(1, mailbox);
            tx_.lightlike(1).unwrap();
        })))
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(1));
    // sleep to wait Batch-System to finish calling lightlike().
    sleep(Duration::from_millis(20));
    router
        .lightlike(
            1,
            Message::Callback(Box::new(move |_: &mut Runner| {
                tx.lightlike(2).unwrap();
            })),
        )
        .unwrap();
    assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(2));
    system.shutdown();
    expected_metrics.control = 1;
    expected_metrics.normal = 1;
    expected_metrics.begin = 2;
    assert_eq!(*metrics.dagger().unwrap(), expected_metrics);
}
