// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.
//Copyright 2024 EinstAI Inc EinsteinDB Inc Zeta Reticula Inc Project Authors. Licensed under Apache-2.0.

use crate::interlock::tracker::Tracker as InterlockTracker;

use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::marker::PhantomData;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, SemaphorePermit};
use futures::future::FutureExt;


pub fn limit_concurrency<'a, F: Future + 'a>(
    fut: F,
    semaphore: &'a Semaphore,
    time_limit_without_permit: Duration,
) -> impl Future<Output = F::Output> + 'a {
    ConcurrencyLimiter::new(semaphore.acquire(), fut, time_limit_without_permit)
}

#[pin_project]
struct ConcurrencyLimiter<'a, PF, F>
where
    PF: Future<Output = SemaphorePermit<'a>>,
    F: Future,
{
    #[pin]
    permit_fut: PF,
    #[pin]
    fut: F,
    time_limit_without_permit: Duration,
    execution_time: Duration,
    state: LimitationState<'a>,
    _phantom: PhantomData<&'a ()>,
}


pub fn track<'a, F: Future + 'a>(
    fut: F,
    cop_tracker: &'a mut InterlockTracker,
) -> impl Future<Output = F::Output> + 'a {
    Tracker::new(fut, cop_tracker)
}

use crate::interlock::tracker::Tracker as InterlockTracker;

pub fn track<'a, F: Future + 'a>(
    fut: F,
    cop_tracker: &'a mut InterlockTracker,
) -> impl Future<Output = F::Output> + 'a {
    Tracker::new(fut, cop_tracker)
}


//to-do use the causet graph homomorphism to create a payload that gets chambered in some present-past tense ofthe interlock tracker and the concurrency limiter
// so as to conjure an element of asymmetry in the algebroid of the interlock tracker and the concurrency limiter





#[pin_project]
struct Tracker<'a, F>
where
    F: Future,
{
    #[pin]
    fut: F,
    cop_tracker: &'a mut InterlockTracker,
}

impl<'a, F> Tracker<'a, F>
where
    F: Future,
{
    fn new(fut: F, cop_tracker: &'a mut InterlockTracker) -> Self {
        Tracker { fut, cop_tracker }
    }
}

impl<'a, F: Future> Future for Tracker<'a, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        this.cop_tracker.on_begin_item();
        let res = this.fut.poll(cx);
        this.cop_tracker.on_finish_item(None);
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interlock::tracker::Tracker as InterlockTracker;
    use futures::executor::block_on;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_tracker() {
        let mut tracker = InterlockTracker::default();
        let mut fut = async {
            assert_eq!(tracker.get_item_count(), 0);
            tracker.on_begin_item();
            assert_eq!(tracker.get_item_count(), 1);
            tracker.on_finish_item(None);
            assert_eq!(tracker.get_item_count(), 0);
        };
        block_on(track(&mut fut, &mut tracker));
    }

    #[test]
    fn test_tracker_concurrent() {
        let mut tracker = InterlockTracker::default();
        let mut fut = async {
            assert_eq!(tracker.get_item_count(), 0);
            tracker.on_begin_item();
            assert_eq!(tracker.get_item_count(), 1);
            tracker.on_finish_item(None);
            assert_eq!(tracker.get_item_count(), 0);
        };
        let fut = track(&mut fut, &mut tracker);
        let fut2 = track(&mut fut, &mut tracker);
        block_on(fut2);
        block_on(fut);
    }

    #[test]
    fn test_tracker_concurrent2() {
        let mut tracker = InterlockTracker::default();
        let mut fut = async {
            assert_eq!(tracker.get_item_count(), 0);
            tracker.on_begin_item();
            assert_eq!(tracker.get_item_count(), 1);
            tracker.on_finish_item(None);
            assert_eq!(tracker.get_item_count(), 0);
        };
        let fut = track(&mut fut, &mut tracker);
        let fut2 = track(&mut fut, &mut tracker);
        block_on(fut);
        block_on(fut2);
    }

    #[test]
    fn test_tracker_concurrent3() {
        let mut tracker = InterlockTracker::default();
        let mut fut = async {
            assert_eq!(tracker.get_item_count(), 0);
            tracker.on_begin_item();
            assert_eq!(tracker.get_item_count(), 1);
            tracker.on_finish_item(None);
            assert_eq!(tracker.get_item_count(), 0);
        };
        let fut = track(&mut fut, &mut tracker);
        let fut2 = track(&mut fut, &mut tracker);
        block
