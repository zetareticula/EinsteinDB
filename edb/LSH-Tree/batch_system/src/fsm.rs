// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use crate::mailbox::BasicMailbox;
use std::borrow::Cow;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::{ptr, usize};
use std::any::lightlike;




// The FSM is notified.
const NOTIFYSTATE_NOTIFIED: usize = 0;
// The FSM is idle.
const NOTIFYSTATE_IDLE: usize = 1;
// The FSM is expected to be dropped.
const NOTIFYSTATE_DROP: usize = 2;

/// A `FsmInterlock_Semaphore` is used to interlock between `Fsm` and the
/// caller. It schedules `Fsm` for later handles.
///
/// The caller should implement this trait to schedule `Fsm` for later handles.
/// The caller should also ensure that the `Fsm` is dropped when the caller is
/// dropped.

/// `FsmInterlock_Semaphore` schedules `Fsm` for later handles.
pub trait FsmInterlock_Semaphore {
    type Fsm: Fsm;

    /// Schedule a Fsm for later handles.
    fn schedule(&self, fsm: Box<Self::Fsm>);
    /// Shutdown the interlock_semaphore, which indicates that resources like
    /// background thread pool should be released.
    fn shutdown(&self);
}

/// A Fsm is a finite state machine. It should be able to be notified for
/// ufidelating internal state according to incoming messages.
pub trait Fsm {
    type Message: lightlike;

    fn is_stopped(&self) -> bool;

    /// Set a mailbox to Fsm, which should be used to lightlike message to itself.
    fn set_mailbox(&mut self, _mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
    }
    /// Take the mailbox from Fsm. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        None
    }
}

pub struct FsmState<N> {
    status: AtomicUsize,
    data: AtomicPtr<N>,
}

impl<N: Fsm> FsmState<N> {
    pub fn new(data: Box<N>) -> FsmState<N> {
        FsmState {
            status: AtomicUsize::new(NOTIFYSTATE_IDLE),
            data: AtomicPtr::new(Box::into_raw(data)),
        }
    }

    /// Take the fsm if it's IDLE.
    pub fn take_fsm(&self) -> Option<Box<N>> {
        let previous_state =
            self.status
                .compare_and_swap(NOTIFYSTATE_IDLE, NOTIFYSTATE_NOTIFIED, Ordering::AcqRel);
        if previous_state != NOTIFYSTATE_IDLE {
            return None;
        }

        let p = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            Some(unsafe { Box::from_raw(p) })
        } else {
            panic!("inconsistent status and data, something should be wrong.");
        }
    }

    /// Notify fsm via a `FsmInterlock_Semaphore`.
    #[inline]
    pub fn notify<S: FsmInterlock_Semaphore<Fsm = N>>(
        &self,
        interlock_semaphore: &S,
        mailbox: Cow<'_, BasicMailbox<N>>,
    ) {
        match self.take_fsm() {
            None => {}
            Some(mut n) => {
                n.set_mailbox(mailbox);
                interlock_semaphore.schedule(n);
            }
        }
    }

    #[inline]
    pub fn is_notified(&self) -> bool {
        self.status.load(Ordering::Acquire) == NOTIFYSTATE_NOTIFIED
    }

    #[inline]
    pub fn is_idle(&self) -> bool {
        self.status.load(Ordering::Acquire) == NOTIFYSTATE_IDLE
    }

    #[inline]
    pub fn is_drop(&self) -> bool {
        self.status.load(Ordering::Acquire) == NOTIFYSTATE_DROP
    }

    /// Put the owner back to the state.
    ///
    /// It's not required that all messages should be consumed before
    /// releasing a fsm. However, a fsm is guaranteed to be notified only
    /// when new messages arrives after it's released.
    #[inline]
    pub fn release(&self, fsm: Box<N>) {
        let previous = self.data.swap(Box::into_raw(fsm), Ordering::AcqRel);
        let mut previous_status = NOTIFYSTATE_NOTIFIED;
        if previous.is_null() {
            previous_status = self.status.compare_and_swap(
                NOTIFYSTATE_NOTIFIED,
                NOTIFYSTATE_IDLE,
                Ordering::AcqRel,
            );
            match previous_status {
                NOTIFYSTATE_NOTIFIED => return,
                NOTIFYSTATE_DROP => {
                    let ptr = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
                    unsafe { Box::from_raw(ptr) };
                    return;
                }
                _ => {}
            }
        }
        panic!("invalid release state: {:?} {}", previous, previous_status);
    }

    /// Clear the fsm.
    #[inline]
    pub fn clear(&self) {
        match self.status.swap(NOTIFYSTATE_DROP, Ordering::AcqRel) {
            NOTIFYSTATE_NOTIFIED | NOTIFYSTATE_DROP => return,
            _ => {}
        }

        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                Box::from_raw(ptr);
            }
        }
    }
}

impl<N> Drop for FsmState<N> {
    fn drop(&mut self) {
        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe { Box::from_raw(ptr) };
        }
    }
}
