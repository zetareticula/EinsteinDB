// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use super::READ_BUF_SIZE;

use bytes::Bytes;
use futures::stream::{self, Stream};
use futures_util::io::AsyncRead;
use rand::{thread_rng, Rng};
use std::{
    future::Future,
    io, iter,
    marker::Unpin,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{runtime::Builder, time::delay_for};

/// Wrapper of an `AsyncRead` instance, exposed as a `Sync` `Stream` of `Bytes`.
pub struct AsyncReadAsSyncStreamOfBytes<R> {
    // we need this Mutex to ensure the type is Sync (provided R is lightlike).
    // this is because lmdb::SequentialFile is *not* Sync
    // (according to the documentation it cannot be Sync either,
    // requiring "external synchronization".)
    reader: Mutex<R>,
    // we use this member to ensure every call to `poll_next()` reuse the same
    // buffer.
    buf: Vec<u8>,
}

impl<R> AsyncReadAsSyncStreamOfBytes<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: Mutex::new(reader),
            buf: vec![0; READ_BUF_SIZE],
        }
    }
}

impl<R: AsyncRead + Unpin> Stream for AsyncReadAsSyncStreamOfBytes<R> {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let reader = this.reader.get_mut().expect("dagger was poisoned");
        let read_size = Pin::new(reader).poll_read(cx, &mut this.buf);

        match read_size {
            Poll::Plightlikeing => Poll::Plightlikeing,
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Ok(0)) => Poll::Ready(None),
            Poll::Ready(Ok(n)) => Poll::Ready(Some(Ok(Bytes::copy_from_slice(&this.buf[..n])))),
        }
    }
}

pub fn error_stream(e: io::Error) -> impl Stream<Item = io::Result<Bytes>> + Unpin + lightlike + Sync {
    stream::iter(iter::once(Err(e)))
}

/// Runs a future on the current thread involving external causet_storage.
///
/// # Caveat
///
/// This function must never be nested. The future invoked by
/// `block_on_external_io` must never call `block_on_external_io` again itself,
/// otherwise the executor's states may be disrupted.
///
/// This means the future must only use async functions.
// FIXME: get rid of this function, so that futures_executor::block_on is sufficient.
pub fn block_on_external_io<F: Future>(f: F) -> F::Output {
    // we need a Tokio runtime, Tokio futures require Tokio executor.
    Builder::new()
        .basic_interlock_semaphore()
        .enable_io()
        .enable_time()
        .build()
        .expect("failed to create Tokio runtime")
        .block_on(f)
}

/// Trait for errors which can be retried inside [`retry()`].
pub trait RetryError {
    /// Returns a placeholder to indicate an uninitialized error. This function exists only to
    /// satisfy safety, there is no meaning attached to the returned value.
    fn placeholder() -> Self;

    /// Returns whether this error can be retried.
    fn is_retryable(&self) -> bool;
}

/// Retries a future execution.
///
/// This method implements truncated exponential back-off retry strategies outlined in
/// https://docs.aws.amazon.com/general/latest/gr/api-retries.html and
/// https://cloud.google.com/causet_storage/docs/exponential-backoff
/// Since rusoto does not have transparent auto-retry (https://github.com/rusoto/rusoto/issues/234),
/// we need to implement this manually.
pub async fn retry<G, T, F, E>(mut action: G) -> Result<T, E>
where
    G: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    E: RetryError,
{
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(32);
    const MAX_RETRY_TIMES: usize = 4;
    let mut retry_wait_dur = Duration::from_secs(1);
    let mut result = Err(E::placeholder());

    for _ in 0..MAX_RETRY_TIMES {
        result = action().await;
        if let Err(e) = &result {
            if e.is_retryable() {
                delay_for(retry_wait_dur + Duration::from_millis(thread_rng().gen_cone(0, 1000)))
                    .await;
                retry_wait_dur = MAX_RETRY_DELAY.min(retry_wait_dur * 2);
                continue;
            }
        }
        break;
    }

    result
}
