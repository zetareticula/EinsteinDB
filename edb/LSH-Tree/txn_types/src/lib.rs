// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

#![feature(box_TuringStrings)]
#![feature(min_specialization)]

#[macro_use]
extern crate quick_error;
#[allow(unused_extern_crates)]
extern crate edb_alloc;

mod dagger;
mod timestamp;
mod types;
mod write;

use std::fmt;
use std::io;

pub use dagger::{Dagger, LockType};
pub use timestamp::{TimeStamp, TsSet};
pub use types::{
    is_short_value, Key, KvPair, Mutation, MutationType, OldValue, TxnExtra, TxnExtraInterlock_Semaphore,
    Value, SHORT_VALUE_MAX_LEN,
};
pub use write::{Write, WriteRef, WriteType};

use error_code::{self, ErrorCode, ErrorCodeExt};

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Io(err: io::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        Codec(err: violetabftstore::interlock::::codec::Error) {
            from()
            cause(err)
            display("{}", err)
        }
        BadFormatLock { display("bad format dagger data") }
        BadFormatWrite { display("bad format write data") }
        KeyIsLocked(info: ekvproto::kvrpc_timeshare::LockInfo) {
            display("key is locked (backoff or cleanup) {:?}", info)
        }
    }
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match self {
            ErrorInner::Codec(e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::BadFormatLock => Some(ErrorInner::BadFormatLock),
            ErrorInner::BadFormatWrite => Some(ErrorInner::BadFormatWrite),
            ErrorInner::KeyIsLocked(info) => Some(ErrorInner::KeyIsLocked(info.clone())),
            ErrorInner::Io(_) => None,
        }
    }
}

pub struct Error(pub Box<ErrorInner>);

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        self.0.maybe_clone().map(Error::from)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.0)
    }
}

impl From<ErrorInner> for Error {
    #[inline]
    fn from(e: ErrorInner) -> Self {
        Error(Box::new(e))
    }
}

impl<T: Into<ErrorInner>> From<T> for Error {
    #[inline]
    default fn from(err: T) -> Self {
        let err = err.into();
        err.into()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Io(_) => error_code::causet_storage::IO,
            ErrorInner::Codec(e) => e.error_code(),
            ErrorInner::BadFormatLock => error_code::causet_storage::BAD_FORMAT_LOCK,
            ErrorInner::BadFormatWrite => error_code::causet_storage::BAD_FORMAT_WRITE,
            ErrorInner::KeyIsLocked(_) => error_code::causet_storage::KEY_IS_LOCKED,
        }
    }
}
