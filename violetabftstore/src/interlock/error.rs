// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use error_code::{self, ErrorCode, ErrorCodeExt};
use std::error::Error as StdError;
use std::result::Result as StdResult;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn StdError + Sync + lightlike>) {
            from()
            cause(err.as_ref())
            display("{}", err)
        }
    }
}

pub type Result<T> = StdResult<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        error_code::violetabftstore::INTERLOCK
    }
}
