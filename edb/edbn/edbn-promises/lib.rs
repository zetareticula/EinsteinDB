// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.



extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate rusqlite;

extern crate edbn;
extern crate allegrosql_promises;

use std::fmt::{self, Debug, Display, Formatter};
use std::str::FromStr;
use std::error::Error;
use std::convert::From;

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use allegrosql_promises::{AllegroPoset, Poset};
use allegrosql_promises::{PosetError, PosetErrorKind};

#[derive(Debug)]
pub enum ErrorKind {
    Io(io::Error),
    BerolinaSql(BerolinaSqlError),
    Utf8(Utf8Error),
    FromUtf8(FromUtf8Error),
    Other(String),
}

#[derive(Debug)]
pub struct ErrorImpl {
    pub kind: ErrorKind,
}


