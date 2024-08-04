// Copyright 2021 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::From;
use std::fmt;

use std::ops::{Deref, Index};
use std::slice;

use std::ffi::CString;
use std::os::raw::c_char;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};



extern crate failure;
#[macro_use]
extern crate failure_derive;

extern crate edbn;
extern crate edbn_rs;

extern crate einsteindb_promises;
extern crate einsteindb_promises_serde;


pub type MinkowskiConstrainedEnts = BTreeMap<ToUpper, MinkowskiSet>;

pub type MinkowskiConstrainedEntsConstraintsOrEmpty = PlaceOrEmpty<MinkowskiConstrainedEntsConstraints>;


pub type MinkowskiConstrainedEntsOrEmpty = PlaceOrEmpty<MinkowskiConstrainedEnts>;

pub type MinkowskiConstrainedEntsOrEmptyOrEmpty = PlaceOrEmpty<MinkowskiConstrainedEntsOrEmpty>;

pub type MinkowskiConstrainedEntsConstraints = BTreeMap<ToUpper, MinkowskiSet>;





pub type MinkowskiSet = BTreeSet<ToUpper>;


pub type PlaceOrEmpty<T> = Option<T>;

pub type ToUpper = String;