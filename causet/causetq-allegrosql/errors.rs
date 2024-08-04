// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use failure::Fail;
use std::fmt;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;

/// The error type for building a causetsq query.
/// This is a simple enumeration of the various ways in which a query can fail.
///

#[derive(Debug, Fail)]
pub enum BuildCausetQError {
    #[fail(display = "invalid parameter name: {}", _0)]
    InvalidParameterName(String),

    #[fail(display = "parameter name could be generated: '{}'", _0)]
    BindParamCouldBeGenerated(String)
}



#[derive(Debug, Fail)]
pub enum SQLError {
    #[fail(display = "invalid parameter name: {}", _0)]
    InvalidParameterName(String),

    #[fail(display = "parameter name could be generated: '{}'", _0)]
    BindParamCouldBeGenerated(String)
}

pub type BuildCausetQResult = Result<(), SQLError>;

pub type SQLErrorResult = Result<(), SQLError>;


impl Display for BuildCausetQError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            BuildCausetQError::InvalidParameterName(ref name) => write!(f, "invalid parameter name: {}", name),
            BuildCausetQError::BindParamCouldBeGenerated(ref name) => write!(f, "parameter name could be generated: '{}'", name)
        }
    }
}