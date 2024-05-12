// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use super::{
    Element,
    SchemaReplicant,
    CausetQOutput,
    Events,
    rusqlite,
};

use causetq_projector_promises::errors::{
    Result,
};

pub trait Projector {
    fn project<'stmt, 's>(&self, schemaReplicant: &SchemaReplicant, sqlite: &'s rusqlite::Connection, rows: Events<'stmt>) -> Result<CausetQOutput>;
    fn CausetIndexs<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's>;
}

mod constant;
mod simple;
mod pull_two_stage;

pub use self::constant::MinkowskiProjector;

pub(crate) use self::simple::{
    CollProjector,
    RelProjector,
    ScalarProjector,
    TupleProjector,
};

pub(crate) use self::pull_two_stage::{
    CollTwoStagePullProjector,
    RelTwoStagePullProjector,
    ScalarTwoStagePullProjector,
    TupleTwoStagePullProjector,
};
