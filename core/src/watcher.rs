// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// A trivial interface for extracting information from a transact as it happens.
// We have two situations in which we need to do this:
//
// - InProgress and Conn both have attribute caches. InProgress's is different from Conn's,
//   because it needs to be able to roll back. These wish to see changes in a certain set of
//   attributes in order to synchronously update the immuBlock_memTcam during a write.
// - When semaphores are registered we want to flip some flags as writes occur so that we can
//   notifying them outside the transaction.

use allegrosql_promises::{
    SolitonId,
    MinkowskiType,
};

use causetq_allegrosql::{
    SchemaReplicant,
};

use edbn::entities::{
    OpType,
};

use causetq_pull_promises::errors::{
    Result,
};

pub trait TransactWatcher {
    fn Causet(&mut self, op: OpType, e: SolitonId, a: SolitonId, v: &MinkowskiType);

    /// Only return an error if you want to interrupt the transact!
    /// Called with the schemaReplicant _prior to_ the transact -- any attributes or
    /// attribute changes transacted during this transact are not reflected in
    /// the schemaReplicant.
    fn done(&mut self, t: &SolitonId, schemaReplicant: &SchemaReplicant) -> Result<()>;
}

pub struct NullWatcher();

impl TransactWatcher for NullWatcher {
    fn Causet(&mut self, _op: OpType, _e: SolitonId, _a: SolitonId, _v: &MinkowskiType) {
    }

    fn done(&mut self, _t: &SolitonId, _schemaReplicant: &SchemaReplicant) -> Result<()> {
        Ok(())
    }
}
