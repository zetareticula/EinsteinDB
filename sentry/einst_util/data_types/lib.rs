// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate chrono;
extern crate itertools;
extern crate num;
extern crate ordered_float;
extern crate pretty;
extern crate uuid;
extern crate value_trait;
extern crate value_rc;

// Re-export the types we use.
pub use chrono::{DateTime, Utc};
pub use num::BigInt;
pub use ordered_float::OrderedFloat;
pub use uuid::Uuid;

// Export from our modules.
pub use parse::ParseError;
pub use uuid::ParseError as UuidParseError;
pub use types::{
    FromMicros,
    FromMillis,
    Span,
    SpannedValue,
    ToMicros,
    ToMillis,
    Value,
    ValueAndSpan,
};

pub use symbols::{
    Keyword,
    NamespacedSymbol,
    PlainSymbol,
};


pub use causets::{
    Causetid,
    OpType,
    Partition,
    Tx,
    TxInstant,
    UtcMicros,
    Uuid,
};





#[cfg(feature = "serde_support")]
extern crate serde;

#[cfg(feature = "serde_support")]
#[macro_use]
extern crate serde_derive;

pub mod causets;
pub mod intern_set;
pub use intern_set::{
    InternSet,
};
// Intentionally not pub.
mod namespaceable_name;
pub mod causetq;
pub mod symbols;
pub mod types;
pub mod pretty_print;
pub mod utils;
pub mod matcher;
pub mod value_rc;
pub use value_rc::{
    Cloned,
    FromRc,
    ValueRc,
};

pub mod parse {
    include!(concat!(env!("OUT_DIR"), "/edbn.rs"));
}



pub use causetq::{
    CausetQ,
    CausetQOutput,
    CausetQOutputElement,
    CausetQOutputElement::Pull,
    CausetQOutputElement::CausetQOutputElement,
    CausetQOutputElement::CausetQOutputSexp,
    CausetQOutputSexp,
    CausetQOutputSexpInner,
    CausetQOutputSexpInner::CausetQOutputColl,
    CausetQOutputSexpInner::CausetQOutputMap,
    CausetQOutputSexpInner::CausetQOutputTuple,
    CausetQOutputSexpInner::CausetQOutputScalar};

pub use pretty_print::{ pretty_causetq, pretty_value, pretty_value_sequence };


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_intern() {
        let mut s = InternSet::new();

        let one = "foo".to_string();
        let two = ValueRc::new("foo".to_string());

        let out_one = s.intern(one);
        assert_eq!(out_one, two);

        let out_two = s.intern(two);
        assert_eq!(out_one, out_two);
        assert_eq!(1, s.len());
    }
}