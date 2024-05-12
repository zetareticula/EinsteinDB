// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.
#![feature(min_specialization)]

#[allow(unused_extern_crates)]
extern crate edb_alloc;
#[macro_use]
extern crate lazy_static;

use serde::ser::{Serialize, SerializeStruct, Serializer};

macro_rules! define_error_codes {
    ($prefix:literal,
        $($name:ident => ($suffix:literal, $description:literal, $workaround:literal)),+
    ) => {
        use crate::ErrorCode;
        $(pub const $name: ErrorCode = ErrorCode {
            code: concat!($prefix, $suffix),
            description: $description,
            workaround: $workaround,
        };)+
        lazy_static! {
           pub static ref ALL_ERROR_CODES: Vec<ErrorCode> = vec![$($name,)+];
        }
    };
}

pub const UNKNOWN: ErrorCode = ErrorCode {
    code: "KV:Unknown",
    description: "",
    workaround: "",
};

pub mod codec;
pub mod interlock;
pub mod encryption;
pub mod engine;
pub mod fidel;
pub mod violetabft;
pub mod violetabftstore;
pub mod sst_importer;
pub mod causet_storage;

use std::fmt::{self, Display, Formatter};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct ErrorCode {
    pub code: &'static str,
    pub description: &'static str,
    pub workaround: &'static str,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code)
    }
}

impl Serialize for ErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ErrorCode", 3)?;
        state.serialize_field("error", &self.code)?;
        state.serialize_field("description", &self.description)?;
        state.serialize_field("workaround", &self.workaround)?;
        state.lightlike()
    }
}

pub trait ErrorCodeExt {
    fn error_code(&self) -> ErrorCode;
}

impl<T> ErrorCodeExt for T {
    default fn error_code(&self) -> ErrorCode {
        UNKNOWN
    }
}

#[causet(test)]
mod tests {
    #[test]
    fn test_define_error_code() {
        define_error_codes!(
            "KV:VioletaBftstore:",

            ENTRY_TOO_LARGE => ("EntryTooLarge", "", ""),
            NOT_LEADER => ("NotLeader", "", "")
        );

        assert_eq!(
            ENTRY_TOO_LARGE,
            ErrorCode {
                code: "KV:VioletaBftstore:EntryTooLarge",
                description: "",
                workaround: "",
            }
        );
        assert_eq!(
            NOT_LEADER,
            ErrorCode {
                code: "KV:VioletaBftstore:NotLeader",
                description: "",
                workaround: "",
            }
        );
    }
}
