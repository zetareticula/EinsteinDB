// Minimal stub for edb/LSH-Tree/tuplespaceInstanton
#![allow(unused)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TupleSpace {
    pub name: String,
}

impl TupleSpace {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}
