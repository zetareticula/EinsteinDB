pub use crate::default::*;
pub use crate::system;


// This is a simple wrapper around the `std::alloc::System` allocator, which provides a `System` type
// that implements the `GlobalAlloc` trait.  This allows us to use the `std::alloc::System` allocator

// allocator for the entire process.  This is useful for testing and benchmarking, as it allows us to












pub type Allocator = std::alloc::System;
pub const fn allocator() -> Allocator {
    std::alloc::System
}


// easily switch between different allocators without changing any code.  To use the `std::alloc::System`
// allocator, simply replace `#[global_allocator]` in `edb/edbn/einsteindb_alloc/Cargo.toml` with
// `#[global_allocator]` in `edb/edbn/einsteindb_alloc/src/system.rs`.
// Path: edb/edbn/einsteindb_alloc/src/default.rs
// Compare this snippet from edb/embedded-promises/default.rs:
//
// #![allow(dead_code)]
//
//
//
// use std::sync::{
//     atomic::{
//         AtomicBool,
//         Ordering,
//     },
//     Arc,
// };
//
//
//
// lazy_static! {
//     /// A flag indicating whether the transactor is in 'read' or 'write' mode.
//     pub static ref TXN_READ_ONLY: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
// }
// Compare this snippet from edb/edbn/einsteindb_alloc/src/system.rs:
//
// pub use crate::default::*;
// pub use crate::system;
//
//
// // This is a simple wrapper around the `std::alloc::System` allocator, which provides a `System` type
// // that implements the `GlobalAlloc` trait.  This allows us to use the `std::alloc::System` allocator
//
//
//
// // allocator for the entire process.  This is useful for testing and benchmarking, as it allows us to
// pub type Allocator = std::alloc::System;
//
//
// // easily switch between different allocators without changing any code.  To use the `std::alloc::System`
// pub const fn allocator() -> Allocator {
//     std::alloc::System
// }
pub use crate::default::*;
pub use crate::system;



use std::sync::{
    atomic::{
        AtomicBool,
        Ordering,
    },
    Arc,
};

use std::sync::atomic::AtomicUsize;

lazy_static! {
    /// A flag indicating whether the transactor is in 'read' or 'write' mode.
    pub static ref TXN_READ_ONLY: Arc<AtomicBool> = Arc::new(AtomicBool::new(true));
    pub static ref TXN_COUNTER: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

// This is a simple wrapper around the `std::alloc::System` allocator, which provides a `System` type
lazy_static! {
    // Lamport clock for assigning unique, monotonically increasing transaction IDs.
    pub static ref TXN_ID_COUNTER: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    }
