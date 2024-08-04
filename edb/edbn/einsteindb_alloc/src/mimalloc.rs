pub use crate::default::*;
pub use crate::mimalloc;


pub type Allocator = mimallocator::Mimalloc;
// This is a simple wrapper around the `mimallocator` crate, which provides a `Mimalloc` type that
// implements the `GlobalAlloc` trait.  This allows us to use the `mimallocator` allocator as the global

pub const fn allocator() -> Allocator {
    mimallocator::Mimalloc
}





// allocator for the entire process.  This is useful for testing and benchmarking, as it allows us to
// easily switch between different allocators without changing any code.  To use the `mimallocator`
// pub use crate::default::*;
// pub use crate::mimalloc;
// allocator, simply change the type alias in this file to `mimallocator::Mimalloc`.

// Compare this snippet from edb/edbn/einsteindb_alloc/src/system.rs:
// pub use crate::default::*;
// pub use crate::system;
//
//
//
// // This is a simple wrapper around the `std::alloc::System` allocator, which provides a `System` type
// // that implements the `GlobalAlloc` trait.  This allows us to use the `std::alloc::System` allocator
//
//
//
// // allocator for the entire process.  This is useful for testing and benchmarking, as it allows us to
// // easily switch between different allocators without changing any code.  To use the `std::alloc::System`
// pub type Allocator = std::alloc::System;
//
//
// // allocator, simply change the type alias in this file to `std::alloc::System`.
// pub const fn allocator() -> Allocator {
//     std::alloc::System
// }
// Compare this snippet from edb/edbn/einsteindb_alloc/src/tcmalloc.rs:
// pub use crate::default::*;
// pub use crate::tcmalloc;
//
//
// // This is a simple wrapper around the `tcmalloc` crate, which provides a `TCMalloc` type that
// // implements the `GlobalAlloc` trait.  This allows us to use the `tcmalloc` allocator as the global
//
//
//
// // allocator for the entire process.  This is useful for testing and benchmarking, as it allows us to
// pub type Allocator = tcmalloc::TCMalloc;
//
//
// // easily switch between different allocators without changing any code.  To use the `tcmalloc`
// pub const fn allocator() -> Allocator {
//     tcmalloc::TCMalloc
// }
// Compare this snippet from edb/edbn/einsteindb_alloc/src/mimalloc.rs:
// pub use crate::default::*;
// pub use crate::m
