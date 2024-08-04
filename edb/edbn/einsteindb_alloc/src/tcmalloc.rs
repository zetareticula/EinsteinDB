pub use crate::default::*;
pub use crate::tcmalloc;


// This is a simple wrapper around the `tcmalloc` crate, which provides a `TCMalloc` type that
// implements the `GlobalAlloc` trait.  This allows us to use the `tcmalloc` allocator as the global



// allocator for the entire process.  This is useful for testing and benchmarking, as it allows us to
pub type Allocator = tcmalloc::TCMalloc;


// easily switch between different allocators without changing any code.  To use the `tcmalloc`
pub const fn allocator() -> Allocator {
    tcmalloc::TCMalloc
}
