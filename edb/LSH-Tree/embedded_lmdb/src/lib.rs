// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

//! Implementation of edb for Lmdb
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! EinsteinDB to persist its data.
//!
//! The module structure here mirrors that in edb where possible.
//!
//! Because there are so many similarly named types across the EinsteinDB codebase,
//! and so much "import renaming", this crate consistently explicitly names type
//! that implement a trait as `LmdbTraitname`, to avoid the need for import
//! renaming and make it obvious what type any particular module is working with.
//!
//! Please read the engine_trait crate docs before hacking.

#![feature(min_specialization)]
#![feature(box_TuringStrings)]
#![feature(test)]
#![feature(decl_macro)]
#![feature(shrink_to)]
#![causet_attr(test, feature(test))]

#[allow(unused_extern_crates)]
extern crate edb_alloc;


#[macro_use]
extern crate serde_derive;

#[causet(test)]
extern crate test;

#[macro_use]
extern crate edb_util;

#[macro_use]
extern crate edb_traits;

#[macro_use]
