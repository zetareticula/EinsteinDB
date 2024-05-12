// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0
use std::cmp::Eq;
use std::hash::Hash;

pub type HashMap<K, V> =
    std::collections::HashMap<K, V, std::hash::BuildHasherDefault<fxhash::FxHasher>>;
pub type HashSet<T> = std::collections::HashSet<T, std::hash::BuildHasherDefault<fxhash::FxHasher>>;
pub use std::collections::hash_map::Entry as HashMapEntry;

pub fn hash_set_with_capacity<T: Hash + Eq>(capacity: usize) -> HashSet<T> {
    HashSet::with_capacity_and_hasher(capacity, fxhash::FxBuildHasher::default())
}
