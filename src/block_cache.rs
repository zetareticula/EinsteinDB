//! Simple LRU Block Cache implemented as a vector-of-vectors
//! This cache stores blocks (Vec<u8>) keyed by u64 identifiers and evicts
//! the least-recently-used block when capacity is exceeded.

use std::collections::HashMap;

#[derive(Debug)]
pub struct BlockCache {
    capacity: usize,
    // Keys aligned with blocks by index; serves as an access-ordered list (most recent at end)
    keys: Vec<u64>,
    // Blocks aligned with keys by index
    blocks: Vec<Vec<u8>>,
    // Fast lookup from key -> index in vectors
    index: HashMap<u64, usize>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        Self {
            capacity,
            keys: Vec::with_capacity(capacity),
            blocks: Vec::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
        }
    }

    /// Insert or update a block. Returns true if inserted, false if updated.
    pub fn put(&mut self, key: u64, data: Vec<u8>) -> bool {
        if let Some(&i) = self.index.get(&key) {
            // Update existing block and mark as most-recent by moving to tail
            self.blocks[i] = data;
            self.touch(i);
            return false;
        }

        // Evict if needed
        if self.keys.len() == self.capacity {
            self.evict_lru();
        }

        self.keys.push(key);
        self.blocks.push(data);
        self.index.insert(key, self.keys.len() - 1);
        true
    }

    /// Get a block slice by key and mark as most-recent
    pub fn get(&mut self, key: &u64) -> Option<&[u8]> {
        let idx = *self.index.get(key)?;
        self.touch(idx);
        self.blocks.last().map(|b| b.as_slice())
    }

    /// Current number of cached blocks
    pub fn len(&self) -> usize { self.keys.len() }
    pub fn is_empty(&self) -> bool { self.keys.is_empty() }

    fn evict_lru(&mut self) {
        if self.keys.is_empty() { return; }
        // Remove the least recently used: at the head (index 0)
        let old_key = self.keys.remove(0);
        self.blocks.remove(0);
        self.index.remove(&old_key);
        // Rebuild indices for shifted elements (O(n), acceptable for small demo)
        for (i, k) in self.keys.iter().enumerate() {
            self.index.insert(*k, i);
        }
    }

    fn touch(&mut self, idx: usize) {
        if idx + 1 == self.keys.len() { return; } // already MRU
        // Move the (key, block) pair to the end to mark as MRU
        let key = self.keys.remove(idx);
        let block = self.blocks.remove(idx);
        self.keys.push(key);
        self.blocks.push(block);
        // Rebuild indices for moved range
        for (i, k) in self.keys.iter().enumerate() {
            self.index.insert(*k, i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_get_and_evict() {
        let mut cache = BlockCache::new(2);
        assert!(cache.put(1, vec![1]));
        assert!(cache.put(2, vec![2]));
        assert_eq!(cache.get(&1).unwrap(), &[1]);
        // Access 1 so it becomes MRU, 2 becomes LRU
        assert!(cache.put(3, vec![3])); // should evict 2
        assert!(cache.get(&2).is_none());
        assert_eq!(cache.get(&1).unwrap(), &[1]);
        assert_eq!(cache.get(&3).unwrap(), &[3]);
    }
}
