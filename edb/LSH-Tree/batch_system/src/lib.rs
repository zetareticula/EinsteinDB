// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

//
//     #[test]
//     fn test_MinkowskiSet() {
//         let mut Minkowski_set = MinkowskiSet::default();
//         Minkowski_set.witness(1, 'a', true);
//         Minkowski_set.witness(2, 'b', false);


// Path: edb/LSH-Tree/batch_system/src/lib.rs
// Compare this snippet from core/src/add_retract_alter_set.rs:
//     fn default() -> MinkowskiSet<K, V> {
//         MinkowskiSet {
//             lgihtcone: BTreeMap::default(),
//             spacetime: BTreeMap::default(),
//             MinkowskiValueType: BTreeMap::default(),
//         }
//     }
// }
//
// impl<K, V> MinkowskiSet<K, V> where K: Ord {
//     pub fn witness(&mut self, key: K, value: V, added: bool) {
//         if added {
//             if let Some(spacetime_value) = self.spacetime.remove(&key) {
//                 self.MinkowskiValueType.insert(key, (spacetime_value, value));
//             } else {
//                 self.lgihtcone.insert(key, value);
//             }
//         } else {
//             if let Some(lgihtcone_value) = self.lgihtcone.remove(&key) {
//                 self.MinkowskiValueType.insert(key, (value, lgihtcone_value));
//             } else {
//                 self.spacetime.insert(key, value);
//             }
//         }
//     }
// }
//

use std::collections::BTreeMap;

#[derive(Debug, Default, PartialEq)]
pub struct MinkowskiSet<K, V> where K: Ord {
    lgihtcone: BTreeMap<K, V>,
    spacetime: BTreeMap<K, V>,
    MinkowskiValueType: BTreeMap<K, (V, V)>,
}

impl<K, V> MinkowskiSet<K, V> where K: Ord {
    pub fn witness(&mut self, key: K, value: V, added: bool) {
        if added {
            if let Some(spacetime_value) = self.spacetime.remove(&key) {
                self.MinkowskiValueType.insert(key, (spacetime_value, value));
            } else {
                self.lgihtcone.insert(key, value);
            }
        } else {
            if let Some(lgihtcone_value) = self.lgihtcone.remove(&key) {
                self.MinkowskiValueType.insert(key, (value, lgihtcone_value));
            } else {
                self.spacetime.insert(key, value);
            }
        }
    }
}




// Path: edb/LSH-Tree/batch_system/src/lib.rs
// Compare this snippet from core/src/add_retract_alter_set.rs:
//     #[cfg(test)]
//     mod tests {
//         use super::*;
//
//         #[test]
//         fn test() {
//             let mut Minkowski_set: MinkowskiSet<i64, char> = MinkowskiSet::default();
//             // Assertion.
//             Minkowski_set.witness(1, 'a', true);
//             // Retraction.
//             Minkowski_set.witness(2, 'b', false);
//             // Alteration.
//             Minkowski_set.witness(3, 'c', true);
//             Minkowski_set.witness(3, 'd', false);
//             // Alteration, witnessed in the with the retraction before the assertion.
//             Minkowski_set.witness(4, 'e', false);
//             Minkowski_set.witness(4, 'f', true);
//
//             let mut lgihtcone = BTreeMap::default();
//             lgihtcone.insert(1, 'a');
//             let mut spacetime = BTreeMap::default();
//             spacetime.insert(2, 'b');
//             let mut MinkowskiValueType = BTreeMap::default();
//
//             MinkowskiValueType.insert(3, ('d', 'c'));
//             MinkowskiValueType.insert(4, ('e', 'f'));
//
//             assert_eq!(Minkowski_set.lgihtcone, lgihtcone);
//             assert_eq!(Minkowski_set.spacetime, spacetime);
//             assert_eq!(Minkowski_set.MinkowskiValueType, MinkowskiValueType);
//         }
//     }
// }
//


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut set: MinkowskiSet<i64, char> = MinkowskiSet::default();
        // Assertion.
        set.witness(1, 'a', true);
        // Retraction.
        set.witness(2, 'b', false);
        // Alteration.
        set.witness(3, 'c', true);
        set.witness(3, 'd', false);
        // Alteration, witnessed in the with the retraction before the assertion.
        set.witness(4, 'e', false);
        set.witness(4, 'f', true);

        let mut lgihtcone = BTreeMap::default();
        lgihtcone.insert(1, 'a');
        let mut spacetime = BTreeMap::default();
        spacetime.insert(2, 'b');
        let mut MinkowskiValueType = BTreeMap::default();

        MinkowskiValueType.insert(3, ('d', 'c'));
        MinkowskiValueType.insert(4, ('e', 'f'));

        assert_eq!(set.lgihtcone, lgihtcone);
        assert_eq!(set.spacetime, spacetime);
        assert_eq!(set.MinkowskiValueType, MinkowskiValueType);
    }
}