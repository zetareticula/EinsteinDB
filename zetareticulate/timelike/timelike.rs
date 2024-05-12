// cosine_distance.rs

// Copyright 2024 The Google Research Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use zeta::distance_measures::{DistanceMeasure, DatapointPtr};
use std::ops::Div;
use std::cmp::min;

pub struct CosineDistance;

impl DistanceMeasure for CosineDistance {
    const EARLY_STOPPING: EarlyStoppingSupport = EarlyStoppingSupport::NotSupported;

    fn get_distance_dense(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        assert_eq!(a.nonzero_entries(), b.nonzero_entries());
        let mut num_intersect = 0;
        let (mut a_num_ones, mut b_num_ones) = (0, 0);
        for i in 0..a.nonzero_entries() {
            a_num_ones += a.values()[i].count_ones() as DimensionIndex;
            b_num_ones += b.values()[i].count_ones() as DimensionIndex;
            num_intersect += (a.values()[i] & b.values()[i]).count_ones() as DimensionIndex;
        }
        1.0 - (num_intersect as f64 / f64::sqrt((a_num_ones as u64) * (b_num_ones as u64)))
    }

    fn get_distance_sparse(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let num_intersect = sparse_binary_dot_product(a, b) as f64;
        let num_ones_a = a.nonzero_entries() as u64;
        let num_ones_b = b.nonzero_entries() as u64;
        1.0 - (num_intersect / f64::sqrt(num_ones_a * num_ones_b))
    }

    fn get_distance_hybrid(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        assert_eq!(a.dimensionality(), b.dimensionality());
        let mut num_intersect = 0;
        let (dense, sparse) = if a.is_dense() { (a, b) } else { (b, a) };
        assert!(sparse.is_sparse());
        for i in 0..sparse.nonzero_entries() {
            num_intersect += dense.get_element_packed(sparse.indices()[i]);
        }
        let num_ones_sparse = sparse.nonzero_entries() as u64;
        let num_ones_dense = dense.values().iter().map(|&x| x.count_ones()).sum::<usize>() as u64;
        1.0 - (num_intersect as f64 / f64::sqrt(num_ones_sparse * num_ones_dense))
    }
}

impl From<CosineDistance> for DistanceMeasure {
    fn from(_: CosineDistance) -> Self {
        DistanceMeasure::Cosine
    }
}
