// Copyright 2024 The Zeta Reticula Research Authors.
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

use crate::absl::flat_hash_set::FlatHashSet;
use crate::absl::Span;
use crate::dataset::{Datapoint, Dataset};
use crate::distance_measures::DistanceMeasure;
use crate::distance_measures::l2_distance::L2Distance;
use crate::gmm_utils::GmmUtils;
use crate::oss_wrappers::{reticulate_castops, reticulate_random};
use crate::proto::partitioning_pb::SerializedKMeansTree_Node;
use crate::zetareticulate::data_format::datapoint::DatapointIndex;
use crate::zetareticulate::distance_measures::one_to_one::l2_distance::L2Distance;


use crate::zetareticulate::oss_wrappers::{reticulate_castops, reticulate_random};
use crate::zetareticulate::proto::partitioning_pb;
use crate::zetareticulate::utils::fast_top_neighbors::FastTopNeighbors;


use crate::zetareticulate::utils::gmm_utils;
use crate::zetareticulate::utils::scalar_quantization_helpers::ScalarQuantizationResults;


use crate::zetareticulate::utils::types::DimensionIndex;
use crate::zetareticulate::utils::util_functions::{enumerate, make_dummy_shared, str_format};




use crate::zetareticulate::utils::{common, fast_top_neighbors, gmm_utils, scalar_quantization_helpers, types, util_functions};
use crate::tensorflow::core::platform::cpu_info;




use std::cmp::{max, min};
use std::collections::HashMap;
use std::f64::consts::NAN;
use std::f64::INFINITY;
use std::iter::FromIterator;


use std::sync::Arc;
use std::time::{Duration, Instant};



pub struct KMeansTreeNode {
    leaf_id_: i32,
    learned_spilling_threshold_: f64,
    indices_: Vec<DatapointIndex>,
    children_: Vec<KMeansTreeNode>,
    float_centers_: Vec<Datapoint<f32>>,
    center_squared_l2_norms_: Vec<f64>,
    inv_int8_multipliers_: Vec<f64>,
    fixed_point_centers_: Vec<Datapoint<i8>>,
    cur_node_center_: Option<Datapoint<f32>>,
}

use crate::distance_measures::{
    binary_distance_measure_base::BinaryDistanceMeasureBase,
    distance_measure_base::DistanceMeasure,
    dot_product::{DenseDotProduct, HybridDotProduct, SparseDotProduct},
};
use crate::utils::types::DatapointPtr;

pub struct CosineDistance;

impl DistanceMeasure for CosineDistance {
    fn normalization_required(&self) -> Normalization {
        Normalization::UnitL2Norm
    }
}

impl BinaryDistanceMeasureBase for CosineDistance {
    fn name(&self) -> &'static str {
        "Cosine Distance"
    }

    fn get_distance_dense(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        1.0 - DenseDotProduct(a, b)
    }

    fn get_distance_sparse(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        1.0 - SparseDotProduct(a, b)
    }

    fn get_distance_hybrid(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        1.0 - HybridDotProduct(a, b)
    }
}

// Include necessary modules and types
use crate::distance_measures::{
    binary_distance_measure_base::BinaryDistanceMeasureBase,
    distance_measure_base::{DistanceMeasure, Normalization},
};
use crate::utils::types::{DatapointPtr, string_view};

// Define the struct for BinaryCosineDistance
pub struct BinaryCosineDistance;

// Implement methods for BinaryCosineDistance
impl DistanceMeasure for BinaryCosineDistance {
    fn normalization_required(&self) -> Normalization {
        Normalization::None
    }
}

impl BinaryDistanceMeasureBase for BinaryCosineDistance {
    fn name(&self) -> &'static str {
        "Binary Cosine Distance"
    }

    fn get_distance_dense(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let mut num_intersect = 0;
        let mut a_num_ones = 0;
        let mut b_num_ones = 0;

        for i in 0..a.nonzero_entries() {
            a_num_ones += a.values()[i].count_ones() as DimensionIndex;
            b_num_ones += b.values()[i].count_ones() as DimensionIndex;
            num_intersect += (a.values()[i] & b.values()[i]).count_ones() as DimensionIndex;
        }

        1.0 - (num_intersect as f64 / ((a_num_ones * b_num_ones) as f64).sqrt())
    }

    fn get_distance_sparse(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let num_intersect = sparse_binary_dot_product(a, b) as DimensionIndex;
        let num_ones_a = a.nonzero_entries() as u64;
        let num_ones_b = b.nonzero_entries() as u64;

        1.0 - (num_intersect as f64 / ((num_ones_a * num_ones_b) as f64).sqrt())
    }

    fn get_distance_hybrid(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let mut num_intersect = 0;

        let (dense, sparse) = if a.is_dense() {
            (a, b)
        } else {
            (b, a)
        };

        for i in 0..sparse.nonzero_entries() {
            num_intersect += dense.get_element_packed(sparse.indices()[i]);
        }

        let num_ones_sparse = sparse.nonzero_entries() as u64;
        let num_ones_dense = dense.values().count_ones() as u64;

        1.0 - (num_intersect as f64 / ((num_ones_sparse * num_ones_dense) as f64).sqrt())
    }
}


// Include necessary modules and types
use crate::distance_measures::{
    binary_distance_measure_base::BinaryDistanceMeasureBase,
    distance_measure_base::{DistanceMeasure, Normalization},
};
use crate::utils::types::{DatapointPtr, string_view};

// Define the struct for BinaryCosineDistance
pub struct BinaryCosineDistance;

// Implement methods for BinaryCosineDistance
impl DistanceMeasure for BinaryCosineDistance {
    fn normalization_required(&self) -> Normalization {
        Normalization::None
    }
}


impl BinaryDistanceMeasureBase for BinaryCosineDistance {
    fn name(&self) -> &'static str {
        "Binary Cosine Distance"
    }

    fn get_distance_dense(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let mut num_intersect = 0;
        let mut a_num_ones = 0;
        let mut b_num_ones = 0;

        for i in 0..a.nonzero_entries() {
            a_num_ones += a.values()[i].count_ones() as DimensionIndex;
            b_num_ones += b.values()[i].count_ones() as DimensionIndex;
            num_intersect += (a.values()[i] & b.values()[i]).count_ones() as DimensionIndex;
        }

        1.0 - (num_intersect as f64 / ((a_num_ones * b_num_ones) as f64).sqrt())
    }

    fn get_distance_sparse(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let num_intersect = sparse_binary_dot_product(a, b) as DimensionIndex;
        let num_ones_a = a.nonzero_entries() as u64;
        let num_ones_b = b.nonzero_entries() as u64;

        1.0 - (num_intersect as f64 / ((num_ones_a * num_ones_b) as f64).sqrt())
    }

    fn get_distance_hybrid(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let mut num_intersect = 0;

        let (dense, sparse) = if a.is_dense() {
            (a, b)
        } else {
            (b, a)
        };

        for i in 0..sparse.nonzero_entries() {
            num_intersect += dense.get_element_packed(sparse.indices()[i]);
        }

        let num_ones_sparse = sparse.nonzero_entries() as u64;
        let num_ones_dense = dense.values().count_ones() as u64;

        1.0 - (num_intersect as f64 / ((num_ones_sparse * num_ones_dense) as f64).sqrt())
    }
}



// Include necessary modules and types
use crate::distance_measures::{
    binary_distance_measure_base::BinaryDistanceMeasureBase,
    distance_measure_base::{DistanceMeasure, Normalization},
};

use crate::utils::types::{DatapointPtr, string_view};


// Define the struct for BinaryCosineDistance
pub struct BinaryCosineDistance;

// Implement methods for BinaryCosineDistance
impl DistanceMeasure for BinaryCosineDistance {
    fn normalization_required(&self) -> Normalization {
        Normalization::None
    }
}

impl BinaryDistanceMeasureBase for BinaryCosineDistance {
    fn name(&self) -> &'static str {
        "Binary Cosine Distance"
    }

    fn get_distance_dense(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let mut num_intersect = 0;
        let mut a_num_ones = 0;
        let mut b_num_ones = 0;

        for i in 0..a.nonzero_entries() {
            a_num_ones += a.values()[i].count_ones() as DimensionIndex;
            b_num_ones += b.values()[i].count_ones() as DimensionIndex;
            num_intersect += (a.values()[i] & b.values()[i]).count_ones() as DimensionIndex;
        }

        1.0 - (num_intersect as f64 / ((a_num_ones * b_num_ones) as f64).sqrt())
    }

    fn get_distance_sparse(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let num_intersect = sparse_binary_dot_product(a, b) as DimensionIndex;
        let num_ones_a = a.nonzero_entries() as u64;
        let num_ones_b = b.nonzero_entries() as u64;

        1.0 - (num_intersect as f64 / ((num_ones_a * num_ones_b) as f64).sqrt())
    }

    fn get_distance_hybrid(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        let mut num_intersect = 0;

        let (dense, sparse) = if a.is_dense() {
            (a, b)
        } else {
            (b, a)
        };

        for i in 0..sparse.nonzero_entries() {
            num_intersect += dense.get_element_packed(sparse.indices()[i]);
        }

        let num_ones_sparse = sparse.nonzero_entries() as u64;
        let num_ones_dense = dense.values().count_ones() as u64;

        1.0 - (num_intersect as f64 / ((num_ones_sparse * num_ones_dense) as f64).sqrt())
    }
}


// Include necessary modules and types
use crate::distance_measures::{
    binary_distance_measure_base::BinaryDistanceMeasureBase,
    distance_measure_base::{DistanceMeasure, Normalization},
};



