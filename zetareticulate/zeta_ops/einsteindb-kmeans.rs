
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

use crate::absl::flat_hash_set::FlatHashSet;
use crate::absl::Span;
use crate::dataset::{Datapoint, Dataset};
use crate::distance_measures::DistanceMeasure;
use crate::distance_measures::l2_distance::L2Distance;
use crate::gmm_utils::GmmUtils;
use crate::oss_wrappers::{scann_castops, scann_random};
use crate::proto::partitioning_pb::SerializedKMeansTree_Node;
use crate::zeta::data_format::datapoint::DatapointIndex;
use crate::zeta::distance_measures::one_to_one::l2_distance::L2Distance;
use crate::zeta::oss_wrappers::{scann_castops, scann_random};
use crate::zeta::proto::partitioning_pb;
use crate::zeta::utils::fast_top_neighbors::FastTopNeighbors;
use crate::zeta::utils::gmm_utils;
use crate::zeta::utils::scalar_quantization_helpers::ScalarQuantizationResults;
use crate::zeta::utils::types::DimensionIndex;
use crate::zeta::utils::util_functions::{enumerate, make_dummy_shared, str_format};
use crate::zeta::utils::{common, fast_top_neighbors, gmm_utils, scalar_quantization_helpers, types, util_functions};
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

impl KMeansTreeNode {
    pub fn new() -> Self {
        Self {
            leaf_id_: -1,
            learned_spilling_threshold_: NAN,
            indices_: Vec::new(),
            children_: Vec::new(),
            float_centers_: Vec::new(),
            center_squared_l2_norms_: Vec::new(),
            inv_int8_multipliers_: Vec::new(),
            fixed_point_centers_: Vec::new(),
            cur_node_center_: None,
        }
    }

    pub fn reset(&mut self) {
        self.leaf_id_ = -1;
        self.learned_spilling_threshold_ = NAN;
        self.indices_.clear();
        self.children_.clear();
    }

    pub fn union_indices(&self) -> Vec<DatapointIndex> {
        let mut union_hash: FlatHashSet<DatapointIndex> = FlatHashSet::default();
        self.union_indices_impl(&mut union_hash);
        union_hash.into_iter().collect()
    }

    fn to_datapoint<T: Clone>(&self, values: &[T]) -> Datapoint<f32> {
        let mut dp = Datapoint::new();
        dp.values.reserve(values.len());
        dp.values.extend_from_slice(values);
        dp
    }

    pub fn build_from_proto(&mut self, proto: &SerializedKMeansTree_Node) {
        self.float_centers_.clear();
        let mut dp = Datapoint::new();
        for center in proto.centers.iter() {
            let values = if !center.float_dimension.is_empty() {
                &center.float_dimension
            } else {
                &center.dimension
            };
            dp = self.to_datapoint(values);
            if self.float_centers_.is_empty() {
                self.float_centers_.resize(proto.centers.len(), Datapoint::new());
            }
            self.float_centers_.push(dp);
        }

        self.learned_spilling_threshold_ = proto.learned_spilling_threshold;
        self.leaf_id_ = proto.leaf_id;

        self.indices_.clear();
        self.children_.clear();
        if proto.children.is_empty() {
            self.indices_ = proto.indices.clone();
        } else {
            for child_proto in proto.children.iter() {
                let mut child = KMeansTreeNode::new();
                child.build_from_proto(child_proto);
                self.children_.push(child);
            }
        }
    }

    fn postprocess_distances_for_spilling(
        &self,
        distances: &[f32],
        spilling_type: partitioning_pb::QuerySpillingConfig_SpillingType,
        spilling_threshold: f64,
        max_centers: i32,
    ) -> Vec<(DatapointIndex, f32)> {
        let mut child_centers: Vec<(DatapointIndex, f32)> = Vec::new();
        let epsilon = f32::INFINITY;
        if spilling_type != partitioning_pb::QuerySpillingConfig_SpillingType::NO_SPILLING
            && spilling_type != partitioning_pb::QuerySpillingConfig_SpillingType::FIXED_NUMBER_OF_CENTERS
        {
            let nearest_center_index = distances
                .iter()
                .enumerate()
                .min_by(|(_, &a), (_, &b)| a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(i, _)| i)
                .unwrap_or(0);
            let nearest_center_distance = distances[nearest_center_index];

            let spill_thresh = (spilling_threshold.next_after(f32::INFINITY)).unwrap_or(f32::INFINITY);
            let max_dist_to_consider = compute_threshold(nearest_center_distance, spill_thresh, spilling_type).unwrap_or(f32::INFINITY);
            let epsilon = (max_dist_to_consider.next_after(f32::INFINITY)).unwrap_or(f32::INFINITY);
        }
        let max_results = if spilling_type == partitioning_pb::QuerySpillingConfig_SpillingType::NO_SPILLING {
            1
        } else {
            max_centers
        };
        let top_n = FastTopNeighbors::new(max_results, epsilon);
        top_n.push_block(distances, 0);
        top_n.finish_unsorted(&mut child_centers);
        child_centers
    }

    pub fn train(
        &mut self,
        training_data: &Dataset,
        subset: Vec<DatapointIndex>,
        training_distance: &DistanceMeasure,
        k_per_level: i32,
        current_level: i32,
        opts: &KMeansTreeTrainingOptions,
    ) {
        let mut centers: Vec<Datapoint<f32>> = Vec::new();
        let mut center_indices: Vec<DatapointIndex> = Vec::new();
        let mut center_squared_l2_norms: Vec<f64> = Vec::new();
        let mut inv_int8_multipliers: Vec<f64> = Vec::new();
        let mut fixed_point_centers: Vec<Datapoint<i8>> = Vec::new();
        let mut cur_node_center: Option<Datapoint<f32>> = None;

        let mut indices = subset.clone();
        let mut distances = vec![0.0; indices.len()];
        let mut centers = vec![Datapoint::new(); k_per_level as usize];
        let mut center_indices = vec![0; k_per_level as usize];
        let mut center_squared_l2_norms = vec![0.0; k_per_level as usize];
        let mut inv_int8_multipliers = vec![0.0; k_per_level as usize];
        let mut fixed_point_centers = vec![Datapoint::new(); k_per_level as usize];
        let mut cur_node_center = None;

        let mut rng = scann_random::new_rng();
        let mut num_centers = 0;
        let mut num_centers_tried = 0;


        let mut num_centers_tried = 0;

        while num_centers < k_per_level {
            let mut best_center_index = -1;
            let mut best_center_distance = f64::INFINITY;
            let mut best_center = Datapoint::new();
            let mut best_center_index = -1;
            let mut best_center_distance = f64::INFINITY;
            let mut best_center = Datapoint::new();

            for i in 0..indices.len() {
                let mut min_distance = f64::INFINITY;
                for j in 0..num_centers {
                    let distance = training_distance.distance(&training_data[indices[i]], &centers[j]);
                    if distance < min_distance {
                        min_distance = distance;
                    }
                }
                if min_distance < best_center_distance {
                    best_center_distance = min_distance;
                    best_center_index = indices[i];
                    best_center = training_data[indices[i]].clone();
                }
            }

            if best_center_index == -1 {
                break;
            }

            let mut min_distance = f64::INFINITY;
            for i in 0..indices.len() {
                let distance = training_distance.distance(&training_data[indices[i]], &best_center);
                if distance < min_distance {
                    min_distance = distance;
                }
            }

            if min_distance < opts.spilling_threshold {
                break;
            }

            centers[num_centers] = best_center.clone();
            center_indices[num_centers] = best_center_index;
            center_squared_l2_norms[num_centers] = training_distance.squared_l2_norm(&best_center);
            inv_int8_multipliers[num_centers] = 1.0;
            fixed_point_centers[num_centers] = gmm_utils::float_to_fixed_point(&best_center, opts.fixed_point_multiplier);
            cur_node_center = Some(best_center.clone());

            num_centers += 1;
            num_centers_tried += 1;
        }

        if num_centers == 0 {
            return;
        }

        let mut distances = vec![0.0; indices.len()];

        for i in 0..indices.len() {
            let mut min_distance = f64::INFINITY;
            for j in 0..num_centers {
                let distance = training_distance.distance(&training_data[indices[i]], &centers[j]);
                if distance < min_distance {
                    min_distance = distance;
                }
            }
            distances[i] = min_distance;
        }

        let mut child_centers = self.postprocess_distances_for_spilling(&distances, opts.spilling_type, opts.spilling_threshold, k_per_level);

        if child_centers.is_empty() {
            return;
        }

        let mut new_indices = Vec::new();
        for (index, _) in child_centers.iter() {
            new_indices.push(indices[*index]);
        }

        self.indices_ = new_indices;
        self.float_centers_ = centers;
        self.center_squared_l2_norms_ = center_squared_l2_norms;
        self.inv_int8_multipliers_ = inv_int8_multipliers;
        self.fixed_point_centers_ = fixed_point_centers;
        self.cur_node_center_ = cur_node_center;

        if self.children_.is_empty() {
            self.indices_ = new_indices;
        } else {
            for (i, child) in self.children_.iter_mut().enumerate() {
                let mut child_indices = Vec::new();
                for (index, _) in child_centers.iter() {
                    child_indices.push(indices[*index]);
                }
                child.train(training_data, child_indices, training_distance, k_per_level, current_level + 1, opts);
            }
        }

        if self.children_.is_empty() {
            self.indices_ = new_indices;
        } else {
            for (i, child) in self.children_.iter_mut().enumerate() {
                let mut child_indices = Vec::new();
                for (index, _) in child_centers.iter() {
                    child_indices.push(indices[*index]);
                }
                child.train(training_data, child_indices, training_distance, k_per_level, current_level + 1, opts);
            }
        }

        if self.children_.is_empty() {
            self.indices_ = new_indices;
        } else {
            for (i, child) in self.children_.iter_mut().enumerate() {
                let mut child_indices = Vec::new();
                for (index, _) in child_centers.iter() {
                    child_indices.push(indices[*index]);
                }
                child.train(training_data, child_indices, training_distance, k_per_level, current_level + 1, opts);
            }
        }





        pub fn train( &mut self, training_data: &Dataset, subset: Vec<DatapointIndex>, training_distance: &DistanceMeasure, k_per_level: i32, current_level: i32, opts: &KMeansTreeTrainingOptions) {
        let mut centers: Vec<Datapoint<f32>> = Vec::new();
        let mut center_indices: Vec<DatapointIndex> = Vec::new();
        let mut center_squared_l2_norms: Vec<f64> = Vec::new();
        let mut inv_int8_multipliers: Vec<f64> = Vec::new();
        let mut fixed_point_centers: Vec<Datapoint<i8>> = Vec::new();
        let mut cur_node_center: Option<Datapoint<f32>> = None;

        }

        for (index, _) in child_centers.iter() {
            new_indices.push(indices[*index]);
        }


        if self.children_.is_empty() {
            self.indices_ = new_indices;
        } else {
            for (i, child) in self.children_.iter_mut().enumerate() {
                let mut child_indices = Vec::new();
                for (index, _) in child_centers.iter() {
                    child_indices.push(indices[*index]);
                }
                child.train(training_data, child_indices, training_distance, k_per_level, current_level + 1, opts);
            }
        }

        if self.children_.is_empty() {
            self.indices_ = new_indices;
        } else {
            for (i, child) in self.children_.iter_mut().enumerate() {
                let mut child_indices = Vec::new();
                for (index, _) in child_centers.iter() {
                    child_indices.push(indices[*index]);
                }
                child.train(training_data, child_indices, training_distance, k_per_level, current_level + 1, opts);
            }
        }

        if self.children_.is_empty() {
            self.indices_ = new_indices;
        } else {
            for (i, child) in self.children_.iter_mut().enumerate() {
                let mut child_indices = Vec::new();
                for (index, _) in child_centers.iter() {
                    child_indices.push(indices[*index]);
                }
                child.train(training_data, child_indices, training_distance, k_per_level, current_level + 1, opts);
            }
        }
    }

    fn union_indices_impl(&self, union_hash: &mut FlatHashSet<DatapointIndex>) {
        for index in self.indices_.iter() {
            union_hash.insert(*index);
        }
        for child in self.children_.iter() {
            child.union_indices_impl(union_hash);
        }
    }

    fn compute_threshold(
        nearest_center_distance: f32,
        spilling_threshold: f64,
        spilling_type: partitioning_pb::QuerySpillingConfig_SpillingType,
    ) -> Option<f32> {
        match spilling_type {
            partitioning_pb::QuerySpillingConfig_SpillingType::FIXED_NUMBER_OF_CENTERS => Some(spilling_threshold as f32),
            partitioning_pb::QuerySpillingConfig_SpillingType::FIXED_DISTANCE_FROM_NEAREST_CENTER => Some(nearest_center_distance + spilling_threshold as f32),
            _ => None,
        }
    }

    pub fn get_leaf_id(&self) -> i32 {
        self.leaf_id_
    }

    pub fn get_learned_spilling_threshold(&self) -> f64 {
        self.learned_spilling_threshold as f64
    }

    pub fn get_indices(&self) -> Vec<DatapointIndex> {
        self.indices_.clone()
    }

    pub fn get_children(&self) -> Vec<KMeansTreeNode> {
        self.children_.clone()
    }

    pub fn get_float_centers(&self) -> Vec<Datapoint<f32>> {
        self.float_centers_.clone()
    }
}
