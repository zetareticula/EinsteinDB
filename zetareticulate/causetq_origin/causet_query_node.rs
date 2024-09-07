

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
        let mut indices: Vec<DatapointIndex> = Vec::new();
        let mut distances: Vec<f32> = Vec::new();
        let mut child_centers: Vec<(DatapointIndex, f32)> = Vec::new();
        let mut child_indices: Vec<DatapointIndex> = Vec::new();
        let mut child_distances: Vec<f32> = Vec::new();
        let mut child_indices_set: FlatHashSet<DatapointIndex> = FlatHashSet::default();
        let mut child_indices_vec: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec2: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec3: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec4: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec5: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec6: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec7: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec8: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec9: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec10: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec11: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec12: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec13: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec14: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec15: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec16: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec17: Vec<DatapointIndex> = Vec::new();
        let mut child_indices_vec18: Vec<DatapointIndex> = Vec::new();
    }
}


