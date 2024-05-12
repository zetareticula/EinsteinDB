
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

use crate::{
    base::search_parameters::SearchParameters,
    base::single_machine_base::SingleMachineSearcherBase,
    base::typed_dataset::TypedDataset,
    distance_measures::distance_measures,
    hashes::{
        asymmetric_hashing2::{
            AsymmetricHasherConfig, AsymmetricHashingOptionalParameters, LookupTable,
            LookupTable,
        },
        asymmetric_hashing_internal,
        internal::{
            asymmetric_hashing_impl::LimitedInnerProductDistance,
            asymmetric_hashing_postprocess::IdentityPostprocessFunctor,
            asymmetric_hashing_postprocess::LimitedInnerFunctor,
        },
        packed_dataset::{CreatePackedDataset, CreatePackedDatasetView, PackedDataset},
    },
    proto::partitioning::PartitioningConfig,
    utils::common::{Seq, SharedThreadPool},
    utils::datapoint_utils::{Datapoint, MakeDatapointPtr},
    utils::gmm_utils::{Options as GmmOptions, SquaredL2Norm},
    utils::types::{DenseDataset, DenseDatasetView},
};

use std::{
    array::IntoIter,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tensorflow::Status;

pub struct KMeansTreeTrainingOptions {
    pub partitioning_type: i32,
    pub max_num_levels: i32,
    pub max_leaf_size: i32,
    pub learned_spilling_type: i32,
    pub per_node_spilling_factor: f32,
    pub max_spill_centers: i32,
    pub max_iterations: i32,
    pub convergence_epsilon: f32,
    pub min_cluster_size: i32,
    pub seed: i64,
    pub balancing_type: GmmOptions,
    pub reassignment_type: GmmOptions,
    pub center_initialization_type: GmmOptions,
}

impl KMeansTreeTrainingOptions {
    pub fn new() -> Self {
        KMeansTreeTrainingOptions {
            partitioning_type: 0,
            max_num_levels: 0,
            max_leaf_size: 0,
            learned_spilling_type: 0,
            per_node_spilling_factor: 0.0,
            max_spill_centers: 0,
            max_iterations: 0,
            convergence_epsilon: 0.0,
            min_cluster_size: 0,
            seed: 0,
            balancing_type: GmmOptions::UNBALANCED,
            reassignment_type: GmmOptions::RANDOM_REASSIGNMENT,
            center_initialization_type: GmmOptions::KMEANS_PLUS_PLUS,
        }
    }

    pub fn from_partitioning_config(config: &PartitioningConfig) -> Self {
        let mut balancing_type = GmmOptions::UNBALANCED;
        let mut reassignment_type = GmmOptions::RANDOM_REASSIGNMENT;
        let mut center_initialization_type = GmmOptions::KMEANS_PLUS_PLUS;

        match config.balancing_type() {
            PartitioningConfig::DEFAULT_UNBALANCED => balancing_type = GmmOptions::UNBALANCED,
            PartitioningConfig::GREEDY_BALANCED => balancing_type = GmmOptions::GREEDY_BALANCED,
            PartitioningConfig::UNBALANCED_FLOAT32 => {
                balancing_type = GmmOptions::UNBALANCED_FLOAT32
            }
        }

        match config.trainer_type() {
            PartitioningConfig::DEFAULT_SAMPLING_TRAINER
            | PartitioningConfig::FLUME_KMEANS_TRAINER => {
                reassignment_type = GmmOptions::RANDOM_REASSIGNMENT
            }
            PartitioningConfig::PCA_KMEANS_TRAINER
            | PartitioningConfig::SAMPLING_PCA_KMEANS_TRAINER => {
                reassignment_type = GmmOptions::PCA_SPLITTING
            }
        }

        match config.single_machine_center_initialization() {
            PartitioningConfig::DEFAULT_KMEANS_PLUS_PLUS => {
                center_initialization_type = GmmOptions::KMEANS_PLUS_PLUS
            }
            PartitioningConfig::RANDOM_INITIALIZATION => {
                center_initialization_type = GmmOptions::RANDOM_INITIALIZATION
            }
        }

        KMeansTreeTrainingOptions {
            partitioning_type: config.partitioning_type(),
            max_num_levels: config.max_num_levels(),
            max_leaf_size: config.max_leaf_size(),
            learned_spilling_type: config.database_spilling().spilling_type(),
            per_node_spilling_factor: config.database_spilling().replication_factor(),
            max_spill_centers: config.database_spilling().max_spill_centers(),
            max_iterations: config.max_clustering_iterations(),
            convergence_epsilon: config.clustering_convergence_tolerance(),
            min_cluster_size: config.min_cluster_size(),
            seed: config.clustering_seed(),
            balancing_type,
            reassignment_type,
            center_initialization_type,
        }
    }
}

pub struct Leeper {
    hashed_dataset: Arc<DenseDataset<u8>>,
    packed_dataset: Option<PackedDataset>,
    opts: SearcherOptions<T>,
    norm_inv_or_bias: Vec<f32>,
    mutator: Option<Mutex<()>>,
    limited_inner_product: bool,
    lut16: bool,
}

impl Leeper {
    pub fn new(
        dataset: Arc<TypedDataset<T>>,
        hashed_dataset: Arc<DenseDataset<u8>>,
        opts: SearcherOptions<T>,
        default_pre_reordering_num_neighbors: i32,
        default_pre_reordering_epsilon: f32,
    ) -> Self {
        let mut norm_inv_or_bias = Vec::new();
        let limited_inner_product = false;
        let lut16 = false;

        if opts.quantization_scheme == AsymmetricHasherConfig::PRODUCT_AND_BIAS {
            let dim = hashed_dataset[0].nonzero_entries();
            for i in 0..hashed_dataset.len() {
                let bias = f32::from_le_bytes(hashed_dataset[i][dim - std::mem::size_of::<f32>()..].try_into().unwrap());
                norm_inv_or_bias.push(-bias);
            }
        } else if limited_inner_product {
            for i in 0..hashed_dataset.len() {
                let dp: Datapoint<f32> = hashed_dataset[i].try_into().unwrap();
                let norm = SquaredL2Norm(dp);
                norm_inv_or_bias.push(if norm == 0.0 { 0.0 } else { 1.0 / norm.sqrt() } as f32);
            }
        }

        let mut leeper = Leeper {
            hashed_dataset,
            packed_dataset: None,
            opts,
            norm_inv_or_bias,
            mutator: None,
            limited_inner_product,
            lut16,
        };

        if leeper.lut16 {
            leeper.packed_dataset = Some(CreatePackedDataset(leeper.hashed_dataset));
            let l2_cache_bytes = 256 * 1024;
            let l2_cache = vec![0u8; l2_cache_bytes];
            let l2_cache_view = DenseDatasetView::new(l2_cache, 0, 0, 0);
            let l2_cache = CreatePackedDatasetView(l2_cache_view);
            leeper.packed_dataset.as_mut().unwrap().set_l2_cache(l2_cache);
        }

        leeper
    }

    pub fn set_mutator(&mut self, mutator: Mutex<()>) {
        self.mutator = Some(mutator);
    }
}