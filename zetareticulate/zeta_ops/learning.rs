
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

use std::fmt;
use std::io::Write;
use std::process::{Command, Stdio};


// Define a struct to hold the status code and message


use crate::proto::partitioning::PartitioningConfig;
use crate::utils::gmm_utils::Options as GmmOptions;


#[derive(Debug)]
struct Status {
    code: i32, // assuming i32 for error code for simplicity
    message: String,
}


impl Status {
    fn new(code: i32, message: &str) -> Self {
        Status {
            code,
            message: message.to_string(),
        }
    }
}








// Define a builder struct to build the status object with additional information

#[derive(Debug)]
struct StatusBuilder {
    status: Status,
    streamptr: Option<std::string::String>,
}


impl StatusBuilder {
    fn new(status: Status) -> Self {
        StatusBuilder {
            status,
            streamptr: None,
        }
    }

    fn log_error(&self) -> &Self {
        self
    }

    fn create_status(mut self) -> Status {
        if let Some(streamptr) = self.streamptr.take() {
            self.status.message.push_str(&streamptr);
        }
        self.status
    }
}

// Implement Display trait for StatusBuilder
impl fmt::Display for StatusBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.status)
    }
}

fn aborted_error_builder() -> StatusBuilder {
    StatusBuilder::new(Status::new(1, "")) // assuming error code 1 for aborted error
}

// Define a struct to hold the status code and message

#[derive(Debug)]
struct Status {
    code: i32, // assuming i32 for error code for simplicity
    message: String,
}


impl Status {
    fn new(code: i32, message: &str) -> Self {
        Status {
            code,
            message: message.to_string(),
        }
    }
}


// Define a builder struct to build the status object with additional information
// do not modify this struct
#[derive(Debug)]
struct StatusBuilder {
    status: Status,
    streamptr: Option<std::string::String>,
}


impl StatusBuilder {
    fn new(status: Status) -> Self {
        StatusBuilder {
            status,
            streamptr: None,
        }
    }

    fn log_error(&self) -> &Self {
        self
    }

    fn create_status(mut self) -> Status {
        if let Some(streamptr) = self.streamptr.take() {
            self.status.message.push_str(&streamptr);
        }
        self.status
    }
}







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





// Define a struct to hold the status code and message

#[derive(Debug)]
struct Status {
    code: i32, // assuming i32 for error code for simplicity
    message: String,
}


impl Status {
    fn new(code: i32, message: &str) -> Self {
        Status {
            code,
            message: message.to_string(),
        }
    }
}



// Define a builder struct to build the status object with additional information
// do not modify this struct




#[derive(Debug)]
struct StatusBuilder {
    status: Status,
    streamptr: Option<std::string::String>,
}