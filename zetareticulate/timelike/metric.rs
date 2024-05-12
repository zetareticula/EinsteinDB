
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
use std::ops::Neg;
use std::cmp::min;



pub struct DotProductDistance;

impl DistanceMeasure for DotProductDistance {
    const EARLY_STOPPING: EarlyStoppingSupport = EarlyStoppingSupport::NotSupported;

    fn get_distance_dense(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        -(dense_binary_dot_product(a, b) as f64)
    }

    fn get_distance_sparse(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        -(sparse_binary_dot_product(a, b) as f64)
    }

    fn get_distance_hybrid(&self, _: &DatapointPtr<u8>, _: &DatapointPtr<u8>) -> f64 {
        unimplemented!("Hybrid dot product distance not implemented")
    }
}

impl From<DotProductDistance> for DistanceMeasure {
    fn from(_: DotProductDistance) -> Self {
        DistanceMeasure::DotProduct
    }
}

pub struct AbsDotProductDistance;

impl DistanceMeasure for AbsDotProductDistance {
    const EARLY_STOPPING: EarlyStoppingSupport = EarlyStoppingSupport::NotSupported;

    fn get_distance_dense(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        -(dense_binary_dot_product(a, b) as f64).abs()
    }

    fn get_distance_sparse(&self, a: &DatapointPtr<u8>, b: &DatapointPtr<u8>) -> f64 {
        -(sparse_binary_dot_product(a, b) as f64).abs()
    }

    fn get_distance_hybrid(&self, _: &DatapointPtr<u8>, _: &DatapointPtr<u8>) -> f64 {
    unimplemented!("Hybrid dot product distance not implemented")
}

impl From<AbsDotProductDistance> for DistanceMeasure {
    fn from(_: AbsDotProductDistance) -> Self {
        DistanceMeasure::AbsDotProduct
    }
}
