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



use crate::asymmetric_hashing2::AsymmetricHasherConfigQuantizationScheme;
use crate::utils::{Status, StatusOr};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Model<T> {
    centers: Vec<DenseDataset<f64>>,
    num_clusters_per_block: usize,
    quantization_scheme: AsymmetricHasherConfigQuantizationScheme,
}

impl<T> Model<T> {
    pub fn from_centers(
        centers: Vec<DenseDataset<f64>>,
        quantization_scheme: AsymmetricHasherConfigQuantizationScheme,
    ) -> Result<Self, Status> {
        if centers.is_empty() {
            return Err(Status::invalid_argument(
                "Cannot construct a Model from empty centers.",
            ));
        } else if centers[0].is_empty() || centers[0].size() > 256 {
            return Err(Status::invalid_argument(format!(
                "Each asymmetric hashing block must contain between 1 and 256 centers, not {}.",
                centers[0].size()
            )));
        }

        for i in 1..centers.len() {
            if centers[i].size() != centers[0].size() {
                return Err(Status::invalid_argument(format!(
                    "All asymmetric hashing blocks must have the same number of centers. ({:?} vs. {:?}).",
                    centers[0].size(),
                    centers[i].size()
                )));
            }
        }

        Ok(Model {
            centers,
            num_clusters_per_block: centers[0].size(),
            quantization_scheme,
        })
    }

    pub fn from_proto(proto: &CentersForAllSubspaces) -> Result<Self, Status> {
        let num_blocks = proto.subspace_centers.len();
        if num_blocks == 0 {
            return Err(Status::invalid_argument(
                "Cannot build a Model from a serialized CentersForAllSubspaces with zero blocks.",
            ));
        }

        let mut all_centers = Vec::with_capacity(num_blocks);
        for i in 0..num_blocks {
            let num_centers = proto.subspace_centers[i].center.len();
            let mut centers = DenseDataset::new();
            for j in 0..num_centers {
                let dp = Datapoint::<f64>::from_gfv(&proto.subspace_centers[i].center[j]);
                centers.append(&dp)?;
            }
            centers.shrink_to_fit();
            all_centers.push(centers);
        }

        Model::from_centers(all_centers, proto.quantization_scheme())
    }

    pub fn to_proto(&self) -> CentersForAllSubspaces {
        let mut result = CentersForAllSubspaces::new();
        for centers in &self.centers {
            let mut centers_serialized = SubspaceCenters::new();
            for j in 0..centers.size() {
                let dp = centers.get_datapoint(j);
                centers_serialized.center.push(dp.to_gfv());
            }
            result.subspace_centers.push(centers_serialized);
        }
        result.set_quantization_scheme(self.quantization_scheme.clone());
        result
    }

    pub fn centers_equal(&self, rhs: &Self) -> bool {
        if self.centers.len() != rhs.centers.len() {
            return false;
        }
        for (i, _) in self.centers.iter().enumerate() {
            if self.centers[i].dimensionality() != rhs.centers[i].dimensionality()
                || self.centers[i].size() != rhs.centers[i].size()
            {
                return false;
            }
            let this_span = self.centers[i].data();
            let rhs_span = rhs.centers[i].data();
            if this_span != rhs_span {
                return false;
            }
        }
        true
    }
}

pub struct DenseDataset<T> {
    data: Vec<T>,
    num_datapoints: usize,
}

impl<T> DenseDataset<T> {
    pub fn new() -> Self {
        DenseDataset {
            data: Vec::new(),
            num_datapoints: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    pub fn get_datapoint(&self, index: usize) -> &T {
        &self.data[index]
    }

    pub fn append(&mut self, dp: &Datapoint<f64>) -> Result<(), Status> {
        self.data.extend_from_slice(&dp.features);
        self.num_datapoints += 1;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Datapoint<T> {
    features: Vec<T>,
}

impl<T> Datapoint<T> {
    pub fn from_gfv(_gfv: &GFV) -> Self {
        unimplemented!()
    }

    pub fn to_gfv(&self) -> GFV {
        unimplemented!()
    }
}

pub struct GFV {
    _placeholder: usize,
}

#[derive(Clone)]
pub struct CentersForAllSubspaces {
    subspace_centers: Vec<SubspaceCenters>,
    quantization_scheme: AsymmetricHasherConfigQuantizationScheme,
}

impl CentersForAllSubspaces {
    pub fn new() -> Self {
        CentersForAllSubspaces {
            subspace_centers: Vec::new(),
            quantization_scheme: AsymmetricHasherConfigQuantizationScheme::default(),
        }
    }

    pub fn set_quantization_scheme(&mut self, quantization_scheme: AsymmetricHasherConfigQuantizationScheme) {
        self.quantization_scheme = quantization_scheme;
    }
}

#[derive(Clone)]
pub struct SubspaceCenters {
    center: Vec<GFV>,
}
