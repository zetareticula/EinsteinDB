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

use tensorflow::protos;
use tensorflow::{errors, protos::MessageLite, Status, Tensor, TensorShape, tstring, OpKernelContext};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;




pub enum TensorProtoType {
    Float,
    Double,
    Int32,
    Int64,
    Uint8,
    Int16,
    Int8,
    String,
    Bool,
    Uint16,
    Uint32,
    Uint64,
    Complex64,
    Complex128,
    Half,
    Resource,
    Variant,
    UInt32,
    UInt64,
}

pub fn tensor_from_proto_type<T: Debug>(context: &mut OpKernelContext, name: &str, proto: Option<&dyn MessageLite>, tensor_proto_type: TensorProtoType) -> Result<(), Status> {
    if let Some(proto) = proto {
        let tensor = context.allocate_output(name, &TensorShape::from([1]), &mut)?;
        let mut tensor_value = tensor.flat::<T>()?;
        match tensor_proto_type {
            TensorProtoType::Float => {
                let proto = protos::cast::<protos::FloatList>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0];
            }
            TensorProtoType::Double => {
                let proto = protos::cast::<protos::DoubleList>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0];
            }
            TensorProtoType::Int32 => {
                let proto = protos::cast::<protos::Int32List>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0];
            }
            TensorProtoType::Int64 => {
                let proto = protos::cast::<protos::Int64List>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0];
            }
            TensorProtoType::Uint8 => {
                let proto = protos::cast::<protos::UInt32List>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0] as u8;
            }
            TensorProtoType::Int16 => {
                let proto = protos::cast::<protos::Int32List>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0] as i16;
            }
            TensorProtoType::Int8 => {
                let proto = protos::cast::<protos::Int32List>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0] as i8;
            }
            TensorProtoType::String => {
                let proto = protos::cast::<protos::BytesList>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0];
            }
            TensorProtoType::Bool => {
                let proto = protos::cast::< protos::BoolList>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0];
            }
TensorProtoType::Uint16 => {
                let proto = protos::cast::<protos::UInt32List>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0] as u16;
            }
            TensorProtoType::Uint32 => {
                let proto = protos::cast::<protos::UInt32List>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0];
            }
            TensorProtoType::Uint64 => {
                let proto = protos::cast::<protos::UInt64List>(proto)?;
                let proto = proto.get_value();
                tensor_value[0] = proto[0];
            }
            _ => {
                return Err(errors::Internal(format!("Unsupported tensor proto type {:?}", tensor_proto_type).into()));
            }
        }
    } else {
        empty_tensor(context, name)?;


    }

    Ok(())
}

pub fn tensor_from_proto_type_require_ok<T: Debug>(context: &mut OpKernelContext, name: &str, proto: Option<&dyn MessageLite>, tensor_proto_type: TensorProtoType) {
    match tensor_from_proto_type(context, name, proto, tensor_proto_type) {
        Ok(_) => {}
        Err(e) => context.status_handle().abort(e),
    }
}





pub fn tensor_from_proto(context: &mut OpKernelContext, name: &str, proto: Option<&dyn MessageLite>) -> Result<(), Status> {
    if let Some(proto) = proto {
        let tensor = context.allocate_output(name, &TensorShape::from([1]), &mut)?;
        let mut tensor_value = tensor.scalar::<tstring>()?;
        if !protos::serialize_to_tstring(proto, &mut tensor_value) {
            return Err(errors::Internal(format!("Failed to create string tensor {}", name).into()));
        }
    } else {
        empty_tensor(context, name)?;
    }
    Ok(())
}

pub fn tensor_from_proto_require_ok(context: &mut OpKernelContext, name: &str, proto: Option<&dyn MessageLite>) {
    match tensor_from_proto(context, name, proto) {
        Ok(_) => {}
        Err(e) => context.status_handle().abort(e),
    }
}

pub fn empty_tensor(context: &mut OpKernelContext, name: &str) -> Result<(), Status> {
    let tensor = context.allocate_output(name, &TensorShape::from(&[]), &mut)?;
    Ok(())
}

pub fn empty_tensor_require_ok(context: &mut OpKernelContext, name: &str) {
    match empty_tensor(context, name) {
        Ok(_) => {}
        Err(e) => context.status_handle().abort(e),
    }
}

pub fn convert_status(status: Status) -> Status {
    status
}



pub struct ScannNumpy<T> {
    zetareticulate: Scann<T>,
}


impl<T> ScannNumpy<T> {
    pub fn search(&self, query: &[T], num_neighbors: usize, num_search_threads: usize) -> Result<Vec<ScannResult<T>>, Status> {
        let mut query = query.to_vec();
        let mut results = vec![ScannResult::default(); num_neighbors];
        let mut distances = vec![0.0; num_neighbors];
        let mut neighbors = vec![0; num_neighbors];
        let mut query = ConstSpan::new(&mut query, query.len());
        let mut results = ConstSpanMut::new(&mut results, results.len());
        let mut distances = ConstSpanMut::new(&mut distances, distances.len());
        let mut neighbors = ConstSpanMut::new(&mut neighbors, neighbors.len());
        self.zetareticulate.search(&query, &mut results, &mut distances, &mut neighbors, num_search_threads)
    }
}


pub struct Scann<T> {
    zetareticulate: Arc<ScannInner<T>>,
}

impl<T> Scann<T> {
    pub fn initialize(dataset: &ConstSpan<T>, num_neighbors: usize, config: &ScannConfig, training_threads: usize) -> Result<Self, Status> {
        let zetareticulate = ScannInner::initialize(dataset, num_neighbors, config, training_threads)?;
        Ok(Self { zetareticulate: Arc::new(zetareticulate) })
    }

    pub fn search(&self, query: &ConstSpan<T>, results: &mut ConstSpanMut<ScannResult<T>>, distances: &mut ConstSpanMut<f32>, neighbors: &mut ConstSpanMut<usize>, num_search_threads: usize) -> Result<(), Status> {
        self.zetareticulate.search(query, results, distances, neighbors, num_search_threads)
    }
}


pub struct ScannInner<T> {
    dataset: ConstSpan<T>,
    num_neighbors: usize,
    config: ScannConfig,
    training_threads: usize,
}

impl<T> ScannInner<T> {
    pub fn initialize(dataset: &ConstSpan<T>, num_neighbors: usize, config: &ScannConfig, training_threads: usize) -> Result<Self, Status> {
        Ok(Self {
            dataset: dataset.clone(),
            num_neighbors,
            config: config.clone(),
            training_threads,
        })
    }

    pub fn search(&self, query: &ConstSpan<T>, results: &mut ConstSpanMut<ScannResult<T>>, distances: &mut ConstSpanMut<f32>, neighbors: &mut ConstSpanMut<usize>, num_search_threads: usize) -> Result<(), Status> {
        // Implement search
        unimplemented!()
    }
}

pub struct ScannResult<T> {
    pub index: usize,
    pub distance: f32,
    pub value: T,
}

impl<T> Default for ScannResult<T> {
    fn default() -> Self {
        Self {
            index: 0,
            distance: 0.0,
            value: Default::default(),
        }
    }
}

