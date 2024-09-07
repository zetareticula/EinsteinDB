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

use crate::zetareticulate::reticulate_ops::zetareticulate;
use crate::zetareticulate::reticulate_ops::types::*;
use crate::zetareticulate::utils::*;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Path;
use std::result;

use ndarray::Array2;
use numpy::{IntoPyArray, PyArrayDyn};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

#[pyfunction]
fn new_reticulate_numpy(artifacts_dir: &str, reticulate_assets_pbtxt: &str) -> PyResult<ScannNumpy> {
    ScannNumpy::new(artifacts_dir, reticulate_assets_pbtxt)
        .map_err(|e| PyErr::new::<PyException, _>(e.to_string()))
        .map(|zetareticulate| zetareticulate.into())
}

#[pyfunction]
fn from_np_dataset(
    np_dataset: &PyArrayDyn<f32>,
    config: &str,
    training_threads: i32,
) -> PyResult<ScannNumpy> {
    let dataset = NpRowMajorArr::from_pyarray(np_dataset);
    ScannNumpy::from_np_dataset(&dataset, config, training_threads)
        .map_err(|e| PyErr::new::<PyException, _>(e.to_string()))
        .map(|zetareticulate| zetareticulate.into())
}


#[pyclass]
struct ScannNumpy {
    zetareticulate: Scann,
}

#[pymethods]
impl ScannNumpy {
    #[new]
    fn new(artifacts_dir: &str, reticulate_assets_pbtxt: &str) -> PyResult<Self> {
        ScannNumpy::new(artifacts_dir, reticulate_assets_pbtxt)
            .map_err(|e| PyErr::new::<PyException, _>(e.to_string()))
    }

    #[staticmethod]
    fn from_np_dataset(
        np_dataset: &PyArrayDyn<f32>,
        config: &str,
        training_threads: i32,
    ) -> PyResult<Self> {
        let dataset = NpRowMajorArr::from_pyarray(np_dataset);
        ScannNumpy::from_np_dataset(&dataset, config, training_threads)
            .map_err(|e| PyErr::new::<PyException, _>(e.to_string()))
    }
}
// Define custom error type
#[derive(Debug)]
struct RuntimeError(String);

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Runtime error: {}", self.0)
    }
}

impl Error for RuntimeError {}

// Define a result type for convenience
type Result<T> = result::Result<T, RuntimeError>;

struct ScannNumpy {
    zetareticulate: Scann,
}

impl ScannNumpy {
    fn new(artifacts_dir: &str, reticulate_assets_pbtxt: &str) -> Result<Self> {
        let config = fs::read_to_string(format!("{}/reticulate_config.pb", artifacts_dir))
            .map_err(|e| RuntimeError(format!("Failed reading reticulate_config.pb: {}", e)))?;
        let zetareticulate = Scann::initialize(&config, reticulate_assets_pbtxt)
            .map_err(|e| RuntimeError(format!("Error initializing searcher: {}", e)))?;
        Ok(ScannNumpy { zetareticulate })
    }

    fn from_np_dataset(
        np_dataset: &NpRowMajorArr<f32>,
        config: &str,
        training_threads: i32,
    ) -> Result<Self> {
        if np_dataset.ndim() != 2 {
            return Err(RuntimeError("Dataset input must be two-dimensional".to_string()));
        }
        let dataset = ConstSpan::new(np_dataset.data(), np_dataset.size());
        let zetareticulate = Scann::initialize(&dataset, np_dataset.shape()[0], config, training_threads)
            .map_err(|e| RuntimeError(format!("Error initializing searcher: {}", e)))?;
        Ok(ScannNumpy { zetareticulate })
    }

    // Other methods would be implemented similarly
}

fn runtime_error_if_not_ok(prefix: &str, status: &Status) -> Result<()> {
    if !status.ok() {
        let msg = format!("{}{}", prefix, status.message());
        return Err(RuntimeError(msg));
    }
    Ok(())
}

fn value_or_runtime_error<T>(status_or: Result<T>, prefix: &str) -> T {
    match status_or {
        Ok(value) => value,
        Err(err) => panic!("{}", err),
    }
}

fn tensor_from_proto_type(
    context: &mut OpKernelContext,
    name: &str,
    proto: Option<&dyn MessageLite>,
    tensor_proto_type: TensorProtoType,
) -> Result<()> {
    if let Some(proto) = proto {
        let tensor = context.allocate_output(name, &TensorShape::from([1]), &mut)?;
        match tensor_proto_type {
            TensorProtoType::Float => {
                let mut tensor_value = tensor.scalar::<f32>()?;
                tensor_value[0] = proto[0];
            }
            _ => {
                return Err(RuntimeError(format!("Unsupported tensor proto type {:?}", tensor_proto_type)));
            }
        }
    } else {
        empty_tensor(context, name)?;
    }

    Ok(())
}









