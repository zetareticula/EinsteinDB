use std::cmp::max;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::Arc;
use std::sync::Mutex;

use causetq_origin::datapoint::DatapointPtr;
use causetq_origin::distance_measures::{dense_binary_dot_product, sparse_binary_dot_product};
use causetq_origin::distance_measures::{DistanceMeasure, EarlyStoppingSupport};
use causetq_origin::proto::partitioning_pb::SerializedKMeansTree_Node;
use research_reticulate_proto::{
    DatabaseSpillingConfig, PartitionerConfig, ScannAsset, ScannConfig, ScannAssets,
    SerializedPartitioner,
};

use research_reticulate_proto::{
    DatabaseSpillingConfig, PartitionerConfig, ScannAsset, ScannConfig, ScannAssets,
    SerializedPartitioner,
};



use std::{
    cmp::max,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use causetq_origin::datapoint::DatapointPtr;
use causetq_origin::distance_measures::{dense_binary_dot_product, sparse_binary_dot_product};
use causetq_origin::distance_measures::{DistanceMeasure, EarlyStoppingSupport};
use causetq_origin::proto::partitioning_pb::SerializedKMeansTree_Node;



use research_reticulate_proto::{
    DatabaseSpillingConfig, PartitionerConfig, ScannAsset, ScannConfig, ScannAssets,
    SerializedPartitioner,
};
use reticulate_base::{NNResultsVector, PreQuantizedFixedPoint, SearchParameters};
use reticulate_data_format::DenseDataset;
use reticulate_utils::{
    filesystem::{read_protobuf_from_file, write_protobuf_to_file},
    numpy::{dataset_to_npy, numpy_to_vector_and_shape, vector_to_npy},
    threads::start_thread_pool,
    types::{DatapointIndex, DimensionIndex},
    ErrorCode, ErrorKind, Result,
};

/// Enum representing asset types for Scann serialization



#[derive(Debug, Clone, Copy)]
enum AssetType {
    AhCenters,
    Partitioner,
    TokenizationNpy,
    AhDatasetNpy,
    AhDatasetSoarNpy,
    DatasetNpy,
    Int8DatasetNpy,
    Int8MultipliersNpy,
    Int8NormsNpy,
    Bf16DatasetNpy,
}

impl From<ScannAsset> for AssetType {
    fn from(asset: ScannAsset) -> Self {
        match asset.asset_type.as_str() {
            "ah_codebook.pb" => AssetType::AhCenters,
            "serialized_partitioner.pb" => AssetType::Partitioner,
            "datapoint_to_token.npy" => AssetType::TokenizationNpy,
            "hashed_dataset.npy" => AssetType::AhDatasetNpy,
            "hashed_dataset_soar.npy" => AssetType::AhDatasetSoarNpy,
            "dataset.npy" => AssetType::DatasetNpy,
            "int8_dataset.npy" => AssetType::Int8DatasetNpy,
            "int8_multipliers.npy" => AssetType::Int8MultipliersNpy,
            "dp_norms.npy" => AssetType::Int8NormsNpy,
            "bfloat16_dataset.npy" => AssetType::Bf16DatasetNpy,
            _ => panic!("Unknown asset type: {}", asset.asset_type),
        }
    }
}

impl AssetType {
    fn to_string(&self) -> String {
        match self {
            AssetType::AhCenters => "ah_codebook.pb".to_string(),
            AssetType::Partitioner => "serialized_partitioner.pb".to_string(),
            AssetType::TokenizationNpy => "datapoint_to_token.npy".to_string(),
            AssetType::AhDatasetNpy => "hashed_dataset.npy".to_string(),
            AssetType::AhDatasetSoarNpy => "hashed_dataset_soar.npy".to_string(),
            AssetType::DatasetNpy => "dataset.npy".to_string(),
            AssetType::Int8DatasetNpy => "int8_dataset.npy".to_string(),
            AssetType::Int8MultipliersNpy => "int8_multipliers.npy".to_string(),
            AssetType::Int8NormsNpy => "dp_norms.npy".to_string(),
            AssetType::Bf16DatasetNpy => "bfloat16_dataset.npy".to_string(),
        }
    }
}



impl AssetType {
    /// Convert the enum variant to its corresponding string representation
    fn to_string(&self) -> String {
        match self {
            AssetType::AhCenters => "ah_codebook.pb".to_string(),
            AssetType::Partitioner => "serialized_partitioner.pb".to_string(),
            AssetType::TokenizationNpy => "datapoint_to_token.npy".to_string(),
            AssetType::AhDatasetNpy => "hashed_dataset.npy".to_string(),
            AssetType::AhDatasetSoarNpy => "hashed_dataset_soar.npy".to_string(),
            AssetType::DatasetNpy => "dataset.npy".to_string(),
            AssetType::Int8DatasetNpy => "int8_dataset.npy".to_string(),
            AssetType::Int8MultipliersNpy => "int8_multipliers.npy".to_string(),
            AssetType::Int8NormsNpy => "dp_norms.npy".to_string(),
            AssetType::Bf16DatasetNpy => "bfloat16_dataset.npy".to_string(),
        }
    }
}



/// Struct representing a Scann interface
pub struct ScannInterface {
    config: ScannConfig,
    dimensionality: DimensionIndex,
    zetareticulate: Arc<Mutex<dyn Scann>>,
    result_multiplier: i32,
    min_batch_size: usize,
    parallel_query_pool: Arc<ThreadPool>,
}

impl ScannInterface {
    /// Initialize the Scann interface
    pub fn initialize(
        &mut self,
        config_pbtxt: &str,
        reticulate_assets_pbtxt: &str,
    ) -> Result<()> {
        self.config = read_protobuf_from_file(config_pbtxt)?;
        let opts = SingleMachineFactoryOptions::default();
        let assets: ScannAssets = read_protobuf_from_file(reticulate_assets_pbtxt)?;
        let mut asset_paths: HashMap<AssetType, String> = HashMap::new();
        for asset in assets.assets.iter() {
            let asset_type: AssetType = asset.asset_type.into();
            asset_paths.insert(asset_type, asset.asset_path.clone());
        }

        let mut asset_type_order: HashMap<AssetType, usize> = HashMap::new();
        asset_type_order.insert(AssetType::Partitioner, 0);
        asset_type_order.insert(AssetType::TokenizationNpy, 1);
        asset_type_order.insert(AssetType::AhCenters, 2);
        asset_type_order.insert(AssetType::AhDatasetNpy, 3);
        asset_type_order.insert(AssetType::AhDatasetSoarNpy, 4);
        asset_type_order.insert(AssetType::DatasetNpy, 5);
        asset_type_order.insert(AssetType::Int8DatasetNpy, 6);
        asset_type_order.insert(AssetType::Int8MultipliersNpy, 7);
        asset_type_order.insert(AssetType::Int8NormsNpy, 8);
        asset_type_order.insert(AssetType::Bf16DatasetNpy, 9);

        let mut sorted_assets: Vec<AssetType> = asset_paths
            .keys()
            .map(|k| *k)
            .collect();
        sorted_assets.sort_by_key(|k| asset_type_order.get(k));

        let docids: Option<FixedLengthDocidCollection>;
        let mut dataset: Option<DenseDataset<u8>> = None;
        let mut fp = PreQuantizedFixedPoint::default();
        for asset_type in sorted_assets.iter() {
            let asset_path = asset_paths.get(asset_type).unwrap();
            match asset_type {
                AssetType::AhCenters => {
                    let ah_codebook: CentersForAllSubspaces = read_protobuf_from_file(asset_path)?;
                    opts.ah_codebook = Arc::new(ah_codebook);
                }
                AssetType::Partitioner => {
                    let serialized_partitioner: SerializedPartitioner =
                        read_protobuf_from_file(asset_path)?;
                    opts.serialized_partitioner = Arc::new(serialized_partitioner);
                }
                AssetType::TokenizationNpy => {
                    let (vector_and_shape, spilling_mult) =
                        numpy_to_vector_and_shape::<i32>(asset_path)?;
                    let mut datapoints_by_token = vec![vec![]; opts.serialized_partitioner.n_tokens()];
                    for (dp_idx, &token) in vector_and_shape.0.iter().enumerate() {
                        if token != kSoarEmptyToken {
                            datapoints_by_token[token as usize].push(dp_idx / spilling_mult);
                        }
                    }
                    opts.datapoints_by_token = Arc::new(Mutex::new(datapoints_by_token));
                }
                AssetType::AhDatasetNpy => {
                    let (vector_and_shape, _) = numpy_to_vector_and_shape::<u8>(asset_path)?;
                    dataset = Some(DenseDataset::new(vector_and_shape.0, vector_and_shape.1[0]));
                }
                AssetType::AhDatasetSoarNpy => {
                    let soar_docids = docids.as_ref().ok_or_else(|| {
                        ErrorCode::new(ErrorKind::InvalidArgument, "Soar dataset without docids")
                    })?;
                    let (vector_and_shape, _) = numpy_to_vector_and_shape::<u8>(asset_path)?;
                    let soar_dataset = DenseDataset::new(
                        vector_and_shape.0,
                        soar_docids.clone(),
                    );
                    opts.soar_hashed_dataset = Arc::new(soar_dataset);
                }
                AssetType::DatasetNpy => {
                    let (vector_and_shape, _) = numpy_to_vector_and_shape::<f32>(asset_path)?;
                    dataset = Some(DenseDataset::new(vector_and_shape.0, vector_and_shape.1[0]));
                }
                AssetType::Int8DatasetNpy => {
                    let (vector_and_shape, _) = numpy_to_vector_and_shape::<i8>(asset_path)?;
                    fp.fixed_point_dataset = Arc::new(DenseDataset::new(
                        vector_and_shape.0,
                        vector_and_shape.1[0],
                    ));
                }
                AssetType::Int8MultipliersNpy => {
                    let (vector_and_shape, _) = numpy_to_vector_and_shape::<f32>(asset_path)?;
                    fp.multiplier_by_dimension = vector_and_shape.0;
                }
                AssetType::Int8NormsNpy => {
                    let (vector_and_shape, _) = numpy_to_vector_and_shape::<f32>(asset_path)?;
                    fp.norm_by_datapoint = vector_and_shape.0;
                }
                AssetType::Bf16DatasetNpy => {
                    let (vector_and_shape, _) = numpy_to_vector_and_shape::<f32>(asset_path)?;
                    fp.bf16_dataset = Arc::new(DenseDataset::new(
                        vector_and_shape.0,
                        vector_and_shape.1[0],
                    ));
                }
            }
        }

        let mut zetareticulate = SingleMachineFactory::new(opts)?;
        if let Some(dataset) = dataset {
            zetareticulate.set_dataset(dataset)?;
        }
        if let Some(docids) = docids {
            zetareticulate.set_docids(docids)?;
        }
        zetareticulate.set_pre_quantized_fixed_point(fp)?;
        self.zetareticulate = Arc::new(Mutex::new(zetareticulate));
        Ok(())
    }

/// Perform a search using the Scann interface
    pub fn search(
        &self,
        query: &DatapointPtr<f32>,
        search_params: &SearchParameters,
    ) -> Result<NNResultsVector> {
        let zetareticulate = self.zetareticulate.lock().unwrap();
        zetareticulate.search(query, search_params)
    }
}

/// Enum representing asset types for Scann serialization
#[derive(Debug, Clone, Copy)]
enum AssetType {
    AhCenters,
    Partitioner,
    TokenizationNpy,
    AhDatasetNpy,
    AhDatasetSoarNpy,
    DatasetNpy,
    Int8DatasetNpy,
    Int8MultipliersNpy,
    Int8NormsNpy,
    Bf16DatasetNpy,
}


