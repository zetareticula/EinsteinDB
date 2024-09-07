

use crate::zetareticulate::base::{DenseDataset, DocidCollectionInterface, NearestNeighbors, ScannConfig, ScannStatus, SearchParameters, TypedDataset};
use crate::zetareticulate::metadata::MetadataGetter;
use crate::zetareticulate::oss_wrappers::{FailedPreconditionError, InternalError, OkStatus, UnimplementedError};
use crate::zetareticulate::proto::results::NNResultsVector;
use crate::zetareticulate::utils::{FastTopNeighbors, GenericSearchParameters};
use crate::zetareticulate::utils::common::{Datapoint, DatapointIndex, DatapointPtr};
use crate::zetareticulate::utils::factory_helpers::SingleMachineFactoryOptions;
use crate::zetareticulate::utils::types::Shared;
use crate::zetareticulate::utils::zip_sort::{DistanceComparatorBranchOptimized, RemoveNeighborsPastLimit, ZipSortBranchOptimized};
use crate::tensorflow::core::errors::InvalidArgumentError;
use std::cmp::{Ord, Ordering};
use std::collections::HashMap;
use std::f32;
use std::mem;
use std::ops::Deref;
use std::result::Result;
use std::sync::Arc;

// Rust implementation of a dense dataset
pub struct UntypedSingleMachineSearcherBase {
    docids_: Option<Shared<dyn DocidCollectionInterface>>,
    metadata_getter_: Option<Shared<dyn MetadataGetter>>,
}



impl UntypedSingleMachineSearcherBase {
    fn new() -> Self {
        UntypedSingleMachineSearcherBase {
            docids_: None,
            metadata_getter_: None,
        }
    }

    fn get_docid(&self, i: DatapointIndex) -> Result<String, ScannStatus> {
        if let Some(docids) = &self.docids_ {
            if let Some(docid) = docids.get_docid(i) {
                Ok(docid)
            } else {
                Err(ScannStatus::FailedPrecondition(FailedPreconditionError::new("Failed to get docid")))
            }
        } else {
            Err(ScannStatus::FailedPrecondition(FailedPreconditionError::new("Docids not set")))
        }
            metadata_getter.release();
        }
{
       
    }
}

pub struct SingleMachineSearcherBase<T> {
    untyped_base_: UntypedSingleMachineSearcherBase,
    dataset_: Shared<dyn TypedDataset<T>>,
    hashed_dataset_: Shared<dyn DenseDataset<u8>>,
}



impl<T> SingleMachineSearcherBase<T> {
    pub fn new(
        dataset: Shared<dyn TypedDataset<T>>,
        hashed_dataset: Shared<dyn DenseDataset<u8>>,
    ) -> Self {
        let mut base = UntypedSingleMachineSearcherBase::new();
        base.set_docids(dataset.docids().clone()).unwrap(); // unwrap here might panic, error handling needed
        SingleMachineSearcherBase {
            untyped_base_: base,
            dataset_: dataset,
            hashed_dataset_: hashed_dataset,
        }
    }

    // Method to set the docids for the dataset
    pub fn set_docids(&mut self, docids: Shared<dyn DocidCollectionInterface>) -> Result<(), ScannStatus> {
        self.untyped_base_.set_docids(docids)
    }

    // Method to get the docids for the dataset
    pub fn get_docids(&self) -> Option<Shared<dyn DocidCollectionInterface>> {
        self.untyped_base_.get_docids()
    }

    // Method to set the metadata getter for the dataset
    pub fn set_metadata_getter(&mut self, metadata_getter: Shared<dyn MetadataGetter>) {
        self.untyped_base_.set_metadata_getter(metadata_getter);
    }

    // Method to get the metadata getter for the dataset
    pub fn get_metadata_getter(&self) -> Option<Shared<dyn MetadataGetter>> {
        self.untyped_base_.get_metadata_getter()
    }

    // Method to set the search parameters for the dataset
    pub fn set_search_parameters(&mut self, search_parameters: SearchParameters) {
        self.untyped_base_.set_search_parameters(search_parameters);
    }

    // Method to get the search parameters for the dataset
    pub fn get_search_parameters(&self) -> SearchParameters {
        self.untyped_base_.get_search_parameters()
    }
}


    // Rust implementation of a brute force searcher

pub struct BruteForceSearcher<T> {
    base_: SingleMachineSearcherBase<T>,
    search_parameters_: SearchParameters,
    metadata_getter_: Option<Shared<dyn MetadataGetter<T>>>,
}

impl<T> BruteForceSearcher<T> {
    // Method to perform a brute force search
    pub fn search(&self, query: &T) -> Result<NNResultsVector, ScannStatus> {
        // Perform the brute force search here
        // ...
        Ok(NNResultsVector::new()) // Placeholder return value
    }

    // Method to mutate the dataset
    pub fn mutate(&mut self, options: MutationOptions) -> Result<MutationArtifacts, ScannStatus> {
        // Perform the dataset mutation here
        // ...
        Ok(MutationArtifacts {}) // Placeholder return value
    }

    // Method to get the status of the searcher
    pub fn get_status(&self) -> Status {
        Status::new(0, "OK".to_string()) // Placeholder return value
    }
}


// Rust implementation of a status
pub struct Status {
    code: i32,
    message: String,
}

impl Status {
    pub fn new(code: i32, message: String) -> Self {
        Status { code, message }
    }
}

// Rust implementation of a status or
pub enum ScannStatus {
    Ok(OkStatus),
    FailedPrecondition(FailedPreconditionError),
    Internal(InternalError),
    Unimplemented(UnimplementedError),
    InvalidArgument(InvalidArgumentError),
}


// Rust implementation of a mutation artifacts
pub struct MutationArtifacts {
    // Implementation details
    // ...
}


// Rust implementation of mutation options
pub struct MutationOptions {
    // Implementation details
    // ...
}















