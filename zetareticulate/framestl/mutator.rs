use std::collections::HashSet;
use std::convert::From;
use std::fmt;
use std::ops::{Deref, Index};
use std::slice;


use crate::timelike::metric::DatapointPtr;
use crate::timelike::metric::Normalization;
use crate::timelike::metric::FeatureType;
use crate::timelike::metric::DimensionIndex;
use crate::timelike::metric::DatapointPtr;
use crate::timelike::metric::Datapoint;

// Path: zetareticulate/timelike/metric.rs



//now we define the structs and enums
#[derive(Debug, Clone)]
pub struct GenericFeatureVector<T> {
    feature_type: FeatureType,
    indices: Option<Vec<DimensionIndex>>,
    values: Option<Vec<T>>,
    dimensionality: DimensionIndex,
}


#[derive(Debug, Clone)]
pub struct Datapoint<T> {
    indices: Vec<DimensionIndex>,
    values: Vec<T>,
    dimensionality: DimensionIndex,
    normalization: Normalization,
}


#[derive(Debug, Clone)]
pub struct DatapointPtr<T> {
    indices: Option<Vec<DimensionIndex>>,
    values: Option<Vec<T>>,
    nonzero_entries: DimensionIndex,
    dimensionality: DimensionIndex,
}







type DimensionIndex = usize;

#[derive(Debug, Clone, Copy)]
pub enum Normalization {
    None,
    // Add more normalization options if needed
}

#[derive(Debug, Clone)]
pub struct DatapointPtr<T> {
    indices: Option<Vec<DimensionIndex>>,
    values: Option<Vec<T>>,
    nonzero_entries: DimensionIndex,
    dimensionality: DimensionIndex,
}


impl<T> DatapointPtr<T> {
    pub fn new() -> Self {
        Self {
            indices: None,
            values: None,
            nonzero_entries: 0,
            dimensionality: 0,
        }
    }

    pub fn indices(&self) -> Option<&[DimensionIndex]> {
        self.indices.as_deref()
    }

    pub fn has_values(&self) -> bool {
        self.values.is_some()
    }

    pub fn values(&self) -> Option<&[T]> {
        self.values.as_deref()
    }

    pub fn nonzero_entries(&self) -> DimensionIndex {
        self.nonzero_entries
    }

    pub fn dimensionality(&self) -> DimensionIndex {
        self.dimensionality
    }
}




impl<T> DatapointPtr<T> {
    pub fn new() -> Self {
        Self {
            indices: None,
            values: None,
            nonzero_entries: 0,
            dimensionality: 0,
        }
    }

    pub fn indices(&self) -> Option<&[DimensionIndex]> {
        self.indices.as_deref()
    }

    pub fn has_values(&self) -> bool {
        self.values.is_some()
    }

    pub fn values(&self) -> Option<&[T]> {
        self.values.as_deref()
    }

    pub fn nonzero_entries(&self) -> DimensionIndex {
        self.nonzero_entries
    }

    pub fn dimensionality(&self) -> DimensionIndex {
