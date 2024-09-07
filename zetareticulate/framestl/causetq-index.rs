use std::collections::HashSet;
use std::convert::From;
use std::fmt;
use std::ops::{Deref, Index};

// Path: zetareticulate/timelike/metric.rs

// Now we define the structs and enums
#[derive(Debug, Clone)]
pub struct GenericFeatureVector<T> {
    feature_type: FeatureType,
    indices: Vec<DimensionIndex>,
    values: Vec<T>,
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
    L1,
    L2,
    LInf,
}

#[derive(Debug, Clone, Copy)]
pub enum FeatureType {
    Dense,
    Sparse,
    SparseBinary,
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

    pub fn is_dense(&self) -> bool {
        self.nonzero_entries > 0 && self.indices.is_none()
    }

    pub fn is_sparse(&self) -> bool {
        !self.is_dense()
    }

    pub fn is_sparse_origin(&self) -> bool {
        self.nonzero_entries == 0
    }

    pub fn is_all_ones(&self) -> bool
    where
        T: PartialEq + Default,
    {
        if let Some(values) = &self.values {
            values.iter().all(|&val| val == T::default())
        } else {
            false
        }
    }

    pub fn is_finite(&self) -> bool
    where
        T: PartialEq + PartialOrd + std::fmt::Debug + std::num::Float,
    {
        if let Some(values) = &self.values {
            values.iter().all(|&val| val.is_finite())
        } else {
            true
        }
    }

    pub fn to_sparse_binary(&self) -> Self {
        Self {
            indices: self.indices.clone(),
            values: None,
            nonzero_entries: self.nonzero_entries,
            dimensionality: self.dimensionality,
        }
    }
}

impl<T> Deref for DatapointPtr<T> {
    type Target = Datapoint<T>;

    fn deref(&self) -> &Self::Target {
        &self.to_datapoint()
    }
}

impl<T> Datapoint<T> {
    pub fn new() -> Self {
        Self {
            indices: Vec::new(),
            values: Vec::new(),
            dimensionality: 0,
            normalization: Normalization::None,
        }
    }

    pub fn from_gfv(&mut self, gfv: &GenericFeatureVector<T>) -> Result<(), ()> {
        // Implement conversion from GenericFeatureVector to Datapoint
        unimplemented!()
    }

    pub fn indices(&self) -> &[DimensionIndex] {
        &self.indices
    }

    pub fn mutable_indices(&mut self) -> &mut Vec<DimensionIndex> {
        &mut self.indices
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }

    pub fn mutable_values(&mut self) -> &mut Vec<T> {
        &mut self.values
    }

    pub fn nonzero_entries(&self) -> DimensionIndex {
        if self.is_dense() {
            self.values.len() as DimensionIndex
        } else {
            self.indices.len() as DimensionIndex
        }
    }

    pub fn dimensionality(&self) -> DimensionIndex {
        if self.dimensionality == 0 {
            self.nonzero_entries()
        } else {
            self.dimensionality
        }
    }

    pub fn set_dimensionality(&mut self, new_value: DimensionIndex) {
        self.dimensionality = new_value;
    }

    pub fn is_dense(&self) -> bool {
        self.indices.is_empty() && !self.values.is_empty()
    }

    pub fn is_sparse(&self) -> bool {
        !self.is_dense()
    }

    pub fn is_sparse_binary(&self) -> bool {
        self.values.is_empty()
    }

    pub fn to_ptr(&self) -> DatapointPtr<T> {
        DatapointPtr {
            indices: Some(self.indices.clone()),
            values: Some(self.values.clone()),
            nonzero_entries: self.nonzero_entries(),
            dimensionality: self.dimensionality(),
        }
    }

    pub fn clear(&mut self) {
        self.indices.clear();
        self.values.clear();
        self.dimensionality = 0;
        self.normalization = Normalization::None;
    }

    pub fn zero_fill(&mut self, dimensionality: DimensionIndex) {
        self.clear();
        self.values = Vec::with_capacity(dimensionality as usize);
    }

    pub fn normalization(&self) -> Normalization {
        self.normalization
    }

    pub fn set_normalization(&mut self, val: Normalization) {
        self.normalization = val;
    }

    pub fn swap(&mut self, rhs: &mut Self) {
        std::mem::swap(self, rhs);
    }
}

impl<T> From<GenericFeatureVector<T>> for Datapoint<T> {
    fn from(gfv: GenericFeatureVector<T>) -> Self {
        let indices = gfv.indices;
        let values = gfv.values;
        let dimensionality = gfv.dimensionality;
        let normalization = Normalization::None; // Set correct normalization
        Self {
            indices,
            values,
            dimensionality,
            normalization,
        }
    }
}

impl<T> Index<usize> for DatapointPtr<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.values().expect("Values are not available")[index]
    }
}

impl<T> Index<usize> for Datapoint<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

#[derive(Debug)]
pub enum Error {
    NotImplemented,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Not implemented")
    }
}

impl std::error::Error for Error {}

