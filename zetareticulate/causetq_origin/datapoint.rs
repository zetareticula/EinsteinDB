
use crate::features::{FeatureType, GenericFeatureVector};
use crate::gfv_conversion::UnpackBinaryToInt64;
use crate::hashed::HashedItem;
use crate::types::{ConstSpan, DimensionIndex, MutableSpan, Status};
use crate::utils::{InfiniteOneArray, IsFloatingType, IsUint8, NONE};
use std::cmp::min;
use std::mem::swap;
use std::slice;




pub enum Normalization {
    None,
    // Add more normalization options if needed
}

pub struct DatapointPtr<T> {
    indices: *const DimensionIndex,
    values: *const T,
    nonzero_entries: DimensionIndex,
    dimensionality: DimensionIndex,
}

impl<T> DatapointPtr<T> {
    pub fn indices(&self) -> Option<&[DimensionIndex]> {
        unsafe {
            if self.indices.is_null() {
                None
            } else {
                Some(slice::from_raw_parts(self.indices, self.nonzero_entries as usize))
            }
        }
    }

    pub fn indices_span(&self) -> ConstSpan<DimensionIndex> {
        unsafe { ConstSpan::new(self.indices, self.nonzero_entries as usize) }
    }

    pub fn has_values(&self) -> bool {
        !self.values.is_null()
    }

    pub fn values(&self) -> Option<&[T]> {
        unsafe {
            if self.values.is_null() {
                None
            } else {
                Some(slice::from_raw_parts(self.values, self.nonzero_entries as usize))
            }
        }
    }

        pub fn values_span(&self) -> ConstSpan<T> {
        unsafe { ConstSpan::new(self.values, self.nonzero_entries as usize) }
    }  pub fn values_span(&self) -> ConstSpan<T> {
        unsafe { ConstSpan::new(self.values, self.nonzero_entries as usize) }
    }

    pub fn nonzero_entries(&self) -> DimensionIndex {
        self.nonzero_entries
    }

    pub fn dimensionality(&self) -> DimensionIndex {
        self.dimensionality
    }

    pub fn is_dense(&self) -> bool {
        self.nonzero_entries > 0 && self.indices.is_null()
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
        if let Some(values) = self.values() {
            values.iter().all(|&val| val == T::default())
        } else {
            false
        }
    }

    pub fn is_finite(&self) -> bool
        where
            T: PartialEq + PartialOrd + std::fmt::Debug + std::num::Float,
    {
        if let Some(values) = self.values() {
            values.iter().all(|&val| val.is_finite())
        } else {
            true
        }
    }

    pub fn to_sparse_binary(&self) -> DatapointPtr<u8> {
        DatapointPtr {
            indices: self.indices,
            values: std::ptr::null(),
            nonzero_entries: self.nonzero_entries,
            dimensionality: self.dimensionality,
        }
    }
}

pub struct Datapoint<T> {
    indices: Vec<DimensionIndex>,
    values: Vec<T>,
    dimensionality: DimensionIndex,
    normalization: Normalization,
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

    // Implement other methods
}

// Implement other associated functions and traits for Datapoint<T>

pub const SPARSE_DIMENSIONALITY: DimensionIndex = DimensionIndex::MAX;

pub fn make_sparse_binary_datapoint_ptr(
    indices: ConstSpan<DimensionIndex>,
    dimensionality: DimensionIndex,
) -> DatapointPtr<u8> {
    DatapointPtr {
        indices: indices.as_ptr(),
        values: std::ptr::null(),
        nonzero_entries: indices.len() as DimensionIndex,
        dimensionality,
    }
}

pub fn make_datapoint_ptr<T>(
    indices: ConstSpan<DimensionIndex>,
    values: ConstSpan<T>,
    dimensionality: DimensionIndex,
) -> DatapointPtr<T> {
    if !indices.is_empty() && !values.is_empty() {
        assert_eq!(indices.len(), values.len());
    }
    DatapointPtr {
        indices: indices.as_ptr(),
        values: values.as_ptr(),
        nonzero_entries: min(indices.len(), values.len()) as DimensionIndex,
        dimensionality,
    }
}

pub fn make_datapoint<T>(
    indices: Vec<DimensionIndex>,
    values: Vec<T>,
    dimensionality: DimensionIndex,
) -> Datapoint<T> {
    Datapoint {
        indices,
        values,
        dimensionality,
        normalization: Normalization::None,
    }
}

pub fn make_sparse_binary_datapoint(
    indices: Vec<DimensionIndex>,
    dimensionality: DimensionIndex,
) -> Datapoint<u8> {
    Datapoint {
        indices,
        values: vec![1; indices.len()],
        dimensionality,
        normalization: Normalization::None,
    }
}

pub fn make_dense_datapoint<T>(
    values: Vec<T>,
    dimensionality: DimensionIndex,
) -> Datapoint<T> {
    Datapoint {
        indices: (0..dimensionality).collect(),
        values,
        dimensionality,
        normalization: Normalization::None,
    }
}

pub fn make_dense_datapoint_from_slice<T>(
    values: &[T],
    dimensionality: DimensionIndex,
) -> Datapoint<T>
    where
        T: Clone,
{
    Datapoint {
        indices: (0..dimensionality).collect(),
        values: values.to_vec(),
        dimensionality,
        normalization: Normalization::None,
    }
}

pub fn make_dense_datapoint_from_slice_with_normalization<T>(
    values: &[T],
    dimensionality: DimensionIndex,
    normalization: Normalization,
) -> Datapoint<T>
    where
        T: Clone,
{
    Datapoint {
        indices: (0..dimensionality).collect(),
        values: values.to_vec(),
        dimensionality,
        normalization,
    }
}

pub fn make_dense_datapoint_from_vec<T>(
    values: Vec<T>,
    dimensionality: DimensionIndex,
) -> Datapoint<T> {
    Datapoint {
        indices: (0..dimensionality).collect(),
        values,
        dimensionality,
        normalization: Normalization::None,
    }
}

