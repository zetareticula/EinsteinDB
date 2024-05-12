
use crate::features::{FeatureType, GenericFeatureVector};
use crate::types::{DimensionIndex, Status};
use crate::utils::{DivRoundUp, ZipSortBranchOptimized};
use log::{error, log_once};
use std::cmp::{max, min};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::ops::{Index, IndexMut};

pub struct DatapointIndex {
    index: usize,
}

impl DatapointIndex {
    pub fn new(index: usize) -> Self {
        Self { index }
    }
}

impl Index<usize> for DatapointIndex {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        &self.index
    }
}

impl IndexMut<usize> for DatapointIndex {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.index
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

    pub fn clear(&mut self) {
        self.indices.clear();
        self.values.clear();
        self.dimensionality = 0;
        self.normalization = Normalization::None;
    }

    pub fn is_sparse(&self) -> bool {
        self.indices.len() < self.dimensionality
    }

    pub fn is_dense(&self) -> bool {
        self.indices.len() == self.dimensionality
    }

    pub fn dimensionality(&self) -> DimensionIndex {
        self.dimensionality
    }

    pub fn get_element_packed(&self, dimension_index: DimensionIndex) -> T
        where
            T: Default + Clone,
    {
        assert!(dimension_index < self.dimensionality);
        if self.indices.is_empty() {
            return T::default();
        }
        let found = self.indices.binary_search(&dimension_index);
        match found {
            Ok(index) => self.values[index].clone(),
            Err(_) => T::default(),
        }
    }

    pub fn sort_indices(&mut self) {
        if self.indices.is_empty() {
            return;
        }
        ZipSortBranchOptimized::zip_sort(&mut self.indices, &mut self.values);
    }

    pub fn remove_explicit_zeroes_from_sparse_vector(&mut self) {
        remove_explicit_zeroes_from_sparse_vector(&mut self.indices, &mut self.values);
    }

    pub fn get_element(&self, dimension_index: DimensionIndex) -> T
        where
            T: Default + Clone,
    {
        assert!(dimension_index < self.dimensionality);
        if self.is_dense() {
            return self.get_element_packed(dimension_index);
        } else {
            if self.indices.is_empty() {
                return T::default();
            }
            let found = self.indices.binary_search(&dimension_index);
            match found {
                Ok(index) => self.values[index].clone(),
                Err(_) => T::default(),
            }
        }
    }

    pub fn union_indices_impl(&self, union_hash: &mut HashSet<DimensionIndex>) {
        for &idx in &self.indices {
            union_hash.insert(idx);
        }
    }

    pub fn union_indices(&self)
        -> Vec<DimensionIndex>
    {
        let mut union_hash: HashSet<DimensionIndex> = HashSet::new();
        self.union_indices_impl(&mut union_hash);
        union_hash.into_iter().collect()
    }

    pub fn to_gfv(&self) -> GenericFeatureVector {
        let mut gfv = GenericFeatureVector::new();
        gfv.set_norm_type(FeatureType::from_i32(self.normalization as i32));
        gfv.set_feature_dim(self.dimensionality);

        for &idx in &self.indices {
            gfv.add_feature_index(idx);
        }

        for val in &self.values {
            match val.try_into() {
                Ok(v) => gfv.add_feature_value_int64(v),
                Err(_) => {}
            }
        }

        gfv
    }

    pub fn from_gfv(&mut self, gfv: &GenericFeatureVector) -> Result<(), String>
        where
            T: TryFrom<i64> + Default + Clone,
    {
        self.clear();
        self.normalization = match gfv.norm_type() {
            FeatureType::None => Normalization::None,
            // Add more cases if needed
            _ => return Err("Invalid normalization type".to_string()),
        };
        self.dimensionality = gfv.feature_dim();
        if gfv.feature_type() == FeatureType::String {
            return Err("Invalid feature type".to_string());
        }

        self.indices = gfv.feature_index().to_vec();

        let is_binary = gfv.feature_type() == FeatureType::Binary;
        if is_binary && !self.indices.is_empty() {
            if !T::default().clone().try_into().is_ok() {
                self.values = vec![T::default(); self.indices.len()];
            }
        } else {
            let values = gfv.feature_value_int64();
            for val in values {
                if let Ok(v) = T::try_from(val) {
                    self.values.push(v);
                } else {
                    return Err("Failed to convert feature value to desired type".to_string());
                }
            }
        }

        if self.indices.is_empty() {
            return Ok(());
        }

        if self.indices.len() != self.values.len() && !is_binary {
            return Err("Size of indices does not match size of values".to_string());
        }

        let mut need_check_dupes = false;
        for i in 1..self.indices.len() {
            if self.indices[i - 1] >= self.indices[i] {
                self.sort_indices();
                need_check_dupes = true;
                break;
            }
        }

        if self.indices.last().cloned().unwrap_or_default() >= self.dimensionality {
            return Err("Largest dimension index is >= dimensionality".to_string());
        }

        if need_check_dupes {
            let mut set = HashSet::new();
            for &idx in &self.indices {
                if !set.insert(idx) {
                    log_once!(
                        error,
                        "Found duplicate indices when parsing GenericFeatureVector"
                    );
                    return Err("Invalid sparse vector. Found duplicate dimension index".to_string());
                }
            }
        }

        self.remove_explicit_zeroes_from_sparse_vector();

        Ok(())
    }
}

impl<T> Index<usize> for Datapoint<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

impl<T> IndexMut<usize> for Datapoint<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.values[index]
    }
}

impl<T> Index<DimensionIndex> for Datapoint<T> {
    type Output = T;

    fn index(&self, index: DimensionIndex) -> &Self::Output {
        &self.get_element(index)
    }
}


impl<T> IndexMut<DimensionIndex> for Datapoint<T> {
    fn index_mut(&mut self, index: DimensionIndex) -> &mut Self::Output {
        &mut self.values[index as usize]
    }
}

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
    pub fn has_nonzero(&self, dimension_index: DimensionIndex) -> bool {
        assert!(dimension_index < self.dimensionality());
        assert!(self.is_sparse());
        if self.nonzero_entries == 0 {
            return false;
        }
        let found = unsafe {
            std::slice::from_raw_parts(self.indices, self.nonzero_entries as usize)
                .binary_search(&dimension_index)
        };
        found.is_ok()
    }

    pub fn get_element(&self, dimension_index: DimensionIndex) -> T
        where
            T: Default + Clone,
    {
        assert!(dimension_index < self.dimensionality());
        if self.is_dense() {
            if self.dimensionality == self.nonzero_entries {
                return unsafe { *self.values.add(dimension_index as usize) };
            } else {
                return self.get_element_packed(dimension_index);
            }
        } else {
            if self.nonzero_entries == 0 {
                return T::default();
            }
            let found = unsafe {
                std::slice::from_raw_parts(self.indices, self.nonzero_entries as usize)
                    .binary_search(&dimension_index)
            };
            if let Ok(index) = found {
                return if self.values.is_null() {
                    T::default()
                } else {
                    unsafe { *self.values.add(index) }
                };
            } else {
                return T::default();
            }
        }
    }

    pub fn to_gfv_indices_and_metadata(&self, gfv: &mut GenericFeatureVector) {
        if self.is_sparse() {
            for i in 0..self.nonzero_entries {
                gfv.add_feature_index(unsafe { *self.indices.add(i as usize) });
            }
            gfv.set_feature_dim(self.dimensionality);
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
    pub fn from_gfv(&mut self, gfv: &GenericFeatureVector) -> Result<(), String>
        where
            T: TryFrom<i64> + Default + Clone,
    {
        self.clear();
        self.normalization = match gfv.norm_type() {
            FeatureType::None => Normalization::None,
            // Add more cases if needed
            _ => return Err("Invalid normalization type".to_string()),
        };
        self.dimensionality = gfv.feature_dim();
        if gfv.feature_type() == FeatureType::String {
            return Err("Invalid feature type".to_string());
        }

        self.indices = gfv.feature_index().to_vec();

        let is_binary = gfv.feature_type() == FeatureType::Binary;
        if is_binary && !self.indices.is_empty() {
            if !T::default().clone().try_into().is_ok() {
                self.values = vec![T::default(); self.indices.len()];
            }
        } else {
            let values = gfv.feature_value_int64();
            for val in values {
                if let Ok(v) = T::try_from(val) {
                    self.values.push(v);
                } else {
                    return Err("Failed to convert feature value to desired type".to_string());
                }
            }
        }

        if self.indices.is_empty() {
            return Ok(());
        }

        if self.indices.len() != self.values.len() && !is_binary {
            return Err("Size of indices does not match size of values".to_string());
        }

        let mut need_check_dupes = false;
        for i in 1..self.indices.len() {
            if self.indices[i - 1] >= self.indices[i] {
                self.sort_indices();
                need_check_dupes = true;
                break;
            }
        }

        if self.indices.last().cloned().unwrap_or_default() >= self.dimensionality {
            return Err("Largest dimension index is >= dimensionality".to_string());
        }

        if need_check_dupes {
            let mut set = HashSet::new();
            for &idx in &self.indices {
                if !set.insert(idx) {
                    log_once!(
                        error,
                        "Found duplicate indices when parsing GenericFeatureVector"
                    );
                    return Err("Invalid sparse vector. Found duplicate dimension index".to_string());
                }
            }
        }

        self.remove_explicit_zeroes_from_sparse_vector();

        Ok(())
    }
}

pub trait ToGenericFeatureVector {
    fn to_gfv(&self) -> GenericFeatureVector;
}

impl<T> ToGenericFeatureVector for Datapoint<T> {
    fn to_gfv(&self) -> GenericFeatureVector {
        let mut gfv = GenericFeatureVector::new();
        gfv.set_norm_type(FeatureType::from_i32(self.normalization as i32));
        gfv.set_feature_dim(self.dimensionality);

        for &idx in &self.indices {
            gfv.add_feature_index(idx);
        }

        for val in &self.values {
            match val.try_into() {
                Ok(v) => gfv.add_feature_value_int64(v),
                Err(_) => {}
            }
        }

        gfv
    }
}

// Implement other methods and traits for Datapoint<T>

pub fn remove_explicit_zeroes_from_sparse_vector<T>(
    indices: &mut Vec<DimensionIndex>,
    values: &mut Vec<T>,
) {
    if indices.is_empty() || values.is_empty() {
        return;
    }

    let mut from = 0;
    let mut to = 0;
    for from in 0..values.len() {
        if values[from] == T::default() {
            continue;
        }
        values[to] = values[from].clone();
        indices[to] = indices[from];
        to += 1;
    }

    indices.resize(to, Default::default());
    values.resize(to, T::default());
}

pub fn make_not_binary<T>(datapoint: &mut Datapoint<T>)
    where
        T: TryFrom<i64> + Default + Clone,
{
    let mut values = &mut datapoint.values;
    if values.is_empty() {
        values.resize(datapoint.indices.len(), T::default());
    } else if T::default().clone().try_into().is_ok() && datapoint.is_dense() {
        if datapoint.nonzero_entries < datapoint.dimensionality {
            let new_values: Vec<T> = (0..datapoint.dimensionality)
                .map(|i| datapoint.get_element_packed(i))
                .collect();
            *values = new_values;
        }
    }
}


pub fn sparse_binary_dot_product<T>(a: &DatapointPtr<T>, b: &DatapointPtr<T>) -> DimensionIndex {
    let mut num_intersect = 0;
    let mut a_index = 0;
    let mut b_index = 0;
    while a_index < a.nonzero_entries && b_index < b.nonzero_entries {
        let a_idx = unsafe { *a.indices.add(a_index as usize) };
        let b_idx = unsafe { *b.indices.add(b_index as usize) };
        if a_idx == b_idx {
            num_intersect += 1;
            a_index += 1;
            b_index += 1;
        } else if a_idx < b_idx {
            a_index += 1;
        } else {
            b_index += 1;
        }
    }
    num_intersect
}




