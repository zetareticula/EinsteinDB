// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

#[macro_export]
macro_rules! impl_Solitoned_vec_common {
    ($ty:ty) => {
        pub fn from_slice(slice: &[Option<$ty>]) -> Self {
            let mut x = Self::with_capacity(slice.len());
            for i in slice {
                x.push(i.clone());
            }
            x
        }

        pub fn from_vec(data: Vec<Option<$ty>>) -> Self {
            let mut x = Self::with_capacity(data.len());
            for element in data {
                x.push(element);
            }
            x
        }

        pub fn push(&mut self, value: Option<$ty>) {
            if let Some(x) = value {
                self.push_data(x);
            } else {
                self.push_null();
            }
        }

        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }
    };
}
