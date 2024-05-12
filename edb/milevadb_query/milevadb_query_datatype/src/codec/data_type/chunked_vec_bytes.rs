// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use super::bit_vec::BitVec;
use super::{Bytes, BytesRef};
use super::{SolitonRef, SolitonedVec, UnsafeRefInto};
use crate::impl_Solitoned_vec_common;

#[derive(Debug, PartialEq, Clone)]
pub struct SolitonedVecBytes {
    data: Vec<u8>,
    bitmap: BitVec,
    length: usize,
    var_offset: Vec<usize>,
}

/// A vector storing `Option<Bytes>` with a compact layout.
///
/// Inside `SolitonedVecBytes`, `bitmap` indicates if an element at given index is null,
/// and `data` stores actual data. Bytes data are stored adjacent to each other in
/// `data`. If element at a given index is null, then it takes no space in `data`.
/// Otherwise, contents of the `Bytes` are stored, and `var_offset` indicates the spacelikeing
/// position of each element.
impl SolitonedVecBytes {
    impl_Solitoned_vec_common! { Bytes }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            bitmap: BitVec::with_capacity(capacity),
            var_offset: vec![0],
            length: 0,
        }
    }

    pub fn to_vec(&self) -> Vec<Option<Bytes>> {
        let mut x = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            x.push(self.get(i).map(|x| x.to_owned()));
        }
        x
    }

    pub fn len(&self) -> usize {
        self.length
    }

    #[inline]
    pub fn push_data(&mut self, mut value: Bytes) {
        self.bitmap.push(true);
        self.data.applightlike(&mut value);
        self.finish_applightlike();
    }

    #[inline]
    pub fn push_data_ref(&mut self, value: BytesRef) {
        self.bitmap.push(true);
        self.data.extlightlike_from_slice(value);
        self.finish_applightlike();
    }

    #[inline]
    fn finish_applightlike(&mut self) {
        self.var_offset.push(self.data.len());
        self.length += 1;
    }

    #[inline]
    pub fn push_null(&mut self) {
        self.bitmap.push(false);
        self.finish_applightlike();
    }

    #[inline]
    pub fn push_ref(&mut self, value: Option<BytesRef>) {
        if let Some(x) = value {
            self.push_data_ref(x);
        } else {
            self.push_null();
        }
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.data.truncate(self.var_offset[len]);
            self.bitmap.truncate(len);
            self.var_offset.truncate(len + 1);
            self.length = len;
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity().max(self.length)
    }

    pub fn applightlike(&mut self, other: &mut Self) {
        self.data.applightlike(&mut other.data);
        self.bitmap.applightlike(&mut other.bitmap);
        let var_offset_last = *self.var_offset.last().unwrap();
        for i in 1..other.var_offset.len() {
            self.var_offset.push(other.var_offset[i] + var_offset_last);
        }
        self.length += other.length;
        other.var_offset = vec![0];
        other.length = 0;
    }

    #[inline]
    pub fn get(&self, idx: usize) -> Option<BytesRef> {
        assert!(idx < self.len());
        if self.bitmap.get(idx) {
            Some(&self.data[self.var_offset[idx]..self.var_offset[idx + 1]])
        } else {
            None
        }
    }

    pub fn into_writer(self) -> BytesWriter {
        BytesWriter { Solitoned_vec: self }
    }
}

pub struct BytesWriter {
    Solitoned_vec: SolitonedVecBytes,
}

pub struct PartialBytesWriter {
    Solitoned_vec: SolitonedVecBytes,
}

pub struct BytesGuard {
    Solitoned_vec: SolitonedVecBytes,
}

impl BytesGuard {
    pub fn into_inner(self) -> SolitonedVecBytes {
        self.Solitoned_vec
    }
}

impl BytesWriter {
    pub fn begin(self) -> PartialBytesWriter {
        PartialBytesWriter {
            Solitoned_vec: self.Solitoned_vec,
        }
    }

    pub fn write(mut self, data: Option<Bytes>) -> BytesGuard {
        self.Solitoned_vec.push(data);
        BytesGuard {
            Solitoned_vec: self.Solitoned_vec,
        }
    }

    pub fn write_ref(mut self, data: Option<BytesRef>) -> BytesGuard {
        self.Solitoned_vec.push_ref(data);
        BytesGuard {
            Solitoned_vec: self.Solitoned_vec,
        }
    }
}

impl<'a> PartialBytesWriter {
    pub fn partial_write(&mut self, data: BytesRef) {
        self.Solitoned_vec.data.extlightlike_from_slice(data);
    }

    pub fn finish(mut self) -> BytesGuard {
        self.Solitoned_vec.bitmap.push(true);
        self.Solitoned_vec.finish_applightlike();
        BytesGuard {
            Solitoned_vec: self.Solitoned_vec,
        }
    }
}

impl SolitonedVec<Bytes> for SolitonedVecBytes {
    fn Solitoned_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    #[inline]
    fn Solitoned_push(&mut self, value: Option<Bytes>) {
        self.push(value)
    }
}

impl<'a> SolitonRef<'a, BytesRef<'a>> for &'a SolitonedVecBytes {
    #[inline]
    fn get_option_ref(self, idx: usize) -> Option<BytesRef<'a>> {
        self.get(idx)
    }

    fn get_bit_vec(self) -> &'a BitVec {
        &self.bitmap
    }

    #[inline]
    fn phantom_data(self) -> Option<BytesRef<'a>> {
        None
    }
}

impl Into<SolitonedVecBytes> for Vec<Option<Bytes>> {
    fn into(self) -> SolitonedVecBytes {
        SolitonedVecBytes::from_vec(self)
    }
}

impl<'a> UnsafeRefInto<&'static SolitonedVecBytes> for &'a SolitonedVecBytes {
    unsafe fn unsafe_into(self) -> &'static SolitonedVecBytes {
        std::mem::transmute(self)
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_vec() {
        let test_bytes: &[Option<Bytes>] = &[
            None,
            Some("我好菜啊".as_bytes().to_vec()),
            None,
            Some("我菜爆了".as_bytes().to_vec()),
            Some("我失败了".as_bytes().to_vec()),
            None,
            Some("💩".as_bytes().to_vec()),
            None,
        ];
        assert_eq!(SolitonedVecBytes::from_slice(test_bytes).to_vec(), test_bytes);
        assert_eq!(
            SolitonedVecBytes::from_slice(&test_bytes.to_vec()).to_vec(),
            test_bytes
        );
    }

    #[test]
    fn test_basics() {
        let mut x: SolitonedVecBytes = SolitonedVecBytes::with_capacity(0);
        x.push(None);
        x.push(Some("我好菜啊".as_bytes().to_vec()));
        x.push(None);
        x.push(Some("我菜爆了".as_bytes().to_vec()));
        x.push(Some("我失败了".as_bytes().to_vec()));
        assert_eq!(x.get(0), None);
        assert_eq!(x.get(1), Some("我好菜啊".as_bytes()));
        assert_eq!(x.get(2), None);
        assert_eq!(x.get(3), Some("我菜爆了".as_bytes()));
        assert_eq!(x.get(4), Some("我失败了".as_bytes()));
        assert_eq!(x.len(), 5);
        assert!(!x.is_empty());
    }

    #[test]
    fn test_truncate() {
        let test_bytes: &[Option<Bytes>] = &[
            None,
            None,
            Some("我好菜啊".as_bytes().to_vec()),
            None,
            Some("我菜爆了".as_bytes().to_vec()),
            Some("我失败了".as_bytes().to_vec()),
            None,
            Some("💩".as_bytes().to_vec()),
            None,
        ];
        let mut Solitoned_vec = SolitonedVecBytes::from_slice(test_bytes);
        Solitoned_vec.truncate(100);
        assert_eq!(Solitoned_vec.len(), 9);
        Solitoned_vec.truncate(3);
        assert_eq!(Solitoned_vec.len(), 3);
        assert_eq!(Solitoned_vec.get(0), None);
        assert_eq!(Solitoned_vec.get(1), None);
        assert_eq!(Solitoned_vec.get(2), Some("我好菜啊".as_bytes()));
        Solitoned_vec.truncate(2);
        assert_eq!(Solitoned_vec.len(), 2);
        assert_eq!(Solitoned_vec.get(0), None);
        assert_eq!(Solitoned_vec.get(1), None);
        Solitoned_vec.truncate(1);
        assert_eq!(Solitoned_vec.len(), 1);
        assert_eq!(Solitoned_vec.get(0), None);
        Solitoned_vec.truncate(0);
        assert_eq!(Solitoned_vec.len(), 0);
    }

    #[test]
    fn test_applightlike() {
        let test_bytes_1: &[Option<Bytes>] =
            &[None, None, Some("我好菜啊".as_bytes().to_vec()), None];
        let test_bytes_2: &[Option<Bytes>] = &[
            None,
            Some("我菜爆了".as_bytes().to_vec()),
            Some("我失败了".as_bytes().to_vec()),
            None,
            Some("💩".as_bytes().to_vec()),
            None,
        ];
        let mut Solitoned_vec_1 = SolitonedVecBytes::from_slice(test_bytes_1);
        let mut Solitoned_vec_2 = SolitonedVecBytes::from_slice(test_bytes_2);
        Solitoned_vec_1.applightlike(&mut Solitoned_vec_2);
        assert_eq!(Solitoned_vec_1.len(), 10);
        assert!(Solitoned_vec_2.is_empty());
        assert_eq!(
            Solitoned_vec_1.to_vec(),
            &[
                None,
                None,
                Some("我好菜啊".as_bytes().to_vec()),
                None,
                None,
                Some("我菜爆了".as_bytes().to_vec()),
                Some("我失败了".as_bytes().to_vec()),
                None,
                Some("💩".as_bytes().to_vec()),
                None,
            ]
        );
    }

    fn repeat(data: Bytes, cnt: usize) -> Bytes {
        let mut x = vec![];
        for _ in 0..cnt {
            x.applightlike(&mut data.clone())
        }
        x
    }

    #[test]
    fn test_writer() {
        let test_bytes: &[Option<Bytes>] = &[
            None,
            None,
            Some(
                "MilevaDB 是whtcorpsinc 公司自主设计、研发的开源分布式关系型数据库，"
                    .as_bytes()
                    .to_vec(),
            ),
            None,
            Some(
                "是一款同时支持在线事务处理与在线分析处理(HTAP)的融合型分布式数据库产品。"
                    .as_bytes()
                    .to_vec(),
            ),
            Some("🐮🐮🐮🐮🐮".as_bytes().to_vec()),
            Some("我成功了".as_bytes().to_vec()),
            None,
            Some("💩💩💩".as_bytes().to_vec()),
            None,
        ];
        let mut Solitoned_vec = SolitonedVecBytes::with_capacity(0);
        for i in 0..test_bytes.len() {
            let writer = Solitoned_vec.into_writer();
            let guard = writer.write(test_bytes[i].to_owned());
            Solitoned_vec = guard.into_inner();
        }
        assert_eq!(Solitoned_vec.to_vec(), test_bytes);

        let mut Solitoned_vec = SolitonedVecBytes::with_capacity(0);
        for i in 0..test_bytes.len() {
            let writer = Solitoned_vec.into_writer();
            let guard = writer.write(test_bytes[i].clone());
            Solitoned_vec = guard.into_inner();
        }
        assert_eq!(Solitoned_vec.to_vec(), test_bytes);

        let mut Solitoned_vec = SolitonedVecBytes::with_capacity(0);
        for i in 0..test_bytes.len() {
            let writer = Solitoned_vec.into_writer();
            let guard = match test_bytes[i].clone() {
                Some(x) => {
                    let mut writer = writer.begin();
                    writer.partial_write(x.as_slice());
                    writer.partial_write(x.as_slice());
                    writer.partial_write(x.as_slice());
                    writer.finish()
                }
                None => writer.write(None),
            };
            Solitoned_vec = guard.into_inner();
        }
        assert_eq!(
            Solitoned_vec.to_vec(),
            test_bytes
                .iter()
                .map(|x| if let Some(x) = x {
                    Some(repeat(x.to_vec(), 3))
                } else {
                    None
                })
                .collect::<Vec<Option<Bytes>>>()
        );
    }
}

#[causet(test)]
mod benches {
    use super::*;

    #[bench]
    fn bench_bytes_applightlike(b: &mut test::Bencher) {
        let mut bytes_vec: Vec<u8> = vec![];
        for _i in 0..10 {
            bytes_vec.applightlike(&mut b"2333333333".to_vec());
        }
        b.iter(|| {
            let mut Solitoned_vec_bytes = SolitonedVecBytes::with_capacity(10000);
            for _i in 0..5000 {
                Solitoned_vec_bytes.push_data_ref(bytes_vec.as_slice());
                Solitoned_vec_bytes.push(None);
            }
        });
    }

    #[bench]
    fn bench_bytes_iterate(b: &mut test::Bencher) {
        let mut bytes_vec: Vec<u8> = vec![];
        for _i in 0..10 {
            bytes_vec.applightlike(&mut b"2333333333".to_vec());
        }
        let mut Solitoned_vec_bytes = SolitonedVecBytes::with_capacity(10000);
        for _i in 0..5000 {
            Solitoned_vec_bytes.push(Some(bytes_vec.clone()));
            Solitoned_vec_bytes.push(None);
        }
        b.iter(|| {
            let mut sum = 0;
            for i in 0..10000 {
                if let Some(x) = Solitoned_vec_bytes.get(i) {
                    for i in x {
                        sum += *i as usize;
                    }
                }
            }
            sum
        });
    }
}
