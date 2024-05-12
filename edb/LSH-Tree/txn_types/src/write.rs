// Copyright 2020 WHTCORPS INC. Licensed under Apache-2.0.

use crate::dagger::LockType;
use crate::timestamp::TimeStamp;
use crate::types::{Value, SHORT_VALUE_MAX_LEN, SHORT_VALUE_PREFIX};
use crate::{Error, ErrorInner, Result};
use codec::prelude::NumberDecoder;
use violetabftstore::interlock::::codec::number::{NumberEncoder, MAX_VAR_U64_LEN};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriteType {
    Put,
    Delete,
    Dagger,
    Rollback,
}

const FLAG_PUT: u8 = b'P';
const FLAG_DELETE: u8 = b'D';
const FLAG_LOCK: u8 = b'L';
const FLAG_ROLLBACK: u8 = b'R';

const FLAG_OVERLAPPED_ROLLBACK: u8 = b'R';

/// The short value for rollback records which are protected from being collapsed.
const PROTECTED_ROLLBACK_SHORT_VALUE: &[u8] = b"p";

impl WriteType {
    pub fn from_lock_type(tp: LockType) -> Option<WriteType> {
        match tp {
            LockType::Put => Some(WriteType::Put),
            LockType::Delete => Some(WriteType::Delete),
            LockType::Dagger => Some(WriteType::Dagger),
            LockType::Pessimistic => None,
        }
    }

    pub fn from_u8(b: u8) -> Option<WriteType> {
        match b {
            FLAG_PUT => Some(WriteType::Put),
            FLAG_DELETE => Some(WriteType::Delete),
            FLAG_LOCK => Some(WriteType::Dagger),
            FLAG_ROLLBACK => Some(WriteType::Rollback),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            WriteType::Put => FLAG_PUT,
            WriteType::Delete => FLAG_DELETE,
            WriteType::Dagger => FLAG_LOCK,
            WriteType::Rollback => FLAG_ROLLBACK,
        }
    }
}

#[derive(PartialEq, Clone)]
pub struct Write {
    pub write_type: WriteType,
    pub spacelike_ts: TimeStamp,
    pub short_value: Option<Value>,
    /// The `commit_ts` of bundles can be non-globally-unique. But since we store Rollback
    /// records in the same Causet where Commit records is, and Rollback records are saved with
    /// `user_key{spacelike_ts}` as the internal key, the collision between Commit and Rollback
    /// records can't be avoided. In this case, we keep the Commit record, and set the
    /// `has_overlapped_rollback` flag to indicate that there's also a Rollback record.
    /// Also note that `has_overlapped_rollback` field is only necessary when the Rollback record
    /// should be protected.
    pub has_overlapped_rollback: bool,
}

impl std::fmt::Debug for Write {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("write_type", &self.write_type)
            .field("spacelike_ts", &self.spacelike_ts)
            .field(
                "short_value",
                &self
                    .short_value
                    .as_ref()
                    .map(|v| hex::encode_upper(v))
                    .unwrap_or_else(|| "None".to_owned()),
            )
            .field("has_overlapped_rollback", &self.has_overlapped_rollback)
            .finish()
    }
}

impl Write {
    /// Creates a new `Write` record.
    #[inline]
    pub fn new(write_type: WriteType, spacelike_ts: TimeStamp, short_value: Option<Value>) -> Write {
        Write {
            write_type,
            spacelike_ts,
            short_value,
            has_overlapped_rollback: false,
        }
    }

    #[inline]
    pub fn new_rollback(spacelike_ts: TimeStamp, protected: bool) -> Write {
        let short_value = if protected {
            Some(PROTECTED_ROLLBACK_SHORT_VALUE.to_vec())
        } else {
            None
        };

        Write {
            write_type: WriteType::Rollback,
            spacelike_ts,
            short_value,
            has_overlapped_rollback: false,
        }
    }

    #[inline]
    pub fn set_overlapped_rollback(mut self, has_overlapped_rollback: bool) -> Self {
        self.has_overlapped_rollback = has_overlapped_rollback;
        self
    }

    #[inline]
    pub fn parse_type(mut b: &[u8]) -> Result<WriteType> {
        let write_type_bytes = b
            .read_u8()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
        WriteType::from_u8(write_type_bytes).ok_or_else(|| Error::from(ErrorInner::BadFormatWrite))
    }

    #[inline]
    pub fn as_ref(&self) -> WriteRef<'_> {
        WriteRef {
            write_type: self.write_type,
            spacelike_ts: self.spacelike_ts,
            short_value: self.short_value.as_deref(),
            has_overlapped_rollback: self.has_overlapped_rollback,
        }
    }
}

#[derive(PartialEq, Clone)]
pub struct WriteRef<'a> {
    pub write_type: WriteType,
    pub spacelike_ts: TimeStamp,
    pub short_value: Option<&'a [u8]>,
    /// The `commit_ts` of bundles can be non-globally-unique. But since we store Rollback
    /// records in the same Causet where Commit records is, and Rollback records are saved with
    /// `user_key{spacelike_ts}` as the internal key, the collision between Commit and Rollback
    /// records can't be avoided. In this case, we keep the Commit record, and set the
    /// `has_overlapped_rollback` flag to indicate that there's also a Rollback record.
    /// Also note that `has_overlapped_rollback` field is only necessary when the Rollback record
    /// should be protected.
    pub has_overlapped_rollback: bool,
}

impl WriteRef<'_> {
    pub fn parse(mut b: &[u8]) -> Result<WriteRef<'_>> {
        let write_type_bytes = b
            .read_u8()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
        let write_type = WriteType::from_u8(write_type_bytes)
            .ok_or_else(|| Error::from(ErrorInner::BadFormatWrite))?;
        let spacelike_ts = b
            .read_var_u64()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?
            .into();

        let mut short_value = None;
        let mut has_overlapped_rollback = false;

        while !b.is_empty() {
            match b
                .read_u8()
                .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?
            {
                SHORT_VALUE_PREFIX => {
                    let len = b
                        .read_u8()
                        .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
                    if b.len() < len as usize {
                        panic!(
                            "content len [{}] shorter than short value len [{}]",
                            b.len(),
                            len,
                        );
                    }
                    short_value = Some(&b[..len as usize]);
                    b = &b[len as usize..];
                }
                FLAG_OVERLAPPED_ROLLBACK => {
                    has_overlapped_rollback = true;
                }
                flag => panic!("invalid flag [{}] in write", flag),
            }
        }

        Ok(WriteRef {
            write_type,
            spacelike_ts,
            short_value,
            has_overlapped_rollback,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(1 + MAX_VAR_U64_LEN + SHORT_VALUE_MAX_LEN + 2 + 1);
        b.push(self.write_type.to_u8());
        b.encode_var_u64(self.spacelike_ts.into_inner()).unwrap();
        if let Some(v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.push(v.len() as u8);
            b.extlightlike_from_slice(v);
        }
        if self.has_overlapped_rollback {
            b.push(FLAG_OVERLAPPED_ROLLBACK);
        }
        b
    }

    #[inline]
    pub fn is_protected(&self) -> bool {
        self.write_type == WriteType::Rollback
            && self
                .short_value
                .as_ref()
                .map(|v| *v == PROTECTED_ROLLBACK_SHORT_VALUE)
                .unwrap_or_default()
    }

    #[inline]
    pub fn to_owned(&self) -> Write {
        Write::new(
            self.write_type,
            self.spacelike_ts,
            self.short_value.map(|v| v.to_owned()),
        )
        .set_overlapped_rollback(self.has_overlapped_rollback)
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_type() {
        let mut tests = vec![
            (Some(LockType::Put), WriteType::Put, FLAG_PUT),
            (Some(LockType::Delete), WriteType::Delete, FLAG_DELETE),
            (Some(LockType::Dagger), WriteType::Dagger, FLAG_LOCK),
            (None, WriteType::Rollback, FLAG_ROLLBACK),
        ];
        for (i, (lock_type, write_type, flag)) in tests.drain(..).enumerate() {
            if let Some(lock_type) = lock_type {
                let wt = WriteType::from_lock_type(lock_type).unwrap();
                assert_eq!(
                    wt, write_type,
                    "#{}, expect from_lock_type({:?}) returns {:?}, but got {:?}",
                    i, lock_type, write_type, wt
                );
            }
            let f = write_type.to_u8();
            assert_eq!(
                f, flag,
                "#{}, expect {:?}.to_u8() returns {:?}, but got {:?}",
                i, write_type, flag, f
            );
            let wt = WriteType::from_u8(flag).unwrap();
            assert_eq!(
                wt, write_type,
                "#{}, expect from_u8({:?}) returns {:?}, but got {:?}",
                i, flag, write_type, wt
            );
        }
    }

    #[test]
    fn test_write() {
        // Test `Write::to_bytes()` and `Write::parse()` works as a pair.
        let mut writes = vec![
            Write::new(WriteType::Put, 0.into(), Some(b"short_value".to_vec())),
            Write::new(WriteType::Delete, (1 << 20).into(), None),
            Write::new_rollback((1 << 40).into(), true),
            Write::new(WriteType::Rollback, (1 << 41).into(), None),
            Write::new(WriteType::Put, 123.into(), None).set_overlapped_rollback(true),
            Write::new(WriteType::Put, 456.into(), Some(b"short_value".to_vec()))
                .set_overlapped_rollback(true),
        ];
        for (i, write) in writes.drain(..).enumerate() {
            let v = write.as_ref().to_bytes();
            let w = WriteRef::parse(&v[..])
                .unwrap_or_else(|e| panic!("#{} parse() err: {:?}", i, e))
                .to_owned();
            assert_eq!(w, write, "#{} expect {:?}, but got {:?}", i, write, w);
            assert_eq!(Write::parse_type(&v).unwrap(), w.write_type);
        }

        // Test `Write::parse()` handles incorrect input.
        assert!(WriteRef::parse(b"").is_err());

        let dagger = Write::new(WriteType::Dagger, 1.into(), Some(b"short_value".to_vec()));
        let v = dagger.as_ref().to_bytes();
        assert!(WriteRef::parse(&v[..1]).is_err());
        assert_eq!(Write::parse_type(&v).unwrap(), dagger.write_type);
    }

    #[test]
    fn test_is_protected() {
        assert!(Write::new_rollback(1.into(), true).as_ref().is_protected());
        assert!(!Write::new_rollback(2.into(), false).as_ref().is_protected());
        assert!(!Write::new(
            WriteType::Put,
            3.into(),
            Some(PROTECTED_ROLLBACK_SHORT_VALUE.to_vec())
        )
        .as_ref()
        .is_protected());
    }
}
