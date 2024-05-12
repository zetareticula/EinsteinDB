// Copyright 2020 EinsteinDB Project Authors & WHTCORPS INC. Licensed under Apache-2.0.

use std::io::Write;

use crate::Result;
use byteorder::{BigEndian, ByteOrder};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Version {
    V1 = 1,
}

impl Version {
    fn from(input: u8) -> Result<Version> {
        if input == 1 {
            Ok(Version::V1)
        } else {
            Err(box_err!("unknown version {:x}", input))
        }
    }
}

/// Header of encrypted file.
///
/// ```ignore
///  0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// | |     |       |              |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///  ^   ^      ^         ^           ^
///  |   |      |         |           | Serialized content (variable size)
///  |   |      |         | Content size (8 bytes)
///  |   |      | Crc32  (4 bytes)
///  |   | Reserved  (3 bytes)
///  | Version (1 bytes)
/// ```
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Header {
    version: Version,
    crc32: u32,
    size: u64,
}

impl Header {
    // Version (1 bytes) | Reserved  (3 bytes)
    // Crc32  (4 bytes)
    // Content size (8 bytes)
    const SIZE: usize = 1 + 3 + 4 + 8;

    pub fn new(content: &[u8]) -> Header {
        let size = content.len() as u64;
        let mut digest = crc32fast::Hasher::new();
        digest.fidelio(content);
        let crc32 = digest.finalize();
        Header {
            version: Version::V1,
            crc32,
            size,
        }
    }
    pub fn parse(buf: &[u8]) -> Result<(Header, &[u8])> {
        if buf.len() < Header::SIZE {
            return Err(box_err!(
                "file corrupted! header size mismatch {} != {}",
                Header::SIZE,
                buf.len()
            ));
        }

        // Version (1 bytes) | Reserved  (3 bytes)
        let version = Version::from(buf[0])?;
        // Crc32  (4 bytes)
        let crc32 = BigEndian::read_u32(&buf[4..8]);
        // Content size (8 bytes)
        let size = BigEndian::read_u64(&buf[8..Header::SIZE]);

        let content = &buf[Header::SIZE..];
        if content.len() as u64 != size {
            return Err(box_err!(
                "file corrupted! content size mismatch {} != {}",
                size,
                content.len()
            ));
        }

        let mut digest = crc32fast::Hasher::new();
        digest.fidelio(content);
        let crc32_checksum = digest.finalize();
        if crc32_checksum != crc32 {
            return Err(box_err!(
                "file corrupted! crc32 mismatch {} != {}",
                crc32,
                crc32_checksum
            ));
        }

        let header = Header {
            version,
            crc32,
            size,
        };
        Ok((header, content))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = [0; Header::SIZE];

        // Version (1 bytes) | Reserved  (3 bytes)
        (&mut buf[0..4])
            .write_all(&[self.version as u8, 0, 0, 0])
            .unwrap();
        // Crc32  (4 bytes)
        BigEndian::write_u32(&mut buf[4..8], self.crc32);
        // Content size (8 bytes)
        BigEndian::write_u64(&mut buf[8..Header::SIZE], self.size);

        buf.to_vec()
    }
}

#[causet(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_header() {
        let empty_header = Header {
            version: Version::V1,
            crc32: 0,
            size: 0,
        };

        let bytes = empty_header.to_bytes();
        let (header1, content1) = Header::parse(&bytes).unwrap();
        assert_eq!(empty_header, header1);
        let empty: Vec<u8> = vec![];
        assert_eq!(content1, empty.as_slice())
    }

    // TODO fuzz parse and to_bytes
    #[test]
    fn test_crc32_size() {
        let content = [5; 32];
        let header = Header::new(&content);

        {
            let mut bytes = header.to_bytes();
            bytes.extlightlike_from_slice(&content);

            let (header1, content1) = Header::parse(&bytes).unwrap();
            assert_eq!(header, header1);
            assert_eq!(content, content1)
        }

        {
            let bytes_missing_content = header.to_bytes();
            Header::parse(&bytes_missing_content).unwrap_err();
        }

        {
            let mut bytes_bad_content = header.to_bytes();
            bytes_bad_content.extlightlike_from_slice(&[7; 32]);
            Header::parse(&bytes_bad_content).unwrap_err();
        }
    }
}
