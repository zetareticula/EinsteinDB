// Copyright 2024 The Zeta Reticula Research Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::{Shl, Shr};
use std::mem;
use std::convert::TryInto;
use std::f32;
use std::f64;


pub fn set_bit(n: u32, bit: u32) -> u32 {
    n | (1 << bit)
}

pub fn set_bit_64(n: u64, bit: u32) -> u64 {
    n | (1 << bit)
}

pub fn clear_bit(n: u32, bit: u32) -> u32 {
    n & !(1 << bit)
}

pub fn clear_bit_64(n: u64, bit: u32) -> u64 {
    n & !(1 << bit)
}

pub fn toggle_bit(n: u32, bit: u32) -> u32 {
    n ^ (1 << bit)
}

pub fn toggle_bit_64(n: u64, bit: u32) -> u64 {
    n ^ (1 << bit)
}

pub fn is_bit_set(n: u32, bit: u32) -> bool {
    n & (1 << bit) != 0
}

pub fn popcount(n: u32) -> i32 {
    n.count_ones() as i32
}


mod strings {
    use std::convert::TryInto;

    fn uint_from_ieee754<T: Copy + Into<u64>>(f: f32) -> T {
        let n: u64 = unsafe { mem::transmute(f) };
        let sign_bit: T = !(std::u64::MAX >> 1).try_into().unwrap();
        if (n & sign_bit.into()) == 0 {
            (n + sign_bit.into()).into()
        } else {
            (0 - n).into()
        }
    }

    fn ieee754_from_uint<T: Copy + Into<u64>>(n: u64) -> f32 {
        let sign_bit: T = !(std::u64::MAX >> 1).try_into().unwrap();
        let n = if n & sign_bit.into() != 0 { n - sign_bit.into() } else { 0 - n };
        unsafe { mem::transmute(n) }
    }

    fn key_from_uint32(u32: u32) -> Vec<u8> {
        let norder = u32.to_be();
        norder.to_be_bytes().to_vec()
    }

    fn key_from_uint64(u64: u64) -> Vec<u8> {
        let norder = u64.to_be();
        norder.to_be_bytes().to_vec()
    }

    fn key_to_uint32(key: &[u8]) -> u32 {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&key[..4]);
        u32::from_be_bytes(bytes)
    }

    fn key_to_uint64(key: &[u8]) -> u64 {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&key[..8]);
        u64::from_be_bytes(bytes)
    }

    fn key_to_uint64(key: &[u8]) -> u64 {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&key[..8]);
        u64::from_be_bytes(bytes)
    }

    fn key_from_float(x: f32) -> Vec<u8> {
        let n = uint_from_ieee754::<u32>(x);
        key_from_uint32(n)
    }

    fn float_to_key(x: f32) -> Vec<u8> {
        key_from_float(x)
    }

    fn key_to_float(key: &[u8]) -> f32 {
        let n = key_to_uint32(key);
        ieee754_from_uint::<u32>(n)
    }

    pub fn int32_to_key(i32: i32) -> Vec<u8> {
        uint32_to_key(i32 as u32)
    }

    pub fn uint32_to_key(u32: u32) -> Vec<u8> {
        key_from_uint32(u32)
    }

    pub fn uint64_to_key(u64: u64) -> Vec<u8> {
        key_from_uint64(u64)
    }

    pub fn key_to_int32(key: &[u8]) -> i32 {
        key_to_uint32(key) as i32
    }
}

fn main() {
    let i32_key = strings::int32_to_key(42);
    let u32_key = strings::uint32_to_key(42);
    let u64_key = strings::uint64_to_key(42);
    let float_key = strings::float_to_key(42.0);

    println!("i32 key: {:?}", i32_key);
    println!("u32 key: {:?}", u32_key);
    println!("u64 key: {:?}", u64_key);
    println!("float key: {:?}", float_key);

    let i32_value = strings::key_to_int32(&i32_key);
    let u32_value = strings::key_to_uint32(&u32_key);
    let u64_value = strings::key_to_uint64(&u64_key);
    let float_value = strings::key_to_float(&float_key);

    println!("i32 value: {:?}", i32_value);
    println!("u32 value: {:?}", u32_value);
    println!("u64 value: {:?}", u64_value);
    println!("float value: {:?}", float_value);
}


