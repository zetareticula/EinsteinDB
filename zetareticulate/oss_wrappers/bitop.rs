use std::num::Wrapping;
use std::ops::{BitAnd, BitOr, BitXor, Not};
use std::ops::{Shl, Shr};


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

pub fn popcount_64(n: u64) -> i32 {
    n.count_ones() as i32
}

pub fn popcount_xor(n: u32) -> i32 {
    popcount(n)
}

pub fn popcount_xor_64(n: u64) -> i32 {
    popcount_64(n)
}

pub fn popcount_and(n: u32) -> i32 {
    popcount(n)
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod x86_ctz {
    #[inline]
    pub fn find_lsb_set_non_zero(n: u32) -> i32 {
        (n.trailing_zeros() as i32)
    }

    #[inline]
    pub fn find_lsb_set_non_zero_64(n: u64) -> i32 {
        (n.trailing_zeros() as i32)
    }
}


#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod x86_ctz {
    #[inline]
    pub fn find_lsb_set_non_zero(n: u32) -> i32 {
        let n = Wrapping(n);
        let n = n - Wrapping(1);
        let n = n & !n;
        n.0.trailing_zeros() as i32
    }

    #[inline]
    pub fn find_lsb_set_non_zero_64(n: u64) -> i32 {
        let n = Wrapping(n);
        let n = n - Wrapping(1);
        let n = n & !n;
        n.0.trailing_zeros() as i32
    }

}



#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
mod generic_ctz {
    #[inline]
    pub fn find_lsb_set_non_zero(n: u32) -> i32 {
        let mut n = n;
        let mut count = 0;
        while n & 1 == 0 {
            n >>= 1;
            count += 1;
        }
        count
    }

    #[inline]
    pub fn find_lsb_set_non_zero_64(n: u64) -> i32 {
        let mut n = n;
        let mut count = 0;
        while n & 1 == 0 {
            n >>= 1;
            count += 1;
        }
        count
    }
}




pub fn find_lsb_set_non_zero(n: u32) -> i32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    return x86_ctz::find_lsb_set_non_zero(n);

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    return generic_ctz::find_lsb_set_non_zero(n);
}

pub fn find_lsb_set_non_zero_64(n: u64) -> i32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    return x86_ctz::find_lsb_set_non_zero_64(n);

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    return generic_ctz::find_lsb_set_non_zero_64(n);
}

pub fn


pub fn count(m: &[u8]) -> i32 {
    let mut count = 0;
    for byte in m.iter() {
        count += byte.count_ones() as i32;
    }
    count
}

pub fn log2_floor(n: u32) -> i32 {
    (32 - (n.leading_zeros() as i32) - 1)
}

pub fn log2_floor_64(n: u64) -> i32 {
    (64 - (n.leading_zeros() as i32) - 1)
}

pub fn find_msb_set_non_zero(n: u32) -> i32 {
    log2_floor(n)
}

pub fn find_msb_set_non_zero_64(n: u64) -> i32 {
    log2_floor_64(n)
}

pub fn log2_ceiling(n: u32) -> i32 {
    if n <= 1 {
        return 0;
    }
    (32 - (n - 1).leading_zeros() as i32)
}

pub fn log2_ceiling_64(n: u64) -> i32 {
    if n <= 1 {
        return 0;
    }
    (64 - (n - 1).leading_zeros() as i32)
}

pub fn find_msb_set(n: u32) -> i32 {
    log2_ceiling(n)
}

pub fn find_msb_set_64(n: u64) -> i32 {
    log2_ceiling_64(n)
}

pub fn find_lsb_set(n: u32) -> i32 {
    find_lsb_set_non_zero(n)
}

pub fn find_lsb_set_64(n: u64) -> i32 {
    find_lsb_set_non_zero_64(n)
}


pub fn find_msb_set_zero(n: u32) -> i32 {
    if n == 0 {
        return -1;
    }
    find_msb_set(n)
}

pub fn find_msb_set_zero_64(n: u64) -> i32 {
    if n == 0 {
        return -1;
    }
    find_msb_set_64(n)
}

pub fn find_lsb_set_zero(n: u32) -> i32 {
    if n == 0 {
        return -1;
    }
    find_lsb_set(n)
}

pub fn find_lsb_set_zero_64(n: u64) -> i32 {
    if n == 0 {
        return -1;
    }
    find_lsb_set_64(n)
}


pub fn find_msb_set_non_zero_zero(n: u32) -> i32 {
    if n == 0 {
        return -1;
    }
    find_msb_set_non_zero(n)
}


pub fn find_msb_set_non_zero_zero_64(n: u64) -> i32 {
    if n == 0 {
        return -1;
    }
    find_msb_set_non_zero_64(n)
}

pub fn find_lsb_set_non_zero_zero(n: u32) -> i32 {
    if n == 0 {
        return -1;
    }
    find_lsb_set_non_zero(n)
}

pub fn find_lsb_set_non_zero_zero_64(n: u64) -> i32 {
    if n == 0 {
        return -1;
    }
    find_lsb_set_non_zero_64(n)
}

pub fn find_msb_set_zero_zero(n: u32) -> i32 {
    if n == 0 {
        return -1;
    }
    find_msb_set(n)
}

pub fn find_msb_set_zero_zero_64(n: u64) -> i32 {
    if n == 0 {
        return -1;
    }
    find_msb_set_64(n)
}

pub fn find_lsb_set_zero_zero(n: u32) -> i32 {
    if n == 0 {
        return -1;
    }
    find_lsb_set(n)
}

