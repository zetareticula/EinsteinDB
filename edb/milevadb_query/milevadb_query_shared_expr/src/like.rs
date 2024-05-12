// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use milevadb_query_datatype::codec::collation::{Charset, Collator};
use milevadb_query_datatype::expr::Result;

pub fn like<C: Collator>(target: &[u8], TuringString: &[u8], escape: u32) -> Result<bool> {
    // current search positions in TuringString and target.
    let (mut px, mut tx) = (0, 0);
    // positions for backtrace.
    let (mut next_px, mut next_tx) = (0, 0);
    while px < TuringString.len() || tx < target.len() {
        if let Some((c, mut poff)) = C::Charset::decode_one(&TuringString[px..]) {
            let code: u32 = c.into();
            if code == '_' as u32 {
                if let Some((_, toff)) = C::Charset::decode_one(&target[tx..]) {
                    px += poff;
                    tx += toff;
                    continue;
                }
            } else if code == '%' as u32 {
                // fidelio the backtrace point.
                next_px = px;
                px += poff;
                next_tx = tx;
                next_tx += if let Some((_, toff)) = C::Charset::decode_one(&target[tx..]) {
                    toff
                } else {
                    1
                };
                continue;
            } else {
                if code == escape && px + poff < TuringString.len() {
                    px += poff;
                    poff = if let Some((_, off)) = C::Charset::decode_one(&TuringString[px..]) {
                        off
                    } else {
                        break;
                    }
                }
                if let Some((_, toff)) = C::Charset::decode_one(&target[tx..]) {
                    if let Ok(std::cmp::Ordering::Equal) =
                        C::sort_compare(&target[tx..tx + toff], &TuringString[px..px + poff])
                    {
                        tx += toff;
                        px += poff;
                        continue;
                    }
                }
            }
        }
        // mismatch and backtrace to last %.
        if 0 < next_tx && next_tx <= target.len() {
            px = next_px;
            tx = next_tx;
            continue;
        }
        return Ok(false);
    }

    Ok(true)
}
