// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use milevadb_query_common::Result;
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::expr::EvalContext;

/// A trait for all summable types.
///
/// This trait is used to implement `AVG()` and `SUM()` by using generics.
pub trait Summable: Evaluable + EvaluableRet {
    /// Returns the zero value.
    fn zero() -> Self;

    /// Adds assign another value.
    fn add_assign(&mut self, ctx: &mut EvalContext, other: &Self) -> Result<()>;
}

impl Summable for Decimal {
    #[inline]
    fn zero() -> Self {
        Decimal::zero()
    }

    #[inline]
    fn add_assign(&mut self, _ctx: &mut EvalContext, other: &Self) -> Result<()> {
        // TODO: If there is truncate error, should it be a warning instead?
        let r: milevadb_query_datatype::codec::Result<Decimal> = (self as &Self + other).into();
        *self = r?;
        Ok(())
    }
}

impl Summable for Real {
    #[inline]
    fn zero() -> Self {
        Real::new(0.0).unwrap()
    }

    #[inline]
    fn add_assign(&mut self, _ctx: &mut EvalContext, other: &Self) -> Result<()> {
        *self += *other;
        Ok(())
    }
}
