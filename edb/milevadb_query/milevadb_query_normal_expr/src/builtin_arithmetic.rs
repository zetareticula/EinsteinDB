// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::borrow::Cow;
use std::ops::{Add, Mul, Sub};
use std::{f64, i64, u64};

use milevadb_query_datatype::codec::mysql::{Decimal, Res};
use milevadb_query_datatype::codec::{div_i64, div_i64_with_u64, div_u64_with_i64, Datum};

use crate::ScalarFunc;
use milevadb_query_datatype::expr::{Error, EvalContext, Result};

impl ScalarFunc {
    pub fn plus_real(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_real(ctx, Evcausetidx));
        let res = lhs + rhs;
        if !res.is_finite() {
            return Err(Error::overflow("DOUBLE", &format!("({} + {})", lhs, rhs)));
        }
        Ok(Some(res))
    }

    pub fn plus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, Evcausetidx));
        let result: Result<Decimal> = lhs.add(&rhs).into();
        result.map(|t| Some(Cow::Owned(t)))
    }

    pub fn plus_int(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        let lus = self.children[0].is_unsigned();
        let rus = self.children[1].is_unsigned();
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_add(rhs as u64).map(|t| t as i64),
            (true, false) => {
                if rhs >= 0 {
                    (lhs as u64).checked_add(rhs as u64).map(|t| t as i64)
                } else {
                    (lhs as u64)
                        .checked_sub(rhs.overflowing_neg().0 as u64)
                        .map(|t| t as i64)
                }
            }
            (false, true) => {
                if lhs >= 0 {
                    (lhs as u64).checked_add(rhs as u64).map(|t| t as i64)
                } else {
                    (rhs as u64)
                        .checked_sub(lhs.overflowing_neg().0 as u64)
                        .map(|t| t as i64)
                }
            }
            (false, false) => lhs.checked_add(rhs),
        };
        let data_type = if lus | rus {
            "BIGINT UNSIGNED"
        } else {
            "BIGINT"
        };
        res.ok_or_else(|| Error::overflow(data_type, &format!("({} + {})", lhs, rhs)))
            .map(Some)
    }

    pub fn minus_real(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_real(ctx, Evcausetidx));
        let res = lhs - rhs;
        if !res.is_finite() {
            return Err(Error::overflow("DOUBLE", &format!("({} - {})", lhs, rhs)));
        }
        Ok(Some(res))
    }

    pub fn minus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, Evcausetidx));
        let result: Result<Decimal> = lhs.sub(&rhs).into();
        result.map(Cow::Owned).map(Some)
    }

    pub fn minus_int(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        let lus = self.children[0].is_unsigned();
        let rus = self.children[1].is_unsigned();
        let data_type = if lus | rus {
            "BIGINT UNSIGNED"
        } else {
            "BIGINT"
        };
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64),
            (true, false) => {
                if rhs >= 0 {
                    (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64)
                } else {
                    (lhs as u64)
                        .checked_add(rhs.overflowing_neg().0 as u64)
                        .map(|t| t as i64)
                }
            }
            (false, true) => {
                if lhs >= 0 {
                    (lhs as u64).checked_sub(rhs as u64).map(|t| t as i64)
                } else {
                    return Err(Error::overflow(data_type, &format!("({} - {})", lhs, rhs)));
                }
            }
            (false, false) => lhs.checked_sub(rhs),
        };
        res.ok_or_else(|| Error::overflow(data_type, &format!("({} - {})", lhs, rhs)))
            .map(Some)
    }

    pub fn multiply_real(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_real(ctx, Evcausetidx));
        let res = lhs * rhs;
        if !res.is_finite() {
            return Err(Error::overflow("DOUBLE", &format!("({} * {})", lhs, rhs)));
        }
        Ok(Some(res))
    }

    pub fn multiply_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, Evcausetidx));
        let result: Result<Decimal> = lhs.mul(&rhs).into();
        result.map(Cow::Owned).map(Some)
    }

    pub fn multiply_int(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        let lus = self.children[0].is_unsigned();
        let rus = self.children[1].is_unsigned();
        let u64_mul_i64 = |u, s| {
            if s >= 0 {
                (u as u64).checked_mul(s as u64).map(|t| t as i64)
            } else {
                None
            }
        };
        let res = match (lus, rus) {
            (true, true) => (lhs as u64).checked_mul(rhs as u64).map(|t| t as i64),
            (false, false) => lhs.checked_mul(rhs),
            (true, false) => u64_mul_i64(lhs, rhs),
            (false, true) => u64_mul_i64(rhs, lhs),
        };
        res.ok_or_else(|| Error::overflow("BIGINT UNSIGNED", &format!("({} * {})", lhs, rhs)))
            .map(Some)
    }

    pub fn multiply_int_unsigned(
        &self,
        ctx: &mut EvalContext,
        Evcausetidx: &[Datum],
    ) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        let res = (lhs as u64).checked_mul(rhs as u64).map(|t| t as i64);
        // TODO: output expression in error when PrimaryCauset's name pushed down.
        res.ok_or_else(|| Error::overflow("BIGINT UNSIGNED", &format!("({} * {})", lhs, rhs)))
            .map(Some)
    }

    pub fn divide_real(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<f64>> {
        let lhs = try_opt!(self.children[0].eval_real(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_real(ctx, Evcausetidx));
        if rhs == 0f64 {
            return ctx.handle_division_by_zero().map(|()| None);
        }
        let res = lhs / rhs;
        if res.is_infinite() {
            Err(Error::overflow("DOUBLE", &format!("({} / {})", lhs, rhs)))
        } else {
            Ok(Some(res))
        }
    }

    pub fn divide_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, Evcausetidx));
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, Evcausetidx));
        let overflow = Error::overflow("DECIMAL", &format!("({} / {})", lhs, rhs));
        match lhs.as_ref() / rhs.as_ref() {
            Some(v) => match v {
                Res::Ok(v) => Ok(Some(Cow::Owned(v))),
                Res::Truncated(_) => Err(Error::truncated()),
                Res::Overflow(_) => Err(overflow),
            },
            None => ctx.handle_division_by_zero().map(|()| None),
        }
    }

    pub fn int_divide_int(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let rhs = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        if rhs == 0 {
            return Ok(None);
        }
        let rus = self.children[1].is_unsigned();

        let lhs = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        let lus = self.children[0].is_unsigned();

        let res = match (lus, rus) {
            (true, true) => Ok(((lhs as u64) / (rhs as u64)) as i64),
            (false, false) => div_i64(lhs, rhs),
            (false, true) => div_i64_with_u64(lhs, rhs as u64).map(|r| r as i64),
            (true, false) => div_u64_with_i64(lhs as u64, rhs).map(|r| r as i64),
        };
        res.map(Some)
    }

    pub fn int_divide_decimal(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        match self.divide_decimal(ctx, Evcausetidx) {
            Ok(Some(v)) => match v.as_i64() {
                Res::Ok(v_i64) => Ok(Some(v_i64)),
                Res::Truncated(v_i64) => Ok(Some(v_i64)),
                Res::Overflow(_) => Err(Error::overflow("BIGINT", &v.to_string())),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn mod_real(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<f64>> {
        let rhs = try_opt!(self.children[1].eval_real(ctx, Evcausetidx));
        if rhs == 0f64 {
            return Ok(None);
        }
        let lhs = try_opt!(self.children[0].eval_real(ctx, Evcausetidx));

        let res = lhs % rhs;
        Ok(Some(res))
    }

    pub fn mod_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        Evcausetidx: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let rhs = try_opt!(self.children[1].eval_decimal(ctx, Evcausetidx));
        let lhs = try_opt!(self.children[0].eval_decimal(ctx, Evcausetidx));
        let overflow = Error::overflow("DECIMAL", &format!("({} % {})", lhs, rhs));
        match lhs.into_owned() % rhs.into_owned() {
            Some(v) => match v {
                Res::Ok(v) => Ok(Some(Cow::Owned(v))),
                Res::Truncated(_) => Err(Error::truncated()),
                Res::Overflow(_) => Err(overflow),
            },
            None => Ok(None),
        }
    }

    pub fn mod_int(&self, ctx: &mut EvalContext, Evcausetidx: &[Datum]) -> Result<Option<i64>> {
        let rhs = try_opt!(self.children[1].eval_int(ctx, Evcausetidx));
        if rhs == 0 {
            return Ok(None);
        }
        let rus = self.children[1].is_unsigned();

        let lhs = try_opt!(self.children[0].eval_int(ctx, Evcausetidx));
        let lus = self.children[0].is_unsigned();

        let res = match (lus, rus) {
            (true, true) => ((lhs as u64) % (rhs as u64)) as i64,
            (false, false) => lhs % rhs,
            (true, false) => ((lhs as u64) % (rhs.overflowing_abs().0 as u64)) as i64,
            (false, true) => ((lhs.overflowing_abs().0 as u64) % (rhs as u64)) as i64,
        };
        Ok(Some(res))
    }
}

#[causet(test)]
mod tests {
    use std::{f64, i64, u64};

    use milevadb_query_datatype::FieldTypeFlag;
    use fidel_timeshare::ScalarFuncSig;

    use crate::tests::{
        check_divide_by_zero, check_overflow, datum_expr, scalar_func_expr, str2dec,
    };
    use crate::*;
    use milevadb_query_datatype::codec::error::ERR_DIVISION_BY_ZERO;
    use milevadb_query_datatype::codec::mysql::Decimal;
    use milevadb_query_datatype::codec::Datum;
    use milevadb_query_datatype::expr::{EvalConfig, Flag, SqlMode};

    #[test]
    fn test_arithmetic_int() {
        let tests = vec![
            (
                ScalarFuncSig::PlusInt,
                Datum::Null,
                Datum::I64(1),
                Datum::Null,
            ),
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(1),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(12),
                Datum::I64(1),
                Datum::I64(13),
            ),
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(i64::MIN),
                Datum::U64(i64::MAX as u64 + 1),
                Datum::U64(0),
            ),
            (
                ScalarFuncSig::MinusInt,
                Datum::I64(12),
                Datum::I64(1),
                Datum::I64(11),
            ),
            (
                ScalarFuncSig::MinusInt,
                Datum::U64(0),
                Datum::I64(i64::MIN),
                Datum::U64(i64::MAX as u64 + 1),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(12),
                Datum::I64(1),
                Datum::I64(12),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(i64::MIN),
                Datum::I64(1),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(13),
                Datum::I64(11),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-13),
                Datum::I64(11),
                Datum::I64(-1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(13),
                Datum::I64(-11),
                Datum::I64(-1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-13),
                Datum::I64(-11),
                Datum::I64(1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(33),
                Datum::I64(11),
                Datum::I64(3),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-33),
                Datum::I64(11),
                Datum::I64(-3),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(33),
                Datum::I64(-11),
                Datum::I64(-3),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-33),
                Datum::I64(-11),
                Datum::I64(3),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(11),
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-11),
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::U64(3),
                Datum::I64(-5),
                Datum::U64(0),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(-3),
                Datum::U64(5),
                Datum::U64(0),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(i64::MIN + 1),
                Datum::I64(-1),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(i64::MIN),
                Datum::I64(1),
                Datum::I64(i64::MIN),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(i64::MAX),
                Datum::I64(1),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::U64(u64::MAX),
                Datum::I64(1),
                Datum::U64(u64::MAX),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(13),
                Datum::I64(11),
                Datum::I64(2),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(-13),
                Datum::I64(11),
                Datum::I64(-2),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(13),
                Datum::I64(-11),
                Datum::I64(2),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(-13),
                Datum::I64(-11),
                Datum::I64(-2),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(33),
                Datum::I64(11),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(33),
                Datum::I64(-11),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(-33),
                Datum::I64(-11),
                Datum::I64(0),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(11),
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(-11),
                Datum::I64(0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
                Datum::I64(-1),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::U64(u64::MAX),
                Datum::I64(i64::MIN),
                Datum::U64(i64::MAX as u64),
            ),
            (
                ScalarFuncSig::ModInt,
                Datum::I64(i64::MIN),
                Datum::U64(u64::MAX),
                Datum::U64(i64::MIN as u64),
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let lus = lhs
                .get_field_type()
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED);
            let rus = rhs
                .get_field_type()
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED);
            let unsigned = lus | rus;

            let mut op = Expression::build(&mut ctx, scalar_func_expr(tt.0, &[lhs, rhs])).unwrap();
            if unsigned {
                // According to MilevaDB, the result is unsigned if any of arguments is unsigned.
                op.mut_field_type()
                    .as_mut_accessor()
                    .set_flag(FieldTypeFlag::UNSIGNED);
            }

            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, tt.3);
        }
    }

    #[test]
    fn test_arithmetic_real() {
        let tests = vec![
            (
                ScalarFuncSig::PlusReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(1.00001),
            ),
            (
                ScalarFuncSig::MinusReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(1.02001),
            ),
            (
                ScalarFuncSig::MultiplyReal,
                Datum::F64(1.01001),
                Datum::F64(-0.01),
                Datum::F64(-0.0101001),
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(2.0),
                Datum::F64(0.3),
                Datum::F64(6.666666666666667),
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(44.3),
                Datum::F64(0.000),
                Datum::Null,
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::Null,
                Datum::F64(1.0),
                Datum::Null,
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(1.0),
                Datum::Null,
                Datum::Null,
            ), // TODO: support precision in divide.
            // (
            //     ScalarFuncSig::DivideReal,
            //     Datum::F64(-12.3),
            //     Datum::F64(41f64),
            //     Datum::F64(-0.3),
            // ),
            // (
            //     ScalarFuncSig::DivideReal,
            //     Datum::F64(12.3),
            //     Datum::F64(0.3),
            //     Datum::F64(41f64)
            // )
            (
                ScalarFuncSig::ModReal,
                Datum::F64(1.0),
                Datum::F64(1.1),
                Datum::F64(1.0),
            ),
            (
                ScalarFuncSig::ModReal,
                Datum::F64(-1.0),
                Datum::F64(1.1),
                Datum::F64(-1.0),
            ),
            (
                ScalarFuncSig::ModReal,
                Datum::F64(1.0),
                Datum::F64(-1.1),
                Datum::F64(1.0),
            ),
            (
                ScalarFuncSig::ModReal,
                Datum::F64(-1.0),
                Datum::F64(-1.1),
                Datum::F64(-1.0),
            ),
            (
                ScalarFuncSig::ModReal,
                Datum::F64(1.0),
                Datum::F64(0.0),
                Datum::Null,
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(&mut ctx, scalar_func_expr(tt.0, &[lhs, rhs])).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, tt.3);
        }
    }

    #[test]
    fn test_arithmetic_decimal() {
        let tests = vec![
            (
                ScalarFuncSig::PlusDecimal,
                str2dec("1.1"),
                str2dec("2.2"),
                str2dec("3.3"),
            ),
            (
                ScalarFuncSig::MinusDecimal,
                str2dec("1.1"),
                str2dec("2.2"),
                str2dec("-1.1"),
            ),
            (
                ScalarFuncSig::MultiplyDecimal,
                str2dec("1.1"),
                str2dec("2.2"),
                str2dec("2.42"),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("-0.3"),
                str2dec("-41"),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("0.3"),
                str2dec("41"),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::DivideDecimal,
                Datum::Null,
                str2dec("123"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("123"),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("11.01"),
                str2dec("1.1"),
                Datum::I64(10),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("-11.01"),
                str2dec("1.1"),
                Datum::I64(-10),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("11.01"),
                str2dec("-1.1"),
                Datum::I64(-10),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("-11.01"),
                str2dec("-1.1"),
                Datum::I64(10),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("123"),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                Datum::Null,
                str2dec("123"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                str2dec("0.0"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("13"),
                str2dec("11"),
                str2dec("2"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-13"),
                str2dec("11"),
                str2dec("-2"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("13"),
                str2dec("-11"),
                str2dec("2"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-13"),
                str2dec("-11"),
                str2dec("-2"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("33"),
                str2dec("11"),
                str2dec("0"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-33"),
                str2dec("11"),
                str2dec("0"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("33"),
                str2dec("-11"),
                str2dec("0"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-33"),
                str2dec("-11"),
                str2dec("0"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("0.0000000001"),
                str2dec("1.0"),
                str2dec("0.0000000001"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("1"),
                str2dec("1.1"),
                str2dec("1"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-1"),
                str2dec("1.1"),
                str2dec("-1"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("1"),
                str2dec("-1.1"),
                str2dec("1"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-1"),
                str2dec("-1.1"),
                str2dec("-1"),
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("3"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-3"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("0"),
                str2dec("0"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                str2dec("-3"),
                Datum::Null,
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                Datum::Null,
                str2dec("-3"),
                Datum::Null,
            ),
            (
                ScalarFuncSig::ModDecimal,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(&mut ctx, scalar_func_expr(tt.0, &[lhs, rhs])).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, tt.3);
        }
    }

    #[test]
    fn test_arithmetic_overflow_int() {
        let tests = vec![
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::PlusInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MIN),
            ),
            (ScalarFuncSig::PlusInt, Datum::I64(-2), Datum::U64(1)),
            (ScalarFuncSig::PlusInt, Datum::U64(1), Datum::I64(-2)),
            (
                ScalarFuncSig::MinusInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::MinusInt,
                Datum::I64(i64::MAX),
                Datum::I64(i64::MIN),
            ),
            (ScalarFuncSig::MinusInt, Datum::I64(-1), Datum::U64(2)),
            (ScalarFuncSig::MinusInt, Datum::U64(1), Datum::I64(2)),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(i64::MIN),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::U64(u64::MAX),
                Datum::I64(i64::MAX),
            ),
            (
                ScalarFuncSig::MultiplyInt,
                Datum::I64(i64::MIN),
                Datum::U64(1),
            ),
            (
                ScalarFuncSig::IntDivideInt,
                Datum::I64(i64::MIN),
                Datum::I64(-1),
            ),
            (ScalarFuncSig::IntDivideInt, Datum::I64(-1), Datum::U64(1)),
            (ScalarFuncSig::IntDivideInt, Datum::I64(-2), Datum::U64(1)),
            (ScalarFuncSig::IntDivideInt, Datum::U64(1), Datum::I64(-1)),
            (ScalarFuncSig::IntDivideInt, Datum::U64(2), Datum::I64(-1)),
            (
                ScalarFuncSig::IntDivideDecimal,
                Datum::Dec(Decimal::from(i64::MIN)),
                Datum::Dec(Decimal::from(-1)),
            ),
            (
                ScalarFuncSig::IntDivideDecimal,
                Datum::Dec(Decimal::from(i64::MAX)),
                str2dec("0.1"),
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let lus = lhs
                .get_field_type()
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED);
            let rus = rhs
                .get_field_type()
                .as_accessor()
                .flag()
                .contains(FieldTypeFlag::UNSIGNED);
            let unsigned = lus | rus;

            let mut op = Expression::build(&mut ctx, scalar_func_expr(tt.0, &[lhs, rhs])).unwrap();
            if unsigned {
                // According to MilevaDB, the result is unsigned if any of arguments is unsigned.
                op.mut_field_type()
                    .as_mut_accessor()
                    .set_flag(FieldTypeFlag::UNSIGNED);
            }

            let got = op.eval(&mut ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_multiply_int_unsigned() {
        let cases = vec![
            (Datum::I64(1), Datum::I64(2), Datum::U64(2)),
            (
                Datum::I64(i64::MIN),
                Datum::I64(1),
                Datum::U64(i64::MIN as u64),
            ),
            (
                Datum::I64(i64::MAX),
                Datum::I64(1),
                Datum::U64(i64::MAX as u64),
            ),
            (Datum::U64(u64::MAX), Datum::I64(1), Datum::U64(u64::MAX)),
        ];

        let mut ctx = EvalContext::default();
        for (left, right, exp) in cases {
            let lhs = datum_expr(left);
            let rhs = datum_expr(right);

            let mut op = Expression::build(
                &mut ctx,
                scalar_func_expr(ScalarFuncSig::MultiplyIntUnsigned, &[lhs, rhs]),
            )
            .unwrap();
            op.mut_field_type()
                .as_mut_accessor()
                .set_flag(FieldTypeFlag::UNSIGNED);

            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // test overflow
        let cases = vec![
            (Datum::I64(-1), Datum::I64(2)),
            (Datum::I64(i64::MAX), Datum::I64(i64::MAX)),
            (Datum::I64(i64::MIN), Datum::I64(i64::MIN)),
        ];

        for (left, right) in cases {
            let lhs = datum_expr(left);
            let rhs = datum_expr(right);

            let mut op = Expression::build(
                &mut ctx,
                scalar_func_expr(ScalarFuncSig::MultiplyIntUnsigned, &[lhs, rhs]),
            )
            .unwrap();
            op.mut_field_type()
                .as_mut_accessor()
                .set_flag(FieldTypeFlag::UNSIGNED);

            let got = op.eval(&mut ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_arithmetic_overflow_real() {
        let tests = vec![
            (
                ScalarFuncSig::PlusReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::MinusReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::MultiplyReal,
                Datum::F64(f64::MIN),
                Datum::F64(f64::MAX),
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(f64::MAX),
                Datum::F64(0.00001),
            ),
        ];
        let mut ctx = EvalContext::default();
        for tt in tests {
            let lhs = datum_expr(tt.1);
            let rhs = datum_expr(tt.2);

            let op = Expression::build(&mut ctx, scalar_func_expr(tt.0, &[lhs, rhs])).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap_err();
            assert!(check_overflow(got).is_ok());
        }
    }

    #[test]
    fn test_divide_by_zero() {
        let data = vec![
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(f64::MAX),
                Datum::F64(f64::from(0)),
            ),
            (
                ScalarFuncSig::DivideReal,
                Datum::F64(f64::MAX),
                Datum::F64(0.00000),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("0.0"),
            ),
            (
                ScalarFuncSig::DivideDecimal,
                str2dec("12.3"),
                str2dec("-0.0"),
            ),
        ];

        let cases = vec![
            //(flag,sql_mode,is_ok,has_warning)
            (Flag::empty(), SqlMode::empty(), true, true), //warning
            (
                Flag::IN_fidelio_OR_DELETE_STMT,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_BlockS,
                false,
                false,
            ), //error
            (
                Flag::IN_fidelio_OR_DELETE_STMT,
                SqlMode::STRICT_ALL_BlockS,
                true,
                false,
            ), //ok
            (
                Flag::IN_fidelio_OR_DELETE_STMT | Flag::DIVIDED_BY_ZERO_AS_WARNING,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO | SqlMode::STRICT_ALL_BlockS,
                true,
                true,
            ), //warning
        ];
        for (sig, left, right) in data {
            let lhs = datum_expr(left);
            let rhs = datum_expr(right);
            let scalar_func = scalar_func_expr(sig, &[lhs, rhs]);
            for (flag, sql_mode, is_ok, has_warning) in &cases {
                let mut causet = EvalConfig::new();
                causet.set_flag(*flag).set_sql_mode(*sql_mode);
                let mut ctx = EvalContext::new(::std::sync::Arc::new(causet));
                let op = Expression::build(&mut ctx, scalar_func.clone()).unwrap();
                let got = op.eval(&mut ctx, &[]);
                if *is_ok {
                    assert_eq!(got.unwrap(), Datum::Null);
                } else {
                    assert!(check_divide_by_zero(got.unwrap_err()).is_ok());
                }
                if *has_warning {
                    assert_eq!(
                        ctx.take_warnings().warnings[0].get_code(),
                        ERR_DIVISION_BY_ZERO
                    );
                } else {
                    assert!(ctx.take_warnings().warnings.is_empty());
                }
            }
        }
    }
}
