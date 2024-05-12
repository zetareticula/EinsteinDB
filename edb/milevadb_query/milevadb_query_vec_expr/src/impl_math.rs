use std::cell::RefCell;

use num::promises::Pow;
use milevadb_query_codegen::rpn_fn;

use milevadb_query_common::Result;
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::codec::mysql::{RoundMode, DEFAULT_FSP};
use milevadb_query_datatype::codec::{self, Error};
use milevadb_query_datatype::expr::EvalContext;
use milevadb_query_shared_expr::rand::MySQLRng;

const I64_TEN_POWS: [i64; 19] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

#[rpn_fn]
#[inline]
pub fn pi() -> Result<Option<Real>> {
    Ok(Some(Real::from(std::f64::consts::PI)))
}

#[rpn_fn]
#[inline]
pub fn crc32(arg: BytesRef) -> Result<Option<Int>> {
    Ok(Some(i64::from(violetabftstore::interlock::::file::calc_crc32_bytes(&arg))))
}

#[inline]
#[rpn_fn]
pub fn log_1_arg(arg: &Real) -> Result<Option<Real>> {
    Ok(f64_to_real(arg.ln()))
}

#[inline]
#[rpn_fn]
#[allow(clippy::float_cmp)]
pub fn log_2_arg(arg0: &Real, arg1: &Real) -> Result<Option<Real>> {
    Ok({
        if **arg0 <= 0f64 || **arg0 == 1f64 || **arg1 <= 0f64 {
            None
        } else {
            f64_to_real(arg1.log(**arg0))
        }
    })
}

#[inline]
#[rpn_fn]
pub fn log2(arg: &Real) -> Result<Option<Real>> {
    Ok(f64_to_real(arg.log2()))
}

#[inline]
#[rpn_fn]
pub fn log10(arg: &Real) -> Result<Option<Real>> {
    Ok(f64_to_real(arg.log10()))
}

// If the given f64 is finite, returns `Some(Real)`. Otherwise returns None.
fn f64_to_real(n: f64) -> Option<Real> {
    if n.is_finite() {
        Some(Real::from(n))
    } else {
        None
    }
}

#[inline]
#[rpn_fn(capture = [ctx])]
pub fn ceil<C: Ceil>(ctx: &mut EvalContext, arg: &C::Input) -> Result<Option<C::Output>> {
    C::ceil(ctx, arg)
}

pub trait Ceil {
    type Input: Evaluable + EvaluableRet;
    type Output: EvaluableRet;

    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>>;
}

pub struct CeilReal;

impl Ceil for CeilReal {
    type Input = Real;
    type Output = Real;

    #[inline]
    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(Real::from(arg.ceil())))
    }
}

pub struct CeilDecToDec;

impl Ceil for CeilDecToDec {
    type Input = Decimal;
    type Output = Decimal;

    #[inline]
    fn ceil(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg.ceil().into_result(ctx).map(Some)?)
    }
}

pub struct CeilIntToDec;

impl Ceil for CeilIntToDec {
    type Input = Int;
    type Output = Decimal;

    #[inline]
    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(Decimal::from(*arg)))
    }
}

pub struct CeilDecToInt;

impl Ceil for CeilDecToInt {
    type Input = Decimal;
    type Output = Int;

    #[inline]
    fn ceil(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg
            .ceil()
            .into_result(ctx)
            .and_then(|decimal| decimal.as_i64_with_ctx(ctx))
            .map(Some)?)
    }
}

pub struct CeilIntToInt;

impl Ceil for CeilIntToInt {
    type Input = Int;
    type Output = Int;

    #[inline]
    fn ceil(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(*arg))
    }
}

#[rpn_fn(capture = [ctx])]
pub fn floor<T: Floor>(ctx: &mut EvalContext, arg: &T::Input) -> Result<Option<T::Output>> {
    T::floor(ctx, arg)
}

pub trait Floor {
    type Input: Evaluable + EvaluableRet;
    type Output: EvaluableRet;
    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>>;
}

pub struct FloorReal;

impl Floor for FloorReal {
    type Input = Real;
    type Output = Real;

    #[inline]
    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(Real::from(arg.floor())))
    }
}

pub struct FloorIntToDec;

impl Floor for FloorIntToDec {
    type Input = Int;
    type Output = Decimal;

    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(Decimal::from(*arg)))
    }
}

pub struct FloorDecToInt;

impl Floor for FloorDecToInt {
    type Input = Decimal;
    type Output = Int;

    #[inline]
    fn floor(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg
            .floor()
            .into_result(ctx)
            .and_then(|decimal| decimal.as_i64_with_ctx(ctx))
            .map(Some)?)
    }
}

pub struct FloorDecToDec;

impl Floor for FloorDecToDec {
    type Input = Decimal;
    type Output = Decimal;

    #[inline]
    fn floor(ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(arg.floor().into_result(ctx).map(Some)?)
    }
}

pub struct FloorIntToInt;

impl Floor for FloorIntToInt {
    type Input = Int;
    type Output = Int;

    #[inline]
    fn floor(_ctx: &mut EvalContext, arg: &Self::Input) -> Result<Option<Self::Output>> {
        Ok(Some(*arg))
    }
}

#[rpn_fn]
#[inline]
fn abs_int(arg: &Int) -> Result<Option<Int>> {
    match (*arg).checked_abs() {
        None => Err(Error::overflow("BIGINT", &format!("abs({})", *arg)).into()),
        Some(arg_abs) => Ok(Some(arg_abs)),
    }
}

#[rpn_fn]
#[inline]
fn abs_uint(arg: &Int) -> Result<Option<Int>> {
    Ok(Some(arg.to_owned()))
}

#[rpn_fn]
#[inline]
fn abs_real(arg: &Real) -> Result<Option<Real>> {
    Ok(Some(num_promises::Signed::abs(arg)))
}

#[rpn_fn]
#[inline]
fn abs_decimal(arg: &Decimal) -> Result<Option<Decimal>> {
    let res: codec::Result<Decimal> = arg.to_owned().abs().into();
    Ok(Some(res?))
}

#[inline]
#[rpn_fn]
fn sign(arg: &Real) -> Result<Option<Int>> {
    Ok(Some({
        if **arg > 0f64 {
            1
        } else if **arg == 0f64 {
            0
        } else {
            -1
        }
    }))
}

#[inline]
#[rpn_fn]
fn sqrt(arg: &Real) -> Result<Option<Real>> {
    Ok({
        if **arg < 0f64 {
            None
        } else {
            let res = arg.sqrt();
            if res.is_nan() {
                None
            } else {
                Some(Real::from(res))
            }
        }
    })
}

#[inline]
#[rpn_fn]
fn radians(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(**arg * std::f64::consts::PI / 180_f64).ok())
}

#[inline]
#[rpn_fn]
pub fn exp(arg: &Real) -> Result<Option<Real>> {
    let ret = arg.exp();
    if ret.is_infinite() {
        Err(Error::overflow("DOUBLE", &format!("exp({})", arg)).into())
    } else {
        Ok(Real::new(ret).ok())
    }
}

#[inline]
#[rpn_fn]
fn sin(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg.sin()).ok())
}

#[inline]
#[rpn_fn]
fn cos(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg.cos()).ok())
}

#[inline]
#[rpn_fn]
fn tan(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg.tan()).ok())
}

#[inline]
#[rpn_fn]
fn cot(arg: &Real) -> Result<Option<Real>> {
    let tan = arg.tan();
    let cot = tan.recip();
    if cot.is_infinite() {
        Err(Error::overflow("DOUBLE", format!("cot({})", arg)).into())
    } else {
        Ok(Real::new(cot).ok())
    }
}

#[inline]
#[rpn_fn]
fn pow(lhs: &Real, rhs: &Real) -> Result<Option<Real>> {
    let pow = (lhs.into_inner()).pow(rhs.into_inner());
    if pow.is_infinite() {
        Err(Error::overflow("DOUBLE", format!("{}.pow({})", lhs, rhs)).into())
    } else {
        Ok(Real::new(pow).ok())
    }
}

#[inline]
#[rpn_fn]
fn rand() -> Result<Option<Real>> {
    let res = MYSQL_RNG.with(|mysql_rng| mysql_rng.borrow_mut().gen());
    Ok(Real::new(res).ok())
}

#[inline]
#[rpn_fn(nullable)]
fn rand_with_seed_first_gen(seed: Option<&i64>) -> Result<Option<Real>> {
    let mut rng = MySQLRng::new_with_seed(seed.cloned().unwrap_or(0));
    let res = rng.gen();
    Ok(Real::new(res).ok())
}

#[inline]
#[rpn_fn]
fn degrees(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg.to_degrees()).ok())
}

#[inline]
#[rpn_fn]
pub fn asin(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg.asin()).ok())
}

#[inline]
#[rpn_fn]
pub fn acos(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg.acos()).ok())
}

#[inline]
#[rpn_fn]
pub fn atan_1_arg(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg.atan()).ok())
}

#[inline]
#[rpn_fn]
pub fn atan_2_args(arg0: &Real, arg1: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg0.atan2(arg1.into_inner())).ok())
}

#[inline]
#[rpn_fn]
pub fn conv(n: BytesRef, from_base: &Int, to_base: &Int) -> Result<Option<Bytes>> {
    use milevadb_query_shared_expr::conv::conv as conv_impl;
    let s = String::from_utf8_lossy(n);
    Ok(conv_impl(s.as_ref(), *from_base, *to_base))
}

#[inline]
#[rpn_fn]
pub fn round_real(arg: &Real) -> Result<Option<Real>> {
    Ok(Real::new(arg.round()).ok())
}

#[inline]
#[rpn_fn]
pub fn round_int(arg: &Int) -> Result<Option<Int>> {
    Ok(Some(arg.to_owned()))
}

#[inline]
#[rpn_fn]
pub fn round_dec(arg: &Decimal) -> Result<Option<Decimal>> {
    let res: codec::Result<Decimal> = arg
        .to_owned()
        .round(DEFAULT_FSP, RoundMode::HalfEven)
        .into();
    Ok(Some(res?))
}

#[inline]
#[rpn_fn]
pub fn truncate_int_with_int(arg0: &Int, arg1: &Int) -> Result<Option<Int>> {
    let x = arg0;
    let d = arg1;
    Ok(Some(if *d >= 0 {
        *x
    } else if *d <= -(I64_TEN_POWS.len() as i64) {
        0
    } else {
        let shift = I64_TEN_POWS[-*d as usize];
        *x / shift * shift
    }))
}

#[inline]
#[rpn_fn]
pub fn truncate_int_with_uint(arg0: &Int, _arg1: &Int) -> Result<Option<Int>> {
    Ok(Some(*arg0))
}

#[inline]
#[rpn_fn]
pub fn truncate_real_with_int(arg0: &Real, arg1: &Int) -> Result<Option<Real>> {
    let x = arg0;
    let d = arg1;
    let d = if *d >= 0 {
        (*d).min(i64::from(i32::max_value())) as i32
    } else {
        (*d).max(i64::from(i32::min_value())) as i32
    };
    Ok(Some(truncate_real(*x, d)))
}

#[inline]
#[rpn_fn]
pub fn truncate_real_with_uint(arg0: &Real, arg1: &Int) -> Result<Option<Real>> {
    let x = arg0;
    let d = (*arg1 as u64).min(i32::max_value() as u64) as i32;
    Ok(Some(truncate_real(*x, d)))
}

pub fn truncate_real(x: Real, d: i32) -> Real {
    let shift = 10_f64.powi(d);
    let tmp = x * shift;
    if *tmp == 0_f64 {
        Real::from(0_f64)
    } else if tmp.is_infinite() {
        x
    } else {
        Real::from(tmp.trunc() / shift)
    }
}

#[inline]
#[rpn_fn]
pub fn round_with_frac_int(arg0: &Int, arg1: &Int) -> Result<Option<Int>> {
    let number = arg0;
    let digits = arg1;
    if *digits >= 0 {
        Ok(Some(*number))
    } else {
        let power = 10.0_f64.powi(-digits as i32);
        let frac = *number as f64 / power;
        Ok(Some((frac.round() * power) as i64))
    }
}

#[rpn_fn]
#[inline]
fn round_with_frac_dec(arg0: &Decimal, arg1: &Int) -> Result<Option<Decimal>> {
    let number = arg0;
    let digits = arg1;
    let res: codec::Result<Decimal> = number
        .to_owned()
        .round(*digits as i8, RoundMode::HalfEven)
        .into();
    Ok(Some(res?))
}

#[inline]
#[rpn_fn]
pub fn round_with_frac_real(arg0: &Real, arg1: &Int) -> Result<Option<Real>> {
    let number = arg0;
    let digits = arg1;
    let power = 10.0_f64.powi(-digits as i32);
    let frac = *number / power;
    Ok(Some(Real::from(frac.round() * power)))
}

thread_local! {
   static MYSQL_RNG: RefCell<MySQLRng> = RefCell::new(MySQLRng::new())
}

#[causet(test)]
mod tests {
    use std::str::FromStr;
    use std::{f64, i64};
    use milevadb_query_datatype::builder::FieldTypeBuilder;
    use milevadb_query_datatype::{FieldTypeFlag, FieldTypeTp};
    use fidel_timeshare::ScalarFuncSig;

    use super::*;
    use crate::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_pi() {
        let output = RpnFnScalarEvaluator::new()
            .evaluate(ScalarFuncSig::Pi)
            .unwrap();
        assert_eq!(output, Some(Real::from(std::f64::consts::PI)));
    }

    #[test]
    fn test_crc32() {
        let cases = vec![
            (Some(""), Some(0)),
            (Some("-1"), Some(808273962)),
            (Some("mysql"), Some(2501908538)),
            (Some("MySQL"), Some(3259397556)),
            (Some("hello"), Some(907060870)),
            (Some("❤️"), Some(4067711813)),
            (None, None),
        ];

        for (input, expect) in cases {
            let input = input.map(|s| s.as_bytes().to_vec());
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Crc32)
                .unwrap();
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_log_1_arg() {
        let test_cases = vec![
            (Some(std::f64::consts::E), Some(Real::from(1.0_f64))),
            (Some(100.0), Some(Real::from(4.605170185988092_f64))),
            (Some(-1.0), None),
            (Some(0.0), None),
            (None, None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Log1Arg)
                .unwrap();
            assert_eq!(output, expect, "{:?}", input);
        }
    }

    #[test]
    fn test_log_2_arg() {
        let test_cases = vec![
            (Some(10.0_f64), Some(100.0_f64), Some(Real::from(2.0_f64))),
            (Some(2.0_f64), Some(1.0_f64), Some(Real::from(0.0_f64))),
            (Some(0.5_f64), Some(0.25_f64), Some(Real::from(2.0_f64))),
            (Some(-0.23323_f64), Some(2.0_f64), None),
            (Some(0_f64), Some(123_f64), None),
            (Some(1_f64), Some(123_f64), None),
            (Some(1123_f64), Some(0_f64), None),
            (None, None, None),
            (Some(2.0_f64), None, None),
            (None, Some(2.0_f64), None),
        ];
        for (a1, a2, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(a1)
                .push_param(a2)
                .evaluate(ScalarFuncSig::Log2Args)
                .unwrap();
            assert_eq!(output, expect, "arg1 {:?}, arg2 {:?}", a1, a2);
        }
    }

    #[test]
    fn test_log2() {
        let test_cases = vec![
            (Some(16_f64), Some(Real::from(4_f64))),
            (Some(5_f64), Some(Real::from(2.321928094887362_f64))),
            (Some(-1.234_f64), None),
            (Some(0_f64), None),
            (None, None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Log2)
                .unwrap();
            assert_eq!(output, expect, "{:?}", input);
        }
    }

    #[test]
    fn test_log10() {
        let test_cases = vec![
            (Some(100_f64), Some(Real::from(2_f64))),
            (Some(101_f64), Some(Real::from(2.0043213737826426_f64))),
            (Some(-1.234_f64), None),
            (Some(0_f64), None),
            (None, None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Log10)
                .unwrap();
            assert_eq!(output, expect, "{:?}", input);
        }
    }

    #[test]
    fn test_abs_int() {
        let test_cases = vec![
            (ScalarFuncSig::AbsInt, -3, Some(3), false),
            (
                ScalarFuncSig::AbsInt,
                std::i64::MAX,
                Some(std::i64::MAX),
                false,
            ),
            (
                ScalarFuncSig::AbsUInt,
                std::u64::MAX as i64,
                Some(std::u64::MAX as i64),
                false,
            ),
            (ScalarFuncSig::AbsInt, std::i64::MIN, Some(0), true),
        ];

        for (sig, arg, expect_output, is_err) in test_cases {
            let output = RpnFnScalarEvaluator::new().push_param(arg).evaluate(sig);

            if is_err {
                assert!(output.is_err());
            } else {
                let output = output.unwrap();
                assert_eq!(output, expect_output, "{:?}", arg);
            }
        }
    }

    #[test]
    fn test_abs_real() {
        let test_cases: Vec<(Real, Option<Real>)> = vec![
            (Real::new(3.5).unwrap(), Real::new(3.5).ok()),
            (Real::new(-3.5).unwrap(), Real::new(3.5).ok()),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::AbsReal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_abs_decimal() {
        let test_cases = vec![("1.1", "1.1"), ("-1.1", "1.1")];

        for (arg, expect_output) in test_cases {
            let arg = arg.parse::<Decimal>().ok();
            let expect_output = expect_output.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::AbsDecimal)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_ceil_real() {
        let cases = vec![
            (4.0, 3.5),
            (4.0, 3.45),
            (4.0, 3.1),
            (-3.0, -3.45),
            (0.0, -0.1),
            (std::f64::MAX, std::f64::MAX),
            (std::f64::MIN, std::f64::MIN),
        ];
        for (expected, input) in cases {
            let arg = Real::from(input);
            let expected = Real::new(expected).ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::CeilReal)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_dec_to_dec() {
        let cases = vec![
            ("9223372036854775808", "9223372036854775808"),
            ("124", "123.456"),
            ("-123", "-123.456"),
        ];

        for (expected, input) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Decimal>(ScalarFuncSig::CeilDecToDec)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_int_to_dec() {
        let cases = vec![
            ("-9223372036854775808", std::i64::MIN),
            ("9223372036854775807", std::i64::MAX),
            ("123", 123),
            ("-123", -123),
        ];
        for (expected, input) in cases {
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Decimal>(ScalarFuncSig::CeilIntToDec)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_dec_to_int() {
        let cases = vec![
            (124, "123.456"),
            (2, "1.23"),
            (-1, "-1.23"),
            (std::i64::MIN, "-9223372036854775808"),
        ];
        for (expected, input) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::CeilDecToInt)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn test_ceil_int_to_int() {
        let cases = vec![
            (1, 1),
            (2, 2),
            (666, 666),
            (-3, -3),
            (-233, -233),
            (std::i64::MAX, std::i64::MAX),
            (std::i64::MIN, std::i64::MIN),
        ];

        for (expected, input) in cases {
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::CeilIntToInt)
                .unwrap();
            assert_eq!(expected, output);
        }
    }

    fn test_unary_func_ok_none<I: Evaluable, O: EvaluableRet>(sig: ScalarFuncSig)
    where
        O: PartialEq,
        Option<I>: Into<ScalarValue>,
        Option<O>: From<ScalarValue>,
    {
        assert_eq!(
            None,
            RpnFnScalarEvaluator::new()
                .push_param(Option::<I>::None)
                .evaluate::<O>(sig)
                .unwrap()
        );
    }

    #[test]
    fn test_floor_real() {
        let cases = vec![
            (3.5, 3.0),
            (3.7, 3.0),
            (3.45, 3.0),
            (3.1, 3.0),
            (-3.45, -4.0),
            (-0.1, -1.0),
            (16140901064495871255.0, 16140901064495871255.0),
            (std::f64::MAX, std::f64::MAX),
            (std::f64::MIN, std::f64::MIN),
        ];
        for (input, expected) in cases {
            let arg = Real::from(input);
            let expected = Real::new(expected).ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::FloorReal)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Real, Real>(ScalarFuncSig::FloorReal);
    }

    #[test]
    fn test_floor_int_to_dec() {
        let tests_cases = vec![
            (std::i64::MIN, "-9223372036854775808"),
            (std::i64::MAX, "9223372036854775807"),
            (123, "123"),
            (-123, "-123"),
        ];

        for (input, expected) in tests_cases {
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Decimal>(ScalarFuncSig::FloorIntToDec)
                .unwrap();
            assert_eq!(output, expected);
        }

        test_unary_func_ok_none::<Int, Decimal>(ScalarFuncSig::FloorIntToDec);
    }

    #[test]
    fn test_floor_dec_to_dec() {
        let cases = vec![
            ("9223372036854775808", "9223372036854775808"),
            ("123.456", "123"),
            ("-123.456", "-124"),
        ];

        for (input, expected) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = expected.parse::<Decimal>().ok();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Decimal>(ScalarFuncSig::FloorDecToDec)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Decimal, Decimal>(ScalarFuncSig::FloorDecToDec);
    }

    #[test]
    fn test_floor_dec_to_int() {
        let cases = vec![
            ("123.456", 123),
            ("1.23", 1),
            ("-1.23", -2),
            ("-9223372036854775808", std::i64::MIN),
        ];
        for (input, expected) in cases {
            let arg = input.parse::<Decimal>().ok();
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::FloorDecToInt)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Decimal, Int>(ScalarFuncSig::FloorDecToInt);
    }

    #[test]
    fn test_floor_int_to_int() {
        let cases = vec![
            (1, 1),
            (2, 2),
            (-3, -3),
            (std::i64::MAX, std::i64::MAX),
            (std::i64::MIN, std::i64::MIN),
        ];

        for (expected, input) in cases {
            let expected = Some(expected);
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate::<Int>(ScalarFuncSig::FloorIntToInt)
                .unwrap();
            assert_eq!(expected, output);
        }

        test_unary_func_ok_none::<Int, Int>(ScalarFuncSig::FloorIntToInt);
    }

    #[test]
    fn test_sign() {
        let test_cases = vec![
            (None, None),
            (Some(42f64), Some(1)),
            (Some(0f64), Some(0)),
            (Some(-47f64), Some(-1)),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Sign)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_sqrt() {
        let test_cases = vec![
            (None, None),
            (Some(64f64), Some(Real::from(8f64))),
            (Some(2f64), Some(Real::from(std::f64::consts::SQRT_2))),
            (Some(-16f64), None),
            (Some(std::f64::NAN), None),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Sqrt)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_radians() {
        let test_cases = vec![
            (None, None),
            (Some(0_f64), Some(Real::from(0_f64))),
            (Some(180_f64), Some(Real::from(std::f64::consts::PI))),
            (
                Some(-360_f64),
                Some(Real::from(-2_f64 * std::f64::consts::PI)),
            ),
            (Some(std::f64::NAN), None),
            (
                Some(std::f64::INFINITY),
                Some(Real::from(std::f64::INFINITY)),
            ),
        ];
        for (input, expect) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Radians)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_exp() {
        let tests = vec![
            (1_f64, std::f64::consts::E),
            (1.23_f64, 3.4212295362896734),
            (-1.23_f64, 0.2922925776808594),
            (0_f64, 1_f64),
        ];
        for (x, expected) in tests {
            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(x)))
                .evaluate(ScalarFuncSig::Exp)
                .unwrap();
            assert_eq!(output, Some(Real::from(expected)));
        }
        test_unary_func_ok_none::<Real, Real>(ScalarFuncSig::Exp);

        let overflow_tests = vec![100000_f64];
        for x in overflow_tests {
            let output: Result<Option<Real>> = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(x)))
                .evaluate(ScalarFuncSig::Exp);
            assert!(output.is_err());
        }
    }

    #[test]
    fn test_degrees() {
        let tests_cases = vec![
            (None, None),
            (Some(std::f64::NAN), None),
            (Some(0f64), Some(Real::from(0f64))),
            (Some(1f64), Some(Real::from(57.29577951308232_f64))),
            (Some(std::f64::consts::PI), Some(Real::from(180.0_f64))),
            (
                Some(-std::f64::consts::PI / 2.0_f64),
                Some(Real::from(-90.0_f64)),
            ),
        ];
        for (input, expect) in tests_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Degrees)
                .unwrap();
            assert_eq!(expect, output, "{:?}", input);
        }
    }

    #[test]
    fn test_sin() {
        let valid_test_cases = vec![
            (0.0_f64, 0.0_f64),
            (
                std::f64::consts::PI / 4.0_f64,
                std::f64::consts::FRAC_1_SQRT_2,
            ),
            (std::f64::consts::PI / 2.0_f64, 1.0_f64),
            (std::f64::consts::PI, 0.0_f64),
        ];
        for (input, expect) in valid_test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(input)))
                .evaluate(ScalarFuncSig::Sin)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_cos() {
        let test_cases = vec![
            (0f64, 1f64),
            (std::f64::consts::PI / 2f64, 0f64),
            (std::f64::consts::PI, -1f64),
            (-std::f64::consts::PI, -1f64),
        ];
        for (input, expect) in test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(input)))
                .evaluate(ScalarFuncSig::Cos)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_tan() {
        let test_cases = vec![
            (0.0_f64, 0.0_f64),
            (std::f64::consts::PI / 4.0_f64, 1.0_f64),
            (-std::f64::consts::PI / 4.0_f64, -1.0_f64),
            (std::f64::consts::PI, 0.0_f64),
            (
                (std::f64::consts::PI * 3.0) / 4.0,
                f64::tan((std::f64::consts::PI * 3.0) / 4.0), //in mysql and rust, it equals -1.0000000000000002, not -1
            ),
        ];
        for (input, expect) in test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(input)))
                .evaluate(ScalarFuncSig::Tan)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_cot() {
        let test_cases = vec![
            (-1.0_f64, -0.6420926159343308_f64),
            (1.0_f64, 0.6420926159343308_f64),
            (
                std::f64::consts::PI / 4.0_f64,
                1.0_f64 / f64::tan(std::f64::consts::PI / 4.0_f64),
            ),
            (
                std::f64::consts::PI / 2.0_f64,
                1.0_f64 / f64::tan(std::f64::consts::PI / 2.0_f64),
            ),
            (
                std::f64::consts::PI,
                1.0_f64 / f64::tan(std::f64::consts::PI),
            ),
        ];
        for (input, expect) in test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(input)))
                .evaluate(ScalarFuncSig::Cot)
                .unwrap();
            assert!((output.unwrap().into_inner() - expect).abs() < std::f64::EPSILON);
        }
        assert!(RpnFnScalarEvaluator::new()
            .push_param(Some(Real::from(0.0_f64)))
            .evaluate::<Real>(ScalarFuncSig::Cot)
            .is_err());
    }

    #[test]
    fn test_pow() {
        let cases = vec![
            (
                Some(Real::from(1.0f64)),
                Some(Real::from(3.0f64)),
                Some(Real::from(1.0f64)),
            ),
            (
                Some(Real::from(3.0f64)),
                Some(Real::from(0.0f64)),
                Some(Real::from(1.0f64)),
            ),
            (
                Some(Real::from(2.0f64)),
                Some(Real::from(4.0f64)),
                Some(Real::from(16.0f64)),
            ),
            (
                Some(Real::from(std::f64::INFINITY)),
                Some(Real::from(0.0f64)),
                Some(Real::from(1.0f64)),
            ),
            (Some(Real::from(4.0f64)), None, None),
            (None, Some(Real::from(4.0f64)), None),
            (None, None, None),
        ];

        for (lhs, rhs, expect) in cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::Pow)
                .unwrap();
            assert_eq!(output, expect);
        }

        let invalid_cases = vec![
            (
                Some(Real::from(std::f64::INFINITY)),
                Some(Real::from(std::f64::INFINITY)),
            ),
            (Some(Real::from(0.0f64)), Some(Real::from(-9999999.0f64))),
        ];

        for (lhs, rhs) in invalid_cases {
            assert!(RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate::<Real>(ScalarFuncSig::Pow)
                .is_err());
        }
    }

    #[test]
    fn test_rand() {
        let got1 = RpnFnScalarEvaluator::new()
            .evaluate::<Real>(ScalarFuncSig::Rand)
            .unwrap()
            .unwrap();
        let got2 = RpnFnScalarEvaluator::new()
            .evaluate::<Real>(ScalarFuncSig::Rand)
            .unwrap()
            .unwrap();

        assert!(got1 < Real::from(1.0));
        assert!(got1 >= Real::from(0.0));
        assert!(got2 < Real::from(1.0));
        assert!(got2 >= Real::from(0.0));
        assert_ne!(got1, got2);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_rand_with_seed_first_gen() {
        let tests: Vec<(i64, f64)> = vec![
            (0, 0.15522042769493574),
            (1, 0.40540353712197724),
            (-1, 0.9050373219931845),
            (622337, 0.3608469249315997),
            (10000000009, 0.3472714008272359),
            (-1845798578934, 0.5058874688166077),
            (922337203685, 0.40536338501178043),
            (922337203685477580, 0.5550739490939993),
            (9223372036854775807, 0.9050373219931845),
        ];

        for (seed, exp) in tests {
            let got = RpnFnScalarEvaluator::new()
                .push_param(Some(seed))
                .evaluate::<Real>(ScalarFuncSig::RandWithSeedFirstGen)
                .unwrap()
                .unwrap();
            assert_eq!(got, Real::from(exp));
        }

        let none_case_got = RpnFnScalarEvaluator::new()
            .push_param(ScalarValue::Int(None))
            .evaluate::<Real>(ScalarFuncSig::RandWithSeedFirstGen)
            .unwrap()
            .unwrap();
        assert_eq!(none_case_got, Real::from(0.15522042769493574));
    }

    #[test]
    fn test_asin() {
        let test_cases = vec![
            (Some(Real::from(0.0_f64)), Some(Real::from(0.0_f64))),
            (
                Some(Real::from(1.0_f64)),
                Some(Real::from(std::f64::consts::PI / 2.0_f64)),
            ),
            (
                Some(Real::from(-1.0_f64)),
                Some(Real::from(-std::f64::consts::PI / 2.0_f64)),
            ),
            (
                Some(Real::from(std::f64::consts::SQRT_2 / 2.0_f64)),
                Some(Real::from(std::f64::consts::PI / 4.0_f64)),
            ),
        ];
        for (input, expect) in test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Asin)
                .unwrap();
            assert!((output.unwrap() - expect.unwrap()).abs() < std::f64::EPSILON);
        }
        let invalid_test_cases = vec![
            (Some(Real::from(std::f64::INFINITY)), None),
            (Some(Real::from(2.0_f64)), None),
            (Some(Real::from(-2.0_f64)), None),
        ];
        for (input, expect) in invalid_test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Asin)
                .unwrap();
            assert_eq!(expect, output);
        }
    }

    #[test]
    fn test_acos() {
        let test_cases = vec![
            (
                Some(Real::from(0.0_f64)),
                Some(Real::from(std::f64::consts::PI / 2.0_f64)),
            ),
            (Some(Real::from(1.0_f64)), Some(Real::from(0.0_f64))),
            (
                Some(Real::from(-1.0_f64)),
                Some(Real::from(std::f64::consts::PI)),
            ),
            (
                Some(Real::from(std::f64::consts::SQRT_2 / 2.0_f64)),
                Some(Real::from(std::f64::consts::PI / 4.0_f64)),
            ),
        ];
        for (input, expect) in test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Acos)
                .unwrap();
            assert!((output.unwrap() - expect.unwrap()).abs() < std::f64::EPSILON);
        }
        let invalid_test_cases = vec![
            (Some(Real::from(std::f64::INFINITY)), None),
            (Some(Real::from(2.0_f64)), None),
            (Some(Real::from(-2.0_f64)), None),
        ];
        for (input, expect) in invalid_test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Acos)
                .unwrap();
            assert_eq!(expect, output);
        }
    }

    #[test]
    fn test_atan_1_arg() {
        let test_cases = vec![
            (
                Some(Real::from(1.0_f64)),
                Some(Real::from(std::f64::consts::PI / 4.0_f64)),
            ),
            (
                Some(Real::from(-1.0_f64)),
                Some(Real::from(-std::f64::consts::PI / 4.0_f64)),
            ),
            (
                Some(Real::from(std::f64::MAX)),
                Some(Real::from(std::f64::consts::PI / 2.0_f64)),
            ),
            (
                Some(Real::from(std::f64::MIN)),
                Some(Real::from(-std::f64::consts::PI / 2.0_f64)),
            ),
            (Some(Real::from(0.0_f64)), Some(Real::from(0.0_f64))),
        ];
        for (input, expect) in test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(input)
                .evaluate(ScalarFuncSig::Atan1Arg)
                .unwrap();
            assert!((output.unwrap() - expect.unwrap()).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_atan_2_args() {
        let test_cases = vec![
            (
                Some(Real::from(0.0_f64)),
                Some(Real::from(0.0_f64)),
                Some(Real::from(0.0_f64)),
            ),
            (
                Some(Real::from(0.0_f64)),
                Some(Real::from(-1.0_f64)),
                Some(Real::from(std::f64::consts::PI)),
            ),
            (
                Some(Real::from(1.0_f64)),
                Some(Real::from(-1.0_f64)),
                Some(Real::from(3.0_f64 * std::f64::consts::PI / 4.0_f64)),
            ),
            (
                Some(Real::from(-1.0_f64)),
                Some(Real::from(1.0_f64)),
                Some(Real::from(-std::f64::consts::PI / 4.0_f64)),
            ),
            (
                Some(Real::from(1.0_f64)),
                Some(Real::from(0.0_f64)),
                Some(Real::from(std::f64::consts::PI / 2.0_f64)),
            ),
        ];
        for (arg0, arg1, expect) in test_cases {
            let output: Option<Real> = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::Atan2Args)
                .unwrap();
            assert!((output.unwrap() - expect.unwrap()).abs() < std::f64::EPSILON);
        }
    }

    #[test]
    fn test_conv() {
        let tests = vec![
            ("a", 16, 2, "1010"),
            ("6E", 18, 8, "172"),
            ("-17", 10, -18, "-H"),
            ("  -17", 10, -18, "-H"),
            ("-17", 10, 18, "2D3FGB0B9CG4BD1H"),
            ("+18aZ", 7, 36, "1"),
            ("  +18aZ", 7, 36, "1"),
            ("18446744073709551615", -10, 16, "7FFFFFFFFFFFFFFF"),
            ("12F", -10, 16, "C"),
            ("  FF ", 16, 10, "255"),
            ("MilevaDB", 10, 8, "0"),
            ("aa", 10, 2, "0"),
            (" A", -10, 16, "0"),
            ("a6a", 10, 8, "0"),
            ("16九a", 10, 8, "20"),
            ("+", 10, 8, "0"),
            ("-", 10, 8, "0"),
        ];
        for (n, f, t, e) in tests {
            let n = Some(n.as_bytes().to_vec());
            let f = Some(f);
            let t = Some(t);
            let e = Some(e.as_bytes().to_vec());
            let got = RpnFnScalarEvaluator::new()
                .push_param(n)
                .push_param(f)
                .push_param(t)
                .evaluate(ScalarFuncSig::Conv)
                .unwrap();
            assert_eq!(got, e);
        }

        let invalid_tests = vec![
            (None, Some(10), Some(10), None),
            (Some(b"a6a".to_vec()), Some(1), Some(8), None),
        ];
        for (n, f, t, e) in invalid_tests {
            let got = RpnFnScalarEvaluator::new()
                .push_param(n)
                .push_param(f)
                .push_param(t)
                .evaluate::<Bytes>(ScalarFuncSig::Conv)
                .unwrap();
            assert_eq!(got, e);
        }
    }

    #[test]
    fn test_round_real() {
        let test_cases = vec![
            (Some(Real::from(-3.12_f64)), Some(Real::from(-3f64))),
            (Some(Real::from(f64::MAX)), Some(Real::from(f64::MAX))),
            (Some(Real::from(f64::MIN)), Some(Real::from(f64::MIN))),
            (None, None),
        ];

        for (arg, exp) in test_cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Real>(ScalarFuncSig::RoundReal)
                .unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_round_int() {
        let test_cases = vec![
            (Some(Int::from(1)), Some(Int::from(1))),
            (Some(Int::from(i64::MAX)), Some(Int::from(i64::MAX))),
            (Some(Int::from(i64::MIN)), Some(Int::from(i64::MIN))),
            (None, None),
        ];

        for (arg, exp) in test_cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Int>(ScalarFuncSig::RoundInt)
                .unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_round_dec() {
        let test_cases = vec![
            (
                Some(Decimal::from_str("123.1").unwrap()),
                Some(Decimal::from_str("123.0").unwrap()),
            ),
            (
                Some(Decimal::from_str("-1111.1").unwrap()),
                Some(Decimal::from_str("-1111.0").unwrap()),
            ),
            (None, None),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Decimal>(ScalarFuncSig::RoundDec)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_truncate_int() {
        let tests = vec![
            (1028 as i64, 0 as i64, false, 1028 as i64),
            (1028, 5, false, 1028),
            (1028, -2, false, 1000),
            (1028, 309, false, 1028),
            (1028, i64::min_value(), false, 0),
            (1028, u64::max_value() as i64, true, 1028),
        ];
        for (lhs, rhs, rhs_is_unsigned, expected) in tests {
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();

            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(Int::from(lhs)))
                .push_param_with_field_type(Some(rhs), rhs_field_type)
                .evaluate::<Int>(ScalarFuncSig::TruncateInt)
                .unwrap();

            assert_eq!(output, Some(Int::from(expected)));
        }
    }

    #[test]
    fn test_truncate_real() {
        let test_cases = vec![
            (-1.23, 0, false, -1.0),
            (1.58, 0, false, 1.0),
            (1.298, 1, false, 1.2),
            (123.2, -1, false, 120.0),
            (123.2, 100, false, 123.2),
            (123.2, -100, false, 0.0),
            (123.2, i64::max_value(), false, 123.2),
            (123.2, i64::min_value(), false, 0.0),
            (123.2, u64::max_value() as i64, true, 123.2),
            (-1.23, 0, false, -1.0),
            (
                1.797693134862315708145274237317043567981e+308,
                2,
                false,
                1.797693134862315708145274237317043567981e+308,
            ),
        ];
        for (lhs, rhs, rhs_is_unsigned, expected) in test_cases {
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();

            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(Real::from(lhs)))
                .push_param_with_field_type(Some(rhs), rhs_field_type)
                .evaluate::<Real>(ScalarFuncSig::TruncateReal)
                .unwrap();

            assert_eq!(output, Some(Real::from(expected)));
        }
    }

    #[test]
    fn test_round_frac() {
        let int_cases = vec![
            (Some(Int::from(23)), Some(Int::from(2)), Some(Int::from(23))),
            (
                Some(Int::from(23)),
                Some(Int::from(-1)),
                Some(Int::from(20)),
            ),
            (
                Some(Int::from(-27)),
                Some(Int::from(-1)),
                Some(Int::from(-30)),
            ),
            (
                Some(Int::from(-27)),
                Some(Int::from(-2)),
                Some(Int::from(0)),
            ),
            (
                Some(Int::from(-27)),
                Some(Int::from(-2)),
                Some(Int::from(0)),
            ),
            (None, Some(Int::from(-27)), None),
            (Some(Int::from(-27)), None, None),
            (None, None, None),
        ];

        for (arg0, arg1, exp) in int_cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::RoundWithFracInt)
                .unwrap();
            assert_eq!(got, exp);
        }

        let dec_cases = vec![
            (
                Some(Decimal::from_str("150.000").unwrap()),
                Some(Int::from(2)),
                Some(Decimal::from_str("150.000").unwrap()),
            ),
            (
                Some(Decimal::from_str("150.257").unwrap()),
                Some(Int::from(1)),
                Some(Decimal::from_str("150.3").unwrap()),
            ),
            (
                Some(Decimal::from_str("153.257").unwrap()),
                Some(Int::from(-1)),
                Some(Decimal::from_str("150").unwrap()),
            ),
            (Some(Decimal::from_str("153.257").unwrap()), None, None),
            (None, Some(Int::from(-27)), None),
            (None, None, None),
        ];

        for (arg0, arg1, exp) in dec_cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::RoundWithFracDec)
                .unwrap();
            assert_eq!(got, exp);
        }

        let real_cases = vec![
            (
                Some(Real::from(-1.298_f64)),
                Some(Int::from(1)),
                Some(Real::from(-1.3_f64)),
            ),
            (
                Some(Real::from(-1.298_f64)),
                Some(Int::from(0)),
                Some(Real::from(-1.0_f64)),
            ),
            (
                Some(Real::from(23.298_f64)),
                Some(Int::from(2)),
                Some(Real::from(23.30_f64)),
            ),
            (
                Some(Real::from(23.298_f64)),
                Some(Int::from(-1)),
                Some(Real::from(20.0_f64)),
            ),
            (Some(Real::from(23.298_f64)), None, None),
            (None, Some(Int::from(2)), None),
            (None, None, None),
        ];

        for (arg0, arg1, exp) in real_cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(ScalarFuncSig::RoundWithFracReal)
                .unwrap();
            assert_eq!(got, exp);
        }
    }
}
