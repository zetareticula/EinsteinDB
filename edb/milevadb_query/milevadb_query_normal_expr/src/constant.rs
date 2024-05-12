// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::borrow::Cow;

use crate::Constant;
use milevadb_query_datatype::codec::mysql::{Decimal, Duration, Json, Time};
use milevadb_query_datatype::codec::Datum;
use milevadb_query_datatype::expr::Result;

impl Constant {
    pub fn eval(&self) -> Datum {
        self.val.clone()
    }

    #[inline]
    pub fn eval_int(&self) -> Result<Option<i64>> {
        self.val.as_int()
    }

    #[inline]
    pub fn eval_real(&self) -> Result<Option<f64>> {
        self.val.as_real()
    }

    #[inline]
    pub fn eval_decimal(&self) -> Result<Option<Cow<'_, Decimal>>> {
        self.val.as_decimal()
    }

    #[inline]
    pub fn eval_string(&self) -> Result<Option<Cow<'_, [u8]>>> {
        self.val.as_string()
    }

    #[inline]
    pub fn eval_time(&self) -> Result<Option<Cow<'_, Time>>> {
        self.val.as_time()
    }

    #[inline]
    pub fn eval_duration(&self) -> Result<Option<Duration>> {
        self.val.as_duration()
    }

    #[inline]
    pub fn eval_json(&self) -> Result<Option<Cow<'_, Json>>> {
        self.val.as_json()
    }
}

#[causet(test)]
mod tests {
    use crate::tests::datum_expr;
    use crate::Expression;
    use std::u64;
    use milevadb_query_datatype::codec::mysql::{Decimal, Duration, Json, Time};
    use milevadb_query_datatype::codec::Datum;
    use milevadb_query_datatype::expr::EvalContext;

    #[derive(PartialEq, Debug)]
    struct EvalResults(
        Option<i64>,
        Option<f64>,
        Option<Decimal>,
        Option<Vec<u8>>,
        Option<Time>,
        Option<Duration>,
        Option<Json>,
    );

    #[test]
    fn test_constant_eval() {
        let dec = "1.1".parse::<Decimal>().unwrap();
        let s = "你好".as_bytes().to_owned();
        let dur = Duration::parse(&mut EvalContext::default(), b"01:00:00", 0).unwrap();

        let tests = vec![
            datum_expr(Datum::Null),
            datum_expr(Datum::I64(-30)),
            datum_expr(Datum::U64(u64::MAX)),
            datum_expr(Datum::F64(124.32)),
            datum_expr(Datum::Dec(dec)),
            datum_expr(Datum::Bytes(s.clone())),
            datum_expr(Datum::Dur(dur)),
        ];

        let expecteds = vec![
            EvalResults(None, None, None, None, None, None, None),
            EvalResults(Some(-30), None, None, None, None, None, None),
            EvalResults(Some(-1), None, None, None, None, None, None),
            EvalResults(None, Some(124.32), None, None, None, None, None),
            EvalResults(None, None, Some(dec), None, None, None, None),
            EvalResults(None, None, None, Some(s), None, None, None),
            EvalResults(None, None, None, None, None, Some(dur), None),
        ];

        let mut ctx = EvalContext::default();
        for (case, expected) in tests.into_iter().zip(expecteds.into_iter()) {
            let expr = Expression::build(&mut ctx, case).unwrap();

            let int = expr.eval_int(&mut ctx, &[]).unwrap_or(None);
            let real = expr.eval_real(&mut ctx, &[]).unwrap_or(None);
            let dec = expr
                .eval_decimal(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let string = expr
                .eval_string(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let time = expr
                .eval_time(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let dur = expr.eval_duration(&mut ctx, &[]).unwrap_or(None);
            let json = expr
                .eval_json(&mut ctx, &[])
                .unwrap_or(None)
                .map(|t| t.into_owned());

            let result = EvalResults(int, real, dec, string, time, dur, json);
            assert_eq!(expected, result);
        }
    }
}
