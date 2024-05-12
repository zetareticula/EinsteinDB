// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use milevadb_query_codegen::AggrFunction;
use milevadb_query_datatype::builder::FieldTypeBuilder;
use milevadb_query_datatype::{EvalType, FieldTypeFlag, FieldTypeTp};
use fidel_timeshare::{Expr, ExprType, FieldType};

use super::summable::Summable;
use super::*;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::expr::EvalContext;
use milevadb_query_vec_expr::RpnExpression;

/// The parser for AVG aggregate function.
pub struct AggrFnDefinitionParserAvg;

impl super::AggrDefinitionParser for AggrFnDefinitionParserAvg {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Avg);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    #[inline]
    fn parse_rpn(
        &self,
        mut root_expr: Expr,
        mut exp: RpnExpression,
        _ctx: &mut EvalContext,
        src_schemaReplicant: &[FieldType],
        out_schemaReplicant: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        use std::convert::TryFrom;
        use milevadb_query_datatype::FieldTypeAccessor;

        assert_eq!(root_expr.get_tp(), ExprType::Avg);

        let col_sum_ft = root_expr.take_field_type();
        let col_sum_et = box_try!(EvalType::try_from(col_sum_ft.as_accessor().tp()));

        // Rewrite expression to insert CAST() if needed.
        super::util::rewrite_exp_for_sum_avg(src_schemaReplicant, &mut exp).unwrap();

        let rewritten_eval_type =
            EvalType::try_from(exp.ret_field_type(src_schemaReplicant).as_accessor().tp()).unwrap();
        if col_sum_et != rewritten_eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                col_sum_ft.as_accessor().tp()
            ));
        }

        // AVG outputs two PrimaryCausets.
        out_schemaReplicant.push(
            FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(FieldTypeFlag::UNSIGNED)
                .build(),
        );
        out_schemaReplicant.push(col_sum_ft);
        out_exp.push(exp);

        Ok(match rewritten_eval_type {
            EvalType::Decimal => Box::new(AggrFnAvg::<Decimal>::new()),
            EvalType::Real => Box::new(AggrFnAvg::<Real>::new()),
            _ => unreachable!(),
        })
    }
}

/// The AVG aggregate function.
///
/// Note that there are `AVG(Decimal) -> (Int, Decimal)` and `AVG(Double) -> (Int, Double)`.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateAvg::<T>::new())]
pub struct AggrFnAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<T>,
}

impl<T> AggrFnAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// The state of the AVG aggregate function.
#[derive(Debug)]
pub struct AggrFnStateAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    sum: T,
    count: usize,
}

impl<T> AggrFnStateAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            sum: T::zero(),
            count: 0,
        }
    }

    #[inline]
    fn fidelio_concrete<'a, TT>(&mut self, ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T>,
    {
        match value {
            None => Ok(()),
            Some(value) => {
                self.sum.add_assign(ctx, &value.to_owned_value())?;
                self.count += 1;
                Ok(())
            }
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggrFnStateAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = &'static T;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        // Note: The result of `AVG()` is returned as `(count, sum)`.
        assert_eq!(target.len(), 2);
        target[0].push_int(Some(self.count as Int));
        if self.count == 0 {
            target[1].push(None);
        } else {
            target[1].push(Some(self.sum.clone()));
        }
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::super::AggrFunction;
    use super::*;

    use milevadb_query_datatype::FieldTypeAccessor;
    use fidel_timeshare_helper::ExprDefBuilder;

    use crate::parser::AggrDefinitionParser;
    use milevadb_query_datatype::codec::batch::{LazyBatchPrimaryCauset, LazyBatchPrimaryCausetVec};

    #[test]
    fn test_fidelio() {
        let mut ctx = EvalContext::default();
        let function = AggrFnAvg::<Real>::new();
        let mut state = function.create_state();

        let mut result = [
            VectorValue::with_capacity(0, EvalType::Int),
            VectorValue::with_capacity(0, EvalType::Real),
        ];
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);
        assert_eq!(result[1].to_real_vec(), &[None]);

        fidelio!(state, &mut ctx, Option::<&Real>::None).unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0), Some(0)]);
        assert_eq!(result[1].to_real_vec(), &[None, None]);

        fidelio!(state, &mut ctx, Real::new(5.0).ok().as_ref()).unwrap();
        fidelio!(state, &mut ctx, Option::<&Real>::None).unwrap();
        fidelio!(state, &mut ctx, Real::new(10.0).ok().as_ref()).unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0), Some(0), Some(2)]);
        assert_eq!(result[1].to_real_vec(), &[None, None, Real::new(15.0).ok()]);

        let x: SolitonedVecSized<Real> = vec![Real::new(0.0).ok(), Real::new(-4.5).ok(), None].into();

        fidelio_vector!(state, &mut ctx, &x, &[0, 1, 2]).unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(
            result[0].to_int_vec(),
            &[Some(0), Some(0), Some(2), Some(4)]
        );
        assert_eq!(
            result[1].to_real_vec(),
            &[None, None, Real::new(15.0).ok(), Real::new(10.5).ok()]
        );
    }

    /// AVG(IntPrimaryCauset) should produce (Int, Decimal).
    #[test]
    fn test_integration() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::NewDecimal)
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong))
            .build();
        AggrFnDefinitionParserAvg.check_supported(&expr).unwrap();

        let src_schemaReplicant = [FieldTypeTp::LongLong.into()];
        let mut PrimaryCausets = LazyBatchPrimaryCausetVec::from(vec![{
            let mut col = LazyBatchPrimaryCauset::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(Some(100));
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(42));
            col.mut_decoded().push_int(None);
            col
        }]);

        let mut schemaReplicant = vec![];
        let mut exp = vec![];

        let mut ctx = EvalContext::default();
        let aggr_fn = AggrFnDefinitionParserAvg
            .parse(expr, &mut ctx, &src_schemaReplicant, &mut schemaReplicant, &mut exp)
            .unwrap();
        assert_eq!(schemaReplicant.len(), 2);
        assert_eq!(schemaReplicant[0].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(schemaReplicant[1].as_accessor().tp(), FieldTypeTp::NewDecimal);

        assert_eq!(exp.len(), 1);

        let mut state = aggr_fn.create_state();
        let mut ctx = EvalContext::default();

        let exp_result = exp[0]
            .eval(&mut ctx, &src_schemaReplicant, &mut PrimaryCausets, &[4, 1, 2, 3], 4)
            .unwrap();
        let exp_result = exp_result.vector_value().unwrap();
        let slice = exp_result.as_ref().to_decimal_vec();
        let slice: SolitonedVecSized<Decimal> = slice.into();
        fidelio_vector!(state, &mut ctx, &slice, exp_result.logical_rows()).unwrap();

        let mut aggr_result = [
            VectorValue::with_capacity(0, EvalType::Int),
            VectorValue::with_capacity(0, EvalType::Decimal),
        ];
        state.push_result(&mut ctx, &mut aggr_result).unwrap();

        assert_eq!(aggr_result[0].to_int_vec(), &[Some(2)]);
        assert_eq!(
            aggr_result[1].to_decimal_vec(),
            &[Some(Decimal::from(43u64))]
        );
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double) // Expect NewDecimal but give Real
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong)) // FIXME: This type can be incorrect as well
            .build();
        AggrFnDefinitionParserAvg.check_supported(&expr).unwrap();

        let src_schemaReplicant = [FieldTypeTp::LongLong.into()];
        let mut schemaReplicant = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserAvg
            .parse(expr, &mut ctx, &src_schemaReplicant, &mut schemaReplicant, &mut exp)
            .unwrap_err();
    }
}
