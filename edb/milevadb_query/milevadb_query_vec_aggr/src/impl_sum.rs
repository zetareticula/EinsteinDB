// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use milevadb_query_codegen::AggrFunction;
use milevadb_query_datatype::EvalType;
use fidel_timeshare::{Expr, ExprType, FieldType};

use super::summable::Summable;
use super::*;
use milevadb_query_common::Result;
use milevadb_query_datatype::codec::data_type::*;
use milevadb_query_datatype::expr::EvalContext;
use milevadb_query_vec_expr::RpnExpression;

/// The parser for SUM aggregate function.
pub struct AggrFnDefinitionParserSum;

impl super::parser::AggrDefinitionParser for AggrFnDefinitionParserSum {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Sum);
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

        assert_eq!(root_expr.get_tp(), ExprType::Sum);

        let out_ft = root_expr.take_field_type();
        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));

        // The rewrite should always success.
        super::util::rewrite_exp_for_sum_avg(src_schemaReplicant, &mut exp).unwrap();

        let rewritten_eval_type =
            EvalType::try_from(exp.ret_field_type(src_schemaReplicant).as_accessor().tp()).unwrap();
        if out_et != rewritten_eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // SUM outputs one PrimaryCauset.
        out_schemaReplicant.push(out_ft);
        out_exp.push(exp);

        // Choose a type-aware SUM implementation based on the eval type after rewriting exp.
        Ok(match rewritten_eval_type {
            EvalType::Decimal => Box::new(AggrFnSum::<Decimal>::new()),
            EvalType::Real => Box::new(AggrFnSum::<Real>::new()),
            // If we meet unexpected types after rewriting, it is an implementation fault.
            _ => unreachable!(),
        })
    }
}

/// The SUM aggregate function.
///
/// Note that there are `SUM(Decimal) -> Decimal` and `SUM(Double) -> Double`.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateSum::<T>::new())]
pub struct AggrFnSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<T>,
}

impl<T> AggrFnSum<T>
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

/// The state of the SUM aggregate function.
#[derive(Debug)]
pub struct AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    sum: T,
    has_value: bool,
}

impl<T> AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            sum: T::zero(),
            has_value: false,
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
                self.has_value = true;
                Ok(())
            }
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = &'static T;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        if !self.has_value {
            target[0].push(None);
        } else {
            target[0].push(Some(self.sum.clone()));
        }
        Ok(())
    }
}

#[causet(test)]
mod tests {
    use super::*;

    use milevadb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use fidel_timeshare_helper::ExprDefBuilder;

    use crate::parser::AggrDefinitionParser;
    use milevadb_query_datatype::codec::batch::{LazyBatchPrimaryCauset, LazyBatchPrimaryCausetVec};

    /// SUM(Bytes) should produce (Real).
    #[test]
    fn test_integration() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Sum, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::VarString))
            .build();
        AggrFnDefinitionParserSum.check_supported(&expr).unwrap();

        let src_schemaReplicant = [FieldTypeTp::VarString.into()];
        let mut PrimaryCausets = LazyBatchPrimaryCausetVec::from(vec![{
            let mut col = LazyBatchPrimaryCauset::decoded_with_capacity_and_tp(0, EvalType::Bytes);
            col.mut_decoded().push_bytes(Some(b"12.5".to_vec()));
            col.mut_decoded().push_bytes(None);
            col.mut_decoded().push_bytes(Some(b"10000.0".to_vec()));
            col.mut_decoded().push_bytes(Some(b"42.0".to_vec()));
            col.mut_decoded().push_bytes(None);
            col
        }]);
        let logical_rows = vec![0, 1, 3, 4];

        let mut schemaReplicant = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();

        let aggr_fn = AggrFnDefinitionParserSum
            .parse(expr, &mut ctx, &src_schemaReplicant, &mut schemaReplicant, &mut exp)
            .unwrap();
        assert_eq!(schemaReplicant.len(), 1);
        assert_eq!(schemaReplicant[0].as_accessor().tp(), FieldTypeTp::Double);

        assert_eq!(exp.len(), 1);

        let mut state = aggr_fn.create_state();

        let exp_result = exp[0]
            .eval(&mut ctx, &src_schemaReplicant, &mut PrimaryCausets, &logical_rows, 4)
            .unwrap();
        let exp_result = exp_result.vector_value().unwrap();
        let vec = exp_result.as_ref().to_real_vec();
        let Solitoned_vec: SolitonedVecSized<Real> = vec.into();
        fidelio_vector!(state, &mut ctx, &Solitoned_vec, exp_result.logical_rows()).unwrap();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Real)];
        state.push_result(&mut ctx, &mut aggr_result).unwrap();

        assert_eq!(aggr_result[0].to_real_vec(), &[Real::new(54.5).ok()]);
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Sum, FieldTypeTp::Double) // Expect NewDecimal but give Double
            .push_child(ExprDefBuilder::PrimaryCauset_ref(0, FieldTypeTp::LongLong)) // FIXME: This type can be incorrect as well
            .build();
        AggrFnDefinitionParserSum.check_supported(&expr).unwrap();

        let src_schemaReplicant = [FieldTypeTp::LongLong.into()];
        let mut schemaReplicant = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserSum
            .parse(expr, &mut ctx, &src_schemaReplicant, &mut schemaReplicant, &mut exp)
            .unwrap_err();
    }
}
