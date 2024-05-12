// Copyright 2019 WHTCORPS INC Project Authors. Licensed under Apache-2.0.

use fidel_timeshare::{Expr, ExprType, FieldType};

use crate::impl_bit_op::*;
use crate::impl_max_min::*;
use crate::AggrFunction;
use milevadb_query_common::Result;
use milevadb_query_datatype::expr::EvalContext;
use milevadb_query_vec_expr::{RpnExpression, RpnExpressionBuilder};

/// Parse a specific aggregate function definition from protobuf.
///
/// All aggregate function implementations should include an impl for this trait as well as
/// add a match arm in `map__timeshare_sig_to_aggr_func_parser` so that the aggregate function can be
/// actually utilized.
pub trait AggrDefinitionParser {
    /// Checks whether the inner expression of the aggregate function definition is supported.
    /// It is ensured that `aggr_def.tp` maps the current parser instance.
    fn check_supported(&self, aggr_def: &Expr) -> Result<()>;

    /// Parses and transforms the aggregate function definition.
    ///
    /// The schemaReplicant of this aggregate function will be applightlikeed in `out_schemaReplicant` and the final
    /// RPN expression (maybe wrapped by some casting according to types) will be applightlikeed in
    /// `out_exp`.
    ///
    /// The parser may choose particular aggregate function implementation based on the data
    /// type, so `schemaReplicant` is also needed in case of data type deplightlikeing on the PrimaryCauset.
    ///
    /// # Panic
    ///
    /// May panic if the aggregate function definition is not supported by this parser.
    fn parse(
        &self,
        mut aggr_def: Expr,
        ctx: &mut EvalContext,
        src_schemaReplicant: &[FieldType],
        out_schemaReplicant: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        // Rewrite expression to insert CAST() if needed.
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let exp = RpnExpressionBuilder::build_from_expr_tree(child, ctx, src_schemaReplicant.len())?;

        Self::parse_rpn(&self, aggr_def, exp, ctx, src_schemaReplicant, out_schemaReplicant, out_exp)
    }

    #[inline]
    fn parse_rpn(
        &self,
        _root_expr: Expr,
        _exp: RpnExpression,
        _ctx: &mut EvalContext,
        _src_schemaReplicant: &[FieldType],
        _out_schemaReplicant: &mut Vec<FieldType>,
        _out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        unimplemented!(
            "This struct neither implemented parse nor parse_rpn, which is not expected."
        )
    }
}

#[inline]
fn map__timeshare_sig_to_aggr_func_parser(value: ExprType) -> Result<Box<dyn AggrDefinitionParser>> {
    match value {
        ExprType::Count => Ok(Box::new(super::impl_count::AggrFnDefinitionParserCount)),
        ExprType::Sum => Ok(Box::new(super::impl_sum::AggrFnDefinitionParserSum)),
        ExprType::Avg => Ok(Box::new(super::impl_avg::AggrFnDefinitionParserAvg)),
        ExprType::First => Ok(Box::new(super::impl_first::AggrFnDefinitionParserFirst)),
        ExprType::AggBitAnd => Ok(Box::new(AggrFnDefinitionParserBitOp::<BitAnd>::new())),
        ExprType::AggBitOr => Ok(Box::new(AggrFnDefinitionParserBitOp::<BitOr>::new())),
        ExprType::AggBitXor => Ok(Box::new(AggrFnDefinitionParserBitOp::<BitXor>::new())),
        ExprType::Max => Ok(Box::new(AggrFnDefinitionParserExtremum::<Max>::new())),
        ExprType::Min => Ok(Box::new(AggrFnDefinitionParserExtremum::<Min>::new())),
        v => Err(other_err!(
            "Aggregation function meet blacklist aggr function {:?}",
            v
        )),
    }
}

/// Parse all aggregate function definition from protobuf.
pub struct AllAggrDefinitionParser;

impl AggrDefinitionParser for AllAggrDefinitionParser {
    /// Checks whether the aggregate function definition is supported.
    #[inline]
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        let parser = map__timeshare_sig_to_aggr_func_parser(aggr_def.get_tp())?;
        parser.check_supported(aggr_def).map_err(|e| {
            other_err!(
                "Aggregation function meet blacklist expr type {:?}: {}",
                aggr_def.get_tp(),
                e
            )
        })
    }

    /// Parses and transforms the aggregate function definition to generate corresponding
    /// `AggrFunction` instance.
    ///
    /// # Panic
    ///
    /// May panic if the aggregate function definition is not supported.
    #[inline]
    fn parse(
        &self,
        aggr_def: Expr,
        ctx: &mut EvalContext,
        src_schemaReplicant: &[FieldType],
        out_schemaReplicant: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        let parser = map__timeshare_sig_to_aggr_func_parser(aggr_def.get_tp()).unwrap();
        parser.parse(aggr_def, ctx, src_schemaReplicant, out_schemaReplicant, out_exp)
    }
}
