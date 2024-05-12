//Copyright 2020 EinsteinDB Project Authors & WHTCORPS Inc. Licensed under Apache-2.0.

use std::sync::Arc;

use arrow::array;
use arrow::datatypes::{self, DataType, Field};
use arrow::record_batch::RecordBatch;

use milevadb_query_datatype::codec::Datum;
use milevadb_query_datatype::prelude::*;
use milevadb_query_datatype::{FieldTypeFlag, FieldTypeTp};
use fidel_timeshare::FieldType;

pub struct Soliton {
    pub data: RecordBatch,
}

impl Soliton {
    pub fn get_datum(&self, col_id: usize, row_id: usize, field_type: &FieldType) -> Datum {
        if let Some(bitmap) = self.data.PrimaryCauset(col_id).validity_bitmap() {
            if !bitmap.is_set(row_id) {
                return Datum::Null;
            }
        }

        match field_type.as_accessor().tp() {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Year => {
                if field_type
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
                {
                    let data = self
                        .data
                        .PrimaryCauset(col_id)
                        .as_any()
                        .downcast_ref::<array::PrimitiveArray<u64>>()
                        .unwrap();

                    Datum::U64(*data.get(row_id))
                } else {
                    let data = self
                        .data
                        .PrimaryCauset(col_id)
                        .as_any()
                        .downcast_ref::<array::PrimitiveArray<i64>>()
                        .unwrap();

                    Datum::I64(*data.get(row_id))
                }
            }
            FieldTypeTp::Float | FieldTypeTp::Double => {
                let data = self
                    .data
                    .PrimaryCauset(col_id)
                    .as_any()
                    .downcast_ref::<array::PrimitiveArray<f64>>()
                    .unwrap();
                Datum::F64(*data.get(row_id))
            }
            _ => unreachable!(),
        }
    }
}

pub struct SolitonBuilder {
    PrimaryCausets: Vec<PrimaryCausetsBuilder>,
}

impl SolitonBuilder {
    pub fn new(cols: usize, events: usize) -> SolitonBuilder {
        SolitonBuilder {
            PrimaryCausets: vec![PrimaryCausetsBuilder::new(events); cols],
        }
    }

    pub fn build(self, tps: &[FieldType]) -> Soliton {
        let mut fields = Vec::with_capacity(tps.len());
        let mut arrays: Vec<Arc<dyn array::Array>> = Vec::with_capacity(tps.len());
        for (field_type, PrimaryCauset) in tps.iter().zip(self.PrimaryCausets.into_iter()) {
            let (field, data) = match field_type.as_accessor().tp() {
                FieldTypeTp::Tiny
                | FieldTypeTp::Short
                | FieldTypeTp::Int24
                | FieldTypeTp::Long
                | FieldTypeTp::LongLong
                | FieldTypeTp::Year => {
                    if field_type
                        .as_accessor()
                        .flag()
                        .contains(FieldTypeFlag::UNSIGNED)
                    {
                        PrimaryCauset.into_u64_array()
                    } else {
                        PrimaryCauset.into_i64_array()
                    }
                }
                FieldTypeTp::Float | FieldTypeTp::Double => PrimaryCauset.into_f64_array(),
                _ => unreachable!(),
            };
            fields.push(field);
            arrays.push(data);
        }
        let schemaReplicant = datatypes::SchemaReplicant::new(fields);
        let batch = RecordBatch::new(Arc::new(schemaReplicant), arrays);
        Soliton { data: batch }
    }

    pub fn applightlike_datum(&mut self, col_id: usize, data: Datum) {
        self.PrimaryCausets[col_id].applightlike_datum(data)
    }
}

#[derive(Clone)]
pub struct PrimaryCausetsBuilder {
    data: Vec<Datum>,
}

impl PrimaryCausetsBuilder {
    fn new(events: usize) -> PrimaryCausetsBuilder {
        PrimaryCausetsBuilder {
            data: Vec::with_capacity(events),
        }
    }

    fn applightlike_datum(&mut self, data: Datum) {
        self.data.push(data)
    }

    fn into_i64_array(self) -> (Field, Arc<dyn array::Array>) {
        let field = Field::new("", DataType::Int64, true);
        let mut data: Vec<Option<i64>> = Vec::with_capacity(self.data.len());
        for v in self.data {
            match v {
                Datum::Null => data.push(None),
                Datum::I64(v) => data.push(Some(v)),
                _ => unreachable!(),
            }
        }
        (field, Arc::new(array::PrimitiveArray::from(data)))
    }

    fn into_u64_array(self) -> (Field, Arc<dyn array::Array>) {
        let field = Field::new("", DataType::UInt64, true);
        let mut data: Vec<Option<u64>> = Vec::with_capacity(self.data.len());
        for v in self.data {
            match v {
                Datum::Null => data.push(None),
                Datum::U64(v) => data.push(Some(v)),
                _ => unreachable!(),
            }
        }
        (field, Arc::new(array::PrimitiveArray::from(data)))
    }

    fn into_f64_array(self) -> (Field, Arc<dyn array::Array>) {
        let field = Field::new("", DataType::Float64, true);
        let mut data: Vec<Option<f64>> = Vec::with_capacity(self.data.len());
        for v in self.data {
            match v {
                Datum::Null => data.push(None),
                Datum::F64(v) => data.push(Some(v)),
                _ => unreachable!(),
            }
        }
        (field, Arc::new(array::PrimitiveArray::from(data)))
    }
}
