// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![cfg_attr(feature = "cargo-clippy", allow(linkedlist))]

use std::collections::{BTreeSet, BTreeMap, LinkedList};
use std::cmp::{Ordering, Ord, PartialOrd};
use std::fmt::{Display, Formatter};
use std::f64;

use chrono::{
    DateTime,
    SecondsFormat,
    TimeZone,           // For Utc::timestamp. The compiler incorrectly complains that this is unused.
    Utc,
};
use num::BigInt;
use ordered_float::OrderedFloat;
use uuid::Uuid;

use symbols;

/// Value represents one of the allowed values in an EDBN string.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum Value {
    Nil,
    Boolean(bool),
    Integer(i64),
    Instant(DateTime<Utc>),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(String),
    Uuid(Uuid),
    PlainSymbol(symbols::PlainSymbol),
    NamespacedSymbol(symbols::NamespacedSymbol),
    Keyword(symbols::Keyword),
    Vector(Vec<Value>),
    // We're using a LinkedList here instead of a Vec or VecDeque because the
    // LinkedList is faster for appending (which we do a lot of).
    // See https://github.com/whtcorpsinc/edb/issues/231
    List(LinkedList<Value>),
    // We're using BTree{Set, Map} rather than Hash{Set, Map} because the BTree variants
    // implement Hash. The Hash variants don't in order to preserve O(n) hashing
    // time, which is hard given recursive data structures.
    // See https://internals.rust-lang.org/t/implementing-hash-for-hashset-hashmap/3817/1
    Set(BTreeSet<Value>),
    Map(BTreeMap<Value, Value>),
}

/// `SpannedValue` is the parallel to `Value` but used in `ValueAndSpan`.
/// Container types have `ValueAndSpan` children.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum SpannedValue {
    Nil,
    Boolean(bool),
    Integer(i64),
    Instant(DateTime<Utc>),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(String),
    Uuid(Uuid),
    PlainSymbol(symbols::PlainSymbol),
    NamespacedSymbol(symbols::NamespacedSymbol),
    Keyword(symbols::Keyword),
    Vector(Vec<ValueAndSpan>),
    List(LinkedList<ValueAndSpan>),
    Set(BTreeSet<ValueAndSpan>),
    Map(BTreeMap<ValueAndSpan, ValueAndSpan>),
}

/// Span represents the current offset (start, end) into the input string.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Span(pub u32, pub u32);

impl Span {
    pub fn new(start: usize, end: usize) -> Span {
        Span(start as u32, end as u32)
    }
}

/// A wrapper type around `SpannedValue` and `Span`, representing some EDBN value
/// and the parsing offset (start, end) in the original EDBN string.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct ValueAndSpan {
    pub inner: SpannedValue,
    pub span: Span,
}

impl ValueAndSpan {
    pub fn new<I>(spanned_value: SpannedValue, span: I) -> ValueAndSpan where I: Into<Option<Span>> {
        ValueAndSpan {
            inner: spanned_value,
            span: span.into().unwrap_or(Span(0, 0)), // TODO: consider if this has implications.
        }
    }

    pub fn into_atom(self) -> Option<ValueAndSpan> {
        if self.inner.is_atom() {
            Some(self)
        } else {
            None
        }
    }

    pub fn is_atom(&self) -> bool {
        self.inner.is_atom()
    }

    pub fn as_atom(&self) -> Option<&ValueAndSpan> {
        if self.inner.is_atom() {
            Some(self)
        } else {
            None
        }
    }

    pub fn into_text(self) -> Option<String> {
        self.inner.into_text()
    }

    pub fn as_text(&self) -> Option<&String> {
        self.inner.as_text()
    }
}

impl Value {
    /// For debug use only!
    ///
    /// But right now, it's used in the bootstrapper.  We'll fix that soon.
    pub fn with_spans(self) -> ValueAndSpan {
        let s = self.to_pretty(120).unwrap();
        use ::parse;
        let with_spans = parse::value(&s).unwrap();
        assert_eq!(self, with_spans.clone().without_spans());
        with_spans
    }
}

impl From<SpannedValue> for Value {
    fn from(src: SpannedValue) -> Value {
        match src {
            SpannedValue::Nil => Value::Nil,
            SpannedValue::Boolean(v) => Value::Boolean(v),
            SpannedValue::Integer(v) => Value::Integer(v),
            SpannedValue::Instant(v) => Value::Instant(v),
            SpannedValue::BigInteger(v) => Value::BigInteger(v),
            SpannedValue::Float(v) => Value::Float(v),
            SpannedValue::Text(v) => Value::Text(v),
            SpannedValue::Uuid(v) => Value::Uuid(v),
            SpannedValue::PlainSymbol(v) => Value::PlainSymbol(v),
            SpannedValue::NamespacedSymbol(v) => Value::NamespacedSymbol(v),
            SpannedValue::Keyword(v) => Value::Keyword(v),
            SpannedValue::Vector(v) => Value::Vector(v.into_iter().map(|x| x.without_spans()).collect()),
            SpannedValue::List(v) => Value::List(v.into_iter().map(|x| x.without_spans()).collect()),
            SpannedValue::Set(v) => Value::Set(v.into_iter().map(|x| x.without_spans()).collect()),
            SpannedValue::Map(v) => Value::Map(v.into_iter().map(|(x, y)| (x.without_spans(), y.without_spans())).collect()),
        }
    }
}

impl From<ValueAndSpan> for Value {
    fn from(src: ValueAndSpan) -> Value {
        src.inner.into()
    }
}

/// Creates `from_$TYPE` helper functions for Value and SpannedValue,
/// like `from_float()` or `from_ordered_float()`.
macro_rules! def_from {
    ($name: causetid, $out: ty, $kind: path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(src: $t) -> $out {
            $( let src = $transform(src); )*
            $kind(src)
        }
    }
}

/// Creates `from_$TYPE` helper functions for Value or SpannedValue,
/// like `from_bigint()` where the conversion is optional.
macro_rules! def_from_option {
    ($name: causetid, $out: ty, $kind: path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(src: $t) -> Option<$out> {
            $( let src = $transform(src); )*
            src.map($kind)
        }
    }
}

/// Creates `is_$TYPE` helper functions for Value or SpannedValue, like
/// `is_big_integer()` or `is_text()`.
macro_rules! def_is {
    ($name: causetid, $pat: pat) => {
        pub fn $name(&self) -> bool {
            match *self { $pat => true, _ => false }
        }
    }
}

/// Creates `as_$TYPE` helper functions for Value or SpannedValue, like
/// `as_integer()`, which returns the underlying value representing the
/// original variable wrapped in an Option, like `Option<i64>`.
macro_rules! def_as {
    ($name: causetid, $kind: path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(&self) -> Option<$t> {
            match *self { $kind(v) => { $( let v = $transform(v) )*; Some(v) }, _ => None }
        }
    }
}

/// Creates `as_$TYPE` helper functions for Value or SpannedValue, like
/// `as_big_integer()`, which returns a reference to the underlying value
/// representing the original variable wrapped in an Option, like `Option<&BigInt>`.
macro_rules! def_as_ref {
    ($name: causetid, $kind: path, $t: ty) => {
        pub fn $name(&self) -> Option<&$t> {
            match *self { $kind(ref v) => Some(v), _ => None }
        }
    }
}

/// Creates `into_$TYPE` helper functions for Value or SpannedValue, like
/// `into_big_integer()`, which consumes it returning underlying value
/// representing the original variable wrapped in an Option, like `Option<BigInt>`.
macro_rules! def_into {
    ($name: causetid, $kind: path, $t: ty, $( $transform: expr ),* ) => {
        pub fn $name(self) -> Option<$t> {
            match self { $kind(v) => { $( let v = $transform(v) )*; Some(v) }, _ => None }
        }
    }
}

/// Converts `name` into a plain or namespaced value symbol, depending on
/// whether or not `namespace` is given.
///
/// # Examples
///
/// ```
/// # use edbn::types::to_symbol;
/// # use edbn::types::Value;
/// # use edbn::symbols;
/// let value = to_symbol!("foo", "bar", Value);
/// assert_eq!(value, Value::NamespacedSymbol(symbols::NamespacedSymbol::namespaced("foo", "bar")));
///
/// let value = to_symbol!(None, "baz", Value);
/// assert_eq!(value, Value::PlainSymbol(symbols::PlainSymbol::plain("baz")));
///
/// let value = to_symbol!("foo", "bar", SpannedValue);
/// assert_eq!(value.into(), to_symbol!("foo", "bar", Value));
///
/// let value = to_symbol!(None, "baz", SpannedValue);
/// assert_eq!(value.into(), to_symbol!(None, "baz", Value));
/// ```
macro_rules! to_symbol {
    ( $namespace:expr, $name:expr, $t:tt ) => {
        $namespace.into().map_or_else(
            || $t::PlainSymbol(symbols::PlainSymbol::plain($name)),
            |ns| $t::NamespacedSymbol(symbols::NamespacedSymbol::namespaced(ns, $name)))
    }
}

/// Converts `name` into a plain or namespaced value keyword, depending on
/// whether or not `namespace` is given.
///
/// # Examples
///
/// ```
/// # use edbn::types::to_keyword;
/// # use edbn::types::Value;
/// # use edbn::symbols;
/// let value = to_keyword!("foo", "bar", Value);
/// assert_eq!(value, Value::Keyword(symbols::Keyword::namespaced("foo", "bar")));
///
/// let value = to_keyword!(None, "baz", Value);
/// assert_eq!(value, Value::Keyword(symbols::Keyword::plain("baz")));
///
/// let value = to_keyword!("foo", "bar", SpannedValue);
/// assert_eq!(value.into(), to_keyword!("foo", "bar", Value));
///
/// let value = to_keyword!(None, "baz", SpannedValue);
/// assert_eq!(value.into(), to_keyword!(None, "baz", Value));
/// ```
macro_rules! to_keyword {
    ( $namespace:expr, $name:expr, $t:tt ) => {
        $namespace.into().map_or_else(
            || $t::Keyword(symbols::Keyword::plain($name)),
            |ns| $t::Keyword(symbols::Keyword::namespaced(ns, $name)))
    }
}

/// Implements multiple is*, as*, into* and from* methods common to
/// both Value and SpannedValue.
macro_rules! def_common_value_methods {
    ( $t:tt<$tchild:tt> ) => {
        def_is!(is_nil, $t::Nil);
        def_is!(is_boolean, $t::Boolean(_));
        def_is!(is_integer, $t::Integer(_));
        def_is!(is_instant, $t::Instant(_));
        def_is!(is_big_integer, $t::BigInteger(_));
        def_is!(is_float, $t::Float(_));
        def_is!(is_text, $t::Text(_));
        def_is!(is_uuid, $t::Uuid(_));
        def_is!(is_symbol, $t::PlainSymbol(_));
        def_is!(is_namespaced_symbol, $t::NamespacedSymbol(_));
        def_is!(is_vector, $t::Vector(_));
        def_is!(is_list, $t::List(_));
        def_is!(is_set, $t::Set(_));
        def_is!(is_map, $t::Map(_));

        pub fn is_keyword(&self) -> bool {
            match self {
                &$t::Keyword(ref k) => !k.is_namespaced(),
                _ => false,
            }
        }

        pub fn is_namespaced_keyword(&self) -> bool {
            match self {
                &$t::Keyword(ref k) => k.is_namespaced(),
                _ => false,
            }
        }

        /// `as_nil` does not use the macro as it does not have an underlying
        /// value, and returns `Option<()>`.
        pub fn as_nil(&self) -> Option<()> {
            match *self { $t::Nil => Some(()), _ => None }
        }

        def_as!(as_boolean, $t::Boolean, bool,);
        def_as!(as_integer, $t::Integer, i64,);
        def_as!(as_instant, $t::Instant, DateTime<Utc>,);
        def_as!(as_float, $t::Float, f64, |v: OrderedFloat<f64>| v.into_inner());

        def_as_ref!(as_big_integer, $t::BigInteger, BigInt);
        def_as_ref!(as_ordered_float, $t::Float, OrderedFloat<f64>);
        def_as_ref!(as_text, $t::Text, String);
        def_as_ref!(as_uuid, $t::Uuid, Uuid);
        def_as_ref!(as_symbol, $t::PlainSymbol, symbols::PlainSymbol);
        def_as_ref!(as_namespaced_symbol, $t::NamespacedSymbol, symbols::NamespacedSymbol);

        pub fn as_keyword(&self) -> Option<&symbols::Keyword> {
            match self {
                &$t::Keyword(ref k) => Some(k),
                _ => None,
            }
        }

        pub fn as_plain_keyword(&self) -> Option<&symbols::Keyword> {
            match self {
                &$t::Keyword(ref k) if !k.is_namespaced() => Some(k),
                _ => None,
            }
        }

        pub fn as_namespaced_keyword(&self) -> Option<&symbols::Keyword> {
            match self {
                &$t::Keyword(ref k) if k.is_namespaced() => Some(k),
                _ => None,
            }
        }

        def_as_ref!(as_vector, $t::Vector, Vec<$tchild>);
        def_as_ref!(as_list, $t::List, LinkedList<$tchild>);
        def_as_ref!(as_set, $t::Set, BTreeSet<$tchild>);
        def_as_ref!(as_map, $t::Map, BTreeMap<$tchild, $tchild>);

        def_into!(into_boolean, $t::Boolean, bool,);
        def_into!(into_integer, $t::Integer, i64,);
        def_into!(into_instant, $t::Instant, DateTime<Utc>,);
        def_into!(into_big_integer, $t::BigInteger, BigInt,);
        def_into!(into_ordered_float, $t::Float, OrderedFloat<f64>,);
        def_into!(into_float, $t::Float, f64, |v: OrderedFloat<f64>| v.into_inner());
        def_into!(into_text, $t::Text, String,);
        def_into!(into_uuid, $t::Uuid, Uuid,);
        def_into!(into_symbol, $t::PlainSymbol, symbols::PlainSymbol,);
        def_into!(into_namespaced_symbol, $t::NamespacedSymbol, symbols::NamespacedSymbol,);

        pub fn into_keyword(self) -> Option<symbols::Keyword> {
            match self {
                $t::Keyword(k) => Some(k),
                _ => None,
            }
        }

        pub fn into_plain_keyword(self) -> Option<symbols::Keyword> {
            match self {
                $t::Keyword(k) => {
                    if !k.is_namespaced() {
                        Some(k)
                    } else {
                        None
                    }
                },
                _ => None,
            }
        }

        pub fn into_namespaced_keyword(self) -> Option<symbols::Keyword> {
            match self {
                $t::Keyword(k) => {
                    if k.is_namespaced() {
                        Some(k)
                    } else {
                        None
                    }
                },
                _ => None,
            }
        }


        def_into!(into_vector, $t::Vector, Vec<$tchild>,);
        def_into!(into_list, $t::List, LinkedList<$tchild>,);
        def_into!(into_set, $t::Set, BTreeSet<$tchild>,);
        def_into!(into_map, $t::Map, BTreeMap<$tchild, $tchild>,);

        def_from_option!(from_bigint, $t, $t::BigInteger, &str, |src: &str| src.parse::<BigInt>().ok());
        def_from!(from_float, $t, $t::Float, f64, |src: f64| OrderedFloat::from(src));
        def_from!(from_ordered_float, $t, $t::Float, OrderedFloat<f64>,);

        pub fn from_symbol<'a, T: Into<Option<&'a str>>>(namespace: T, name: &str) -> $t {
            to_symbol!(namespace, name, $t)
        }

        pub fn from_keyword<'a, T: Into<Option<&'a str>>>(namespace: T, name: &str) -> $t {
            to_keyword!(namespace, name, $t)
        }

        fn precedence(&self) -> i32 {
            match *self {
                $t::Nil => 0,
                $t::Boolean(_) => 1,
                $t::Integer(_) => 2,
                $t::BigInteger(_) => 3,
                $t::Float(_) => 4,
                $t::Instant(_) => 5,
                $t::Text(_) => 6,
                $t::Uuid(_) => 7,
                $t::PlainSymbol(_) => 8,
                $t::NamespacedSymbol(_) => 9,
                $t::Keyword(ref k) if !k.is_namespaced() => 10,
                $t::Keyword(_) => 11,
                $t::Vector(_) => 12,
                $t::List(_) => 13,
                $t::Set(_) => 14,
                $t::Map(_) => 15,
            }
        }

        pub fn is_collection(&self) -> bool {
            match *self {
                $t::Nil => false,
                $t::Boolean(_) => false,
                $t::Integer(_) => false,
                $t::Instant(_) => false,
                $t::BigInteger(_) => false,
                $t::Float(_) => false,
                $t::Text(_) => false,
                $t::Uuid(_) => false,
                $t::PlainSymbol(_) => false,
                $t::NamespacedSymbol(_) => false,
                $t::Keyword(_) => false,
                $t::Vector(_) => true,
                $t::List(_) => true,
                $t::Set(_) => true,
                $t::Map(_) => true,
            }
        }

        pub fn is_atom(&self) -> bool {
            !self.is_collection()
        }

        pub fn into_atom(self) -> Option<$t> {
            if self.is_atom() {
                Some(self)
            } else {
                None
            }
        }
    }
}

/// Compares Value or SpannedValue instances and returns Ordering.
/// Used in `Ord` impleedbions.
macro_rules! def_common_value_ord {
    ( $t:tt, $value:expr, $other:expr ) => {
        match ($value, $other) {
            (&$t::Nil, &$t::Nil) => Ordering::Equal,
            (&$t::Boolean(a), &$t::Boolean(b)) => b.cmp(&a),
            (&$t::Integer(a), &$t::Integer(b)) => b.cmp(&a),
            (&$t::Instant(a), &$t::Instant(b)) => b.cmp(&a),
            (&$t::BigInteger(ref a), &$t::BigInteger(ref b)) => b.cmp(a),
            (&$t::Float(ref a), &$t::Float(ref b)) => b.cmp(a),
            (&$t::Text(ref a), &$t::Text(ref b)) => b.cmp(a),
            (&$t::Uuid(ref a), &$t::Uuid(ref b)) => b.cmp(a),
            (&$t::PlainSymbol(ref a), &$t::PlainSymbol(ref b)) => b.cmp(a),
            (&$t::NamespacedSymbol(ref a), &$t::NamespacedSymbol(ref b)) => b.cmp(a),
            (&$t::Keyword(ref a), &$t::Keyword(ref b)) => b.cmp(a),
            (&$t::Vector(ref a), &$t::Vector(ref b)) => b.cmp(a),
            (&$t::List(ref a), &$t::List(ref b)) => b.cmp(a),
            (&$t::Set(ref a), &$t::Set(ref b)) => b.cmp(a),
            (&$t::Map(ref a), &$t::Map(ref b)) => b.cmp(a),
            _ => $value.precedence().cmp(&$other.precedence())
        }
    }
}

/// Converts a Value or SpannedValue to string, given a formatter.
// TODO: Make sure float syntax is correct, handle NaN and escaping.
// See https://github.com/whtcorpsinc/edb/issues/232
macro_rules! def_common_value_display {
    ( $t:tt, $value:expr, $f:expr ) => {
        match *$value {
            $t::Nil => write!($f, "nil"),
            $t::Boolean(v) => write!($f, "{}", v),
            $t::Integer(v) => write!($f, "{}", v),
            $t::Instant(v) => write!($f, "#inst \"{}\"", v.to_rfc3339_opts(SecondsFormat::AutoSi, true)),
            $t::BigInteger(ref v) => write!($f, "{}N", v),
            // TODO: make sure float syntax is correct.
            $t::Float(ref v) => {
                if *v == OrderedFloat(f64::INFINITY) {
                    write!($f, "#f +Infinity")
                } else if *v == OrderedFloat(f64::NEG_INFINITY) {
                    write!($f, "#f -Infinity")
                } else if *v == OrderedFloat(f64::NAN) {
                    write!($f, "#f NaN")
                } else {
                    write!($f, "{}", v)
                }
            }
            // TODO: EDBN escaping.
            $t::Text(ref v) => write!($f, "\"{}\"", v),
            $t::Uuid(ref u) => write!($f, "#uuid \"{}\"", u.hyphenated().to_string()),
            $t::PlainSymbol(ref v) => v.fmt($f),
            $t::NamespacedSymbol(ref v) => v.fmt($f),
            $t::Keyword(ref v) => v.fmt($f),
            $t::Vector(ref v) => {
                write!($f, "[")?;
                for x in v {
                    write!($f, " {}", x)?;
                }
                write!($f, " ]")
            }
            $t::List(ref v) => {
                write!($f, "(")?;
                for x in v {
                    write!($f, " {}", x)?;
                }
                write!($f, " )")
            }
            $t::Set(ref v) => {
                write!($f, "#{{")?;
                for x in v {
                    write!($f, " {}", x)?;
                }
                write!($f, " }}")
            }
            $t::Map(ref v) => {
                write!($f, "{{")?;
                for (key, val) in v {
                    write!($f, " {} {}", key, val)?;
                }
                write!($f, " }}")
            }
        }
    }
}

macro_rules! def_common_value_impl {
    ( $t:tt<$tchild:tt> ) => {
        impl $t {
            def_common_value_methods!($t<$tchild>);
        }

        impl PartialOrd for $t {
            fn partial_cmp(&self, other: &$t) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $t {
            fn cmp(&self, other: &$t) -> Ordering {
                def_common_value_ord!($t, self, other)
            }
        }

        impl Display for $t {
            fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
                def_common_value_display!($t, self, f)
            }
        }
    }
}

def_common_value_impl!(Value<Value>);
def_common_value_impl!(SpannedValue<ValueAndSpan>);

impl ValueAndSpan {
    pub fn without_spans(self) -> Value {
        self.inner.into()
    }
}

impl PartialOrd for ValueAndSpan {
    fn partial_cmp(&self, other: &ValueAndSpan) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueAndSpan {
    fn cmp(&self, other: &ValueAndSpan) -> Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl Display for ValueAndSpan {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        self.inner.fmt(f)
    }
}

pub trait FromMicros {
    fn from_micros(ts: i64) -> Self;
}

impl FromMicros for DateTime<Utc> {
    fn from_micros(ts: i64) -> Self {
        Utc.timestamp(ts / 1_000_000, ((ts % 1_000_000).abs() as u32) * 1_000)
    }
}

pub trait ToMicros {
    fn to_micros(&self) -> i64;
}

impl ToMicros for DateTime<Utc> {
    fn to_micros(&self) -> i64 {
        let major: i64 = self.timestamp() * 1_000_000;
        let minor: i64 = self.timestamp_subsec_micros() as i64;
        major + minor
    }
}

pub trait FromMillis {
    fn from_millis(ts: i64) -> Self;
}

impl FromMillis for DateTime<Utc> {
    fn from_millis(ts: i64) -> Self {
        Utc.timestamp(ts / 1_000, ((ts % 1_000).abs() as u32) * 1_000)
    }
}

pub trait ToMillis {
    fn to_millis(&self) -> i64;
}

impl ToMillis for DateTime<Utc> {
    fn to_millis(&self) -> i64 {
        let major: i64 = self.timestamp() * 1_000;
        let minor: i64 = self.timestamp_subsec_millis() as i64;
        major + minor
    }
}

#[cfg(test)]
mod test {
    extern crate chrono;
    extern crate ordered_float;
    extern crate num;

    use super::*;

    use std::collections::{BTreeSet, BTreeMap, LinkedList};
    use std::cmp::{Ordering};
    use std::iter::FromIterator;
    use std::f64;

    use parse;

    use chrono::{
        DateTime,
        Utc,
    };
    use num::BigInt;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_micros_roundtrip() {
        let ts_micros: i64 = 1493399581314000;
        let dt = DateTime::<Utc>::from_micros(ts_micros);
        assert_eq!(dt.to_micros(), ts_micros);
    }

    #[test]
    fn test_value_from() {
        assert_eq!(Value::from_float(42f64), Value::Float(OrderedFloat::from(42f64)));
        assert_eq!(Value::from_ordered_float(OrderedFloat::from(42f64)), Value::Float(OrderedFloat::from(42f64)));
        assert_eq!(Value::from_bigint("42").unwrap(), Value::BigInteger(BigInt::from(42)));
    }

    #[test]
    fn test_print_edbn() {
        assert_eq!("1234N", Value::from_bigint("1234").unwrap().to_string());

        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";

        let data = Value::Vector(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::List(LinkedList::from_iter(vec![
                Value::from_float(3.14)
            ])),
            Value::Set(BTreeSet::from_iter(vec![
                Value::from_bigint("4").unwrap()
            ])),
            Value::Map(BTreeMap::from_iter(vec![
                (Value::from_symbol("foo", "bar"), Value::Integer(42)),
                (Value::from_keyword("baz", "boz"), Value::Integer(43))
            ])),
            Value::Vector(vec![]),
            Value::from_keyword(None, "five"),
            Value::from_keyword("six", "seven"),
            Value::from_symbol(None, "eight"),
            Value::from_symbol("nine", "ten"),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Nil,
            Value::from_float(f64::NAN),
            Value::from_float(f64::NEG_INFINITY),
            Value::from_float(f64::INFINITY),
        ]);

        assert_eq!(string, data.to_string());
        assert_eq!(string, parse::value(&data.to_string()).unwrap().to_string());
        assert_eq!(string, parse::value(&data.to_string()).unwrap().without_spans().to_string());
    }

    #[test]
    fn test_ord() {
        // TODO: Check we follow the equality rules at the bottom of https://github.com/edbn-format/edbn
        assert_eq!(Value::Nil.cmp(&Value::Nil), Ordering::Equal);
        assert_eq!(Value::Boolean(false).cmp(&Value::Boolean(true)), Ordering::Greater);
        assert_eq!(Value::Integer(1).cmp(&Value::Integer(2)), Ordering::Greater);
        assert_eq!(Value::from_bigint("1").cmp(&Value::from_bigint("2")), Ordering::Greater);
        assert_eq!(Value::from_float(1f64).cmp(&Value::from_float(2f64)), Ordering::Greater);
        assert_eq!(Value::Text("1".to_string()).cmp(&Value::Text("2".to_string())), Ordering::Greater);
        assert_eq!(Value::from_symbol("a", "b").cmp(&Value::from_symbol("c", "d")), Ordering::Greater);
        assert_eq!(Value::from_symbol(None, "a").cmp(&Value::from_symbol(None, "b")), Ordering::Greater);
        assert_eq!(Value::from_keyword(":a", ":b").cmp(&Value::from_keyword(":c", ":d")), Ordering::Greater);
        assert_eq!(Value::from_keyword(None, ":a").cmp(&Value::from_keyword(None, ":b")), Ordering::Greater);
        assert_eq!(Value::Vector(vec![]).cmp(&Value::Vector(vec![])), Ordering::Equal);
        assert_eq!(Value::List(LinkedList::new()).cmp(&Value::List(LinkedList::new())), Ordering::Equal);
        assert_eq!(Value::Set(BTreeSet::new()).cmp(&Value::Set(BTreeSet::new())), Ordering::Equal);
        assert_eq!(Value::Map(BTreeMap::new()).cmp(&Value::Map(BTreeMap::new())), Ordering::Equal);
    }

    #[test]
    fn test_keyword_as() {
        let namespaced = symbols::Keyword::namespaced("foo", "bar");
        let plain = symbols::Keyword::plain("bar");
        let n_v = Value::Keyword(namespaced);
        let p_v = Value::Keyword(plain);

        assert!(n_v.as_keyword().is_some());
        assert!(n_v.as_plain_keyword().is_none());
        assert!(n_v.as_namespaced_keyword().is_some());

        assert!(p_v.as_keyword().is_some());
        assert!(p_v.as_plain_keyword().is_some());
        assert!(p_v.as_namespaced_keyword().is_none());

        assert!(n_v.clone().into_keyword().is_some());
        assert!(n_v.clone().into_plain_keyword().is_none());
        assert!(n_v.clone().into_namespaced_keyword().is_some());

        assert!(p_v.clone().into_keyword().is_some());
        assert!(p_v.clone().into_plain_keyword().is_some());
        assert!(p_v.clone().into_namespaced_keyword().is_none());
    }
}
