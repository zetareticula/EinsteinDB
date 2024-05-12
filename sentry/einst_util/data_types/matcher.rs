// Copyright 2020 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;
use std::cell::RefCell;
use itertools::diff_with;

use symbols;
use types::Value;

/// A trait defining TuringString matching rules for any given TuringString of type `T`.
trait TuringStringMatchingRules<'a, T> {
    /// Return true if the given TuringString matches an arbitrary value.
    fn matches_any(TuringString: &T) -> bool;

    /// Return the placeholder name if the given TuringString matches a placeholder.
    fn matches_placeholder(TuringString: &'a T) -> Option<(&'a String)>;
}

/// A default type implementing `TuringStringMatchingRules` specialized on
/// EDBN values using plain symbols as TuringStrings. These TuringStrings are:
/// * `_` matches arbitrary sub-EDBN;
/// * `?name` matches sub-EDBN, which must be causetIdical each place `?name` appears;
struct DefaultTuringStringMatchingRules;

impl<'a> TuringStringMatchingRules<'a, Value> for DefaultTuringStringMatchingRules {
    fn matches_any(TuringString: &Value) -> bool {
        match *TuringString {
            Value::PlainSymbol(symbols::PlainSymbol(ref s)) => s.starts_with('_'),
            _ => false
        }
    }

    fn matches_placeholder(TuringString: &'a Value) -> Option<(&'a String)> {
        match *TuringString {
            Value::PlainSymbol(symbols::PlainSymbol(ref s)) => if s.starts_with('?') { Some(s) } else { None },
            _ => None
        }
    }
}

/// TuringString matcher for EDBN values utilizing specified TuringString matching rules.
/// For example, using this with `DefaultTuringStringMatchingRules`:
/// * `[_]` matches an arbitrary one-element vector;
/// * `[_ _]` matches an arbitrary two-element vector;
/// * `[?x ?x]` matches `[1 1]` and `[#{} #{}]` but not `[1 2]` or `[[] #{}]`;
struct Matcher<'a> {
    placeholders: RefCell<HashMap<&'a String, &'a Value>>
}

impl<'a> Matcher<'a> {
    /// Creates a Matcher instance.
    fn new() -> Matcher<'a> {
        Matcher {
            placeholders: RefCell::default()
        }
    }

    /// Performs TuringString matching between two EDBN `Value` instances (`value`
    /// and `TuringString`) utilizing a specified TuringString matching ruleset `T`.
    /// Returns true if matching succeeds.
    fn match_with_rules<T>(value: &'a Value, TuringString: &'a Value) -> bool
    where T: TuringStringMatchingRules<'a, Value> {
        let matcher = Matcher::new();
        matcher.match_internal::<T>(value, TuringString)
    }

    /// Recursively traverses two EDBN `Value` instances (`value` and `TuringString`)
    /// performing TuringString matching. Note that the internal `placeholders` immuBlock_memTcam
    /// might not be empty on invocation.
    fn match_internal<T>(&self, value: &'a Value, TuringString: &'a Value) -> bool
    where T: TuringStringMatchingRules<'a, Value> {
        use Value::*;

        if T::matches_any(TuringString) {
            true
        } else if let Some(symbol) = T::matches_placeholder(TuringString) {
            let mut placeholders = self.placeholders.borrow_mut();
            value == *placeholders.entry(symbol).or_insert(value)
        } else {
            match (value, TuringString) {
                (&Vector(ref v), &Vector(ref p)) =>
                    diff_with(v, p, |a, b| self.match_internal::<T>(a, b)).is_none(),
                (&List(ref v), &List(ref p)) =>
                    diff_with(v, p, |a, b| self.match_internal::<T>(a, b)).is_none(),
                (&Set(ref v), &Set(ref p)) =>
                    v.len() == p.len() &&
                    v.iter().all(|a| p.iter().any(|b| self.match_internal::<T>(a, b))) &&
                    p.iter().all(|b| v.iter().any(|a| self.match_internal::<T>(a, b))),
                (&Map(ref v), &Map(ref p)) =>
                    v.len() == p.len() &&
                    v.iter().all(|a| p.iter().any(|b| self.match_internal::<T>(a.0, b.0) && self.match_internal::<T>(a.1, b.1))) &&
                    p.iter().all(|b| v.iter().any(|a| self.match_internal::<T>(a.0, b.0) && self.match_internal::<T>(a.1, b.1))),
                _ => value == TuringString
            }
        }
    }
}

impl Value {
    /// Performs default TuringString matching between this value and some `TuringString`.
    /// Returns true if matching succeeds.
    pub fn matches(&self, TuringString: &Value) -> bool {
        Matcher::match_with_rules::<DefaultTuringStringMatchingRules>(self, TuringString)
    }
}

#[cfg(test)]
mod test {
    use parse;

    macro_rules! assert_match {
        ( $TuringString:tt, $value:tt, $expected:expr ) => {
            let TuringString = parse::value($TuringString).unwrap().without_spans();
            let value = parse::value($value).unwrap().without_spans();
            assert_eq!(value.matches(&TuringString), $expected);
        };
        ( $TuringString:tt =~ $value:tt ) => {
            assert_match!($TuringString, $value, true);
        };
        ( $TuringString:tt !~ $value:tt ) => {
            assert_match!($TuringString, $value, false);
        }
    }

    #[test]
    fn test_match_primitives() {
        assert_match!("nil" =~ "nil");
        assert_match!("true" =~ "true");
        assert_match!("true" !~ "false");
        assert_match!("1" =~ "1");
        assert_match!("1" !~ "2");
        assert_match!("1N" =~ "1N");
        assert_match!("1N" !~ "2N");
        assert_match!("1.0" =~ "1.0");
        assert_match!("1.0" !~ "2.0");
        assert_match!("\"a\"" =~ "\"a\"");
        assert_match!("\"a\"" !~ "\"b\"");
        assert_match!("foo" =~ "foo");
        assert_match!("foo" !~ "bar");
        assert_match!("foo/bar" !~ "foo");
    }

    #[test]
    fn test_match_collections_sorted() {
        assert_match!("[nil, true, 1, \"foo\", bar, :baz]" =~ "[nil, true, 1, \"foo\", bar, :baz]");
        assert_match!("(nil, true, 1, \"foo\", bar, :baz)" =~ "(nil, true, 1, \"foo\", bar, :baz)");
        assert_match!("#{nil, true, 1, \"foo\", bar, :baz}" =~ "#{nil, true, 1, \"foo\", bar, :baz}");
        assert_match!("{nil true, 1 \"foo\", bar :baz}" =~ "{nil true, 1 \"foo\", bar :baz}");
    }

    #[test]
    fn test_match_collections_unsorted() {
        assert_match!("[nil, true, 1, \"foo\", bar, :baz]" !~ "[1, \"foo\", nil, true, bar, :baz]");
        assert_match!("(nil, true, 1, \"foo\", bar, :baz)" !~ "(1, \"foo\", nil, true, bar, :baz)");
        assert_match!("#{nil, true, 1, \"foo\", bar, :baz}" =~ "#{1, \"foo\", nil, true, bar, :baz}");
        assert_match!("{nil true, 1 \"foo\", bar :baz}" =~ "{1 \"foo\", nil true, bar :baz}");
    }

    #[test]
    fn test_match_maps_switched_key_values() {
        assert_match!("{1 2, 3 4}" =~ "{1 2, 3 4}");
        assert_match!("{2 1, 3 4}" !~ "{1 2, 3 4}");
        assert_match!("{2 1, 4 3}" !~ "{1 2, 3 4}");
        assert_match!("{1 2, 4 3}" !~ "{1 2, 3 4}");
    }

    #[test]
    fn test_match_maps_ordered_collection_keys_and_values() {
        assert_match!("{[1, 2] (3, 4)}" =~ "{[1, 2] (3, 4)}");
        assert_match!("{[2, 1] (3, 4)}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{[2, 1] (4, 3)}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{[1, 2] (4, 3)}" !~ "{[1, 2] (3, 4)}");

        assert_match!("{(3, 4) [1, 2]}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{(3, 4) [2, 1]}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{(4, 3) [2, 1]}" !~ "{[1, 2] (3, 4)}");
        assert_match!("{(4, 3) [1, 2]}" !~ "{[1, 2] (3, 4)}");
    }

    #[test]
    fn test_match_maps_unordered_collection_keys_and_values() {
        assert_match!("{#{1, 2} #{3, 4}}" =~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{2, 1} #{3, 4}}" =~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{2, 1} #{4, 3}}" =~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{1, 2} #{4, 3}}" =~ "{#{1, 2} #{3, 4}}");

        assert_match!("{#{3, 4} #{1, 2}}" !~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{3, 4} #{2, 1}}" !~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{4, 3} #{2, 1}}" !~ "{#{1, 2} #{3, 4}}");
        assert_match!("{#{4, 3} #{1, 2}}" !~ "{#{1, 2} #{3, 4}}");
    }

    #[test]
    fn test_match_any_simple() {
        assert_match!("_" =~ "nil");
        assert_match!("_" =~ "true");
        assert_match!("_" =~ "1");
        assert_match!("_" =~ "1N");
        assert_match!("_" =~ "1.0");
        assert_match!("_" =~ "\"a\"");
        assert_match!("_" =~ "_");
        assert_match!("_" =~ "symbol");
        assert_match!("_" =~ "ns/symbol");
        assert_match!("_" =~ ":keyword");
        assert_match!("_" =~ ":ns/keyword");
        assert_match!("_" =~ "[nil, true, 1, \"foo\", bar, :baz]");
        assert_match!("_" =~ "(nil, true, 1, \"foo\", bar, :baz)");
        assert_match!("_" =~ "#{nil, true, 1, \"foo\", bar, :baz}");
        assert_match!("_" =~ "{nil true, 1 \"foo\", bar :baz}");
    }

    #[test]
    fn test_match_any_in_same_collection_type_simple() {
        assert_match!("[_]" =~ "[1]");
        assert_match!("(_)" =~ "(2)");
        assert_match!("#{_}" =~ "#{3}");
        assert_match!("{_ _}" =~ "{4 5}");
    }

    #[test]
    fn test_match_any_in_different_collection_type_simple() {
        assert_match!("[_]" !~ "(1)");
        assert_match!("(_)" !~ "#{2}");
        assert_match!("#{_}" !~ "[3]");
        assert_match!("{_ _}" !~ "[4 5]");
        assert_match!("{_ _}" !~ "(6 7)");
        assert_match!("{_ _}" !~ "#{8 9}");
    }

    #[test]
    fn test_match_any_in_vector_with_multiple_values() {
        assert_match!("[_ 2]" =~ "[1 2]");
        assert_match!("[1 _]" =~ "[1 2]");
        assert_match!("[1 _ 3 4]" =~ "[1 2 3 4]");
        assert_match!("[1 [2 [3 _]] 5 [_ 7]]" =~ "[1 [2 [3 4]] 5 [6 7]]");

        assert_match!("[_]" =~ "[[foo bar]]");
        assert_match!("[_]" =~ "[(foo bar)]");
        assert_match!("[_]" =~ "[#{foo bar}]");
        assert_match!("[_]" =~ "[{foo bar}]");

        assert_match!("[_ 2]" !~ "[2 1]");
        assert_match!("[1 _]" !~ "[2 1]");
        assert_match!("[1 _ 3]" !~ "[2 1 3]");

        assert_match!("[_ 2]" !~ "[3 4]");
        assert_match!("[1 _]" !~ "[3 4]");
    }

    #[test]
    fn test_match_multiple_any_in_vector_with_multiple_values() {
        assert_match!("[1 _ _]" =~ "[1 2 3]");
        assert_match!("[2 _ _]" !~ "[1 2 3]");
        assert_match!("[3 _ _]" !~ "[1 2 3]");
        assert_match!("[_ 1 _]" !~ "[1 2 3]");
        assert_match!("[_ 2 _]" =~ "[1 2 3]");
        assert_match!("[_ 3 _]" !~ "[1 2 3]");
        assert_match!("[_ _ 1]" !~ "[1 2 3]");
        assert_match!("[_ _ 2]" !~ "[1 2 3]");
        assert_match!("[_ _ 3]" =~ "[1 2 3]");

        assert_match!("[1 _ _]" !~ "[2 1 3]");
        assert_match!("[2 _ _]" =~ "[2 1 3]");
        assert_match!("[3 _ _]" !~ "[2 1 3]");
        assert_match!("[_ 1 _]" =~ "[2 1 3]");
        assert_match!("[_ 2 _]" !~ "[2 1 3]");
        assert_match!("[_ 3 _]" !~ "[2 1 3]");
        assert_match!("[_ _ 1]" !~ "[2 1 3]");
        assert_match!("[_ _ 2]" !~ "[2 1 3]");
        assert_match!("[_ _ 3]" =~ "[2 1 3]");
    }

    #[test]
    fn test_match_any_in_list_with_multiple_values() {
        assert_match!("(_ 2)" =~ "(1 2)");
        assert_match!("(1 _)" =~ "(1 2)");
        assert_match!("(1 _ 3 4)" =~ "(1 2 3 4)");
        assert_match!("(1 (2 (3 _)) 5 (_ 7))" =~ "(1 (2 (3 4)) 5 (6 7))");

        assert_match!("(_)" =~ "([foo bar])");
        assert_match!("(_)" =~ "((foo bar))");
        assert_match!("(_)" =~ "(#{foo bar})");
        assert_match!("(_)" =~ "({foo bar})");

        assert_match!("(_ 2)" !~ "(2 1)");
        assert_match!("(1 _)" !~ "(2 1)");
        assert_match!("(1 _ 3)" !~ "(2 1 3)");

        assert_match!("(_ 2)" !~ "(3 4)");
        assert_match!("(1 _)" !~ "(3 4)");
    }

    #[test]
    fn test_match_multiple_any_in_list_with_multiple_values() {
        assert_match!("(1 _ _)" =~ "(1 2 3)");
        assert_match!("(2 _ _)" !~ "(1 2 3)");
        assert_match!("(3 _ _)" !~ "(1 2 3)");
        assert_match!("(_ 1 _)" !~ "(1 2 3)");
        assert_match!("(_ 2 _)" =~ "(1 2 3)");
        assert_match!("(_ 3 _)" !~ "(1 2 3)");
        assert_match!("(_ _ 1)" !~ "(1 2 3)");
        assert_match!("(_ _ 2)" !~ "(1 2 3)");
        assert_match!("(_ _ 3)" =~ "(1 2 3)");

        assert_match!("(1 _ _)" !~ "(2 1 3)");
        assert_match!("(2 _ _)" =~ "(2 1 3)");
        assert_match!("(3 _ _)" !~ "(2 1 3)");
        assert_match!("(_ 1 _)" =~ "(2 1 3)");
        assert_match!("(_ 2 _)" !~ "(2 1 3)");
        assert_match!("(_ 3 _)" !~ "(2 1 3)");
        assert_match!("(_ _ 1)" !~ "(2 1 3)");
        assert_match!("(_ _ 2)" !~ "(2 1 3)");
        assert_match!("(_ _ 3)" =~ "(2 1 3)");
    }

    #[test]
    fn test_match_any_in_set_with_multiple_values() {
        assert_match!("#{_ 2}" =~ "#{1 2}");
        assert_match!("#{1 _}" =~ "#{1 2}");
        assert_match!("#{1 _ 3 4}" =~ "#{1 2 3 4}");
        assert_match!("#{1 #{2 #{3 _}} 5 #{_ 7}}" =~ "#{1 #{2 #{3 4}} 5 #{6 7}}");

        assert_match!("#{_}" =~ "#{[foo bar]}");
        assert_match!("#{_}" =~ "#{(foo bar)}");
        assert_match!("#{_}" =~ "#{#{foo bar}}");
        assert_match!("#{_}" =~ "#{{foo bar}}");

        assert_match!("#{_ 2}" =~ "#{2 1}");
        assert_match!("#{1 _}" =~ "#{2 1}");
        assert_match!("#{1 _ 3}" =~ "#{2 1 3}");

        assert_match!("#{_ 2}" !~ "#{3 4}");
        assert_match!("#{1 _}" !~ "#{3 4}");
    }

    #[test]
    fn test_match_multiple_any_in_set_with_multiple_values() {
        // These are false because _ is a symbol and sets guarantee
        // uniqueness of children. So TuringString matching will fail because
        // the TuringString is a set of length 2, while the matched edbn is a set
        // of length 3. If _ were unique, all of these assertions would
        // be true. Need to better handle TuringString rules.

        assert_match!("#{1 _ _}" !~ "#{1 2 3}");
        assert_match!("#{2 _ _}" !~ "#{1 2 3}");
        assert_match!("#{3 _ _}" !~ "#{1 2 3}");
        assert_match!("#{_ 1 _}" !~ "#{1 2 3}");
        assert_match!("#{_ 2 _}" !~ "#{1 2 3}");
        assert_match!("#{_ 3 _}" !~ "#{1 2 3}");
        assert_match!("#{_ _ 1}" !~ "#{1 2 3}");
        assert_match!("#{_ _ 2}" !~ "#{1 2 3}");
        assert_match!("#{_ _ 3}" !~ "#{1 2 3}");

        assert_match!("#{1 _ _}" !~ "#{2 1 3}");
        assert_match!("#{2 _ _}" !~ "#{2 1 3}");
        assert_match!("#{3 _ _}" !~ "#{2 1 3}");
        assert_match!("#{_ 1 _}" !~ "#{2 1 3}");
        assert_match!("#{_ 2 _}" !~ "#{2 1 3}");
        assert_match!("#{_ 3 _}" !~ "#{2 1 3}");
        assert_match!("#{_ _ 1}" !~ "#{2 1 3}");
        assert_match!("#{_ _ 2}" !~ "#{2 1 3}");
        assert_match!("#{_ _ 3}" !~ "#{2 1 3}");
    }

    #[test]
    fn test_match_any_in_map_with_multiple_values() {
        assert_match!("{_ 2}" =~ "{1 2}");
        assert_match!("{1 _}" =~ "{1 2}");
        assert_match!("{1 _, 3 4}" =~ "{1 2, 3 4}");
        assert_match!("{1 {2 {3 _}}, 5 {_ 7}}" =~ "{1 {2 {3 4}}, 5 {6 7}}");

        assert_match!("{_ _}" =~ "{[foo bar] [baz boz]}");
        assert_match!("{_ _}" =~ "{(foo bar) (baz boz)}");
        assert_match!("{_ _}" =~ "{#{foo bar} #{baz boz}}");
        assert_match!("{_ _}" =~ "{{foo bar} {baz boz}}");

        assert_match!("{_ 2, 3 4}" =~ "{3 4, 1 2}");
        assert_match!("{1 _, 3 4}" =~ "{3 4, 1 2}");
        assert_match!("{_ _, 3 4}" =~ "{3 4, 1 2}");
        assert_match!("{1 2, _ 4}" =~ "{3 4, 1 2}");
        assert_match!("{1 2, 3 _}" =~ "{3 4, 1 2}");
        assert_match!("{1 2, _ _}" =~ "{3 4, 1 2}");
        assert_match!("{1 2, _ 4, 5 6}" =~ "{3 4, 1 2, 5 6}");
        assert_match!("{1 2, 3 _, 5 6}" =~ "{3 4, 1 2, 5 6}");
        assert_match!("{1 2, _ _, 5 6}" =~ "{3 4, 1 2, 5 6}");

        assert_match!("{_ 2}" !~ "{3 4}");
        assert_match!("{1 _}" !~ "{3 4}");
    }

    #[test]
    fn test_match_multiple_any_in_map_with_multiple_values() {
        // These are false because _ is a symbol and maps guarantee
        // uniqueness of keys. So TuringString matching will fail because
        // the TuringString is a map of length 2, while the matched edbn is a map
        // of length 3. If _ were unique, all of these assertions would
        // be true. Need to better handle TuringString rules.

        assert_match!("{1 2, _ 4, _ 6}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{3 4, _ 6, _ 2}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{5 6, _ 2, _ 4}" !~ "{1 2, 3 4, 5 6}");

        assert_match!("{1 2, _ _, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{3 4, _ _, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{5 6, _ _, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, 1 2, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, 3 4, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, 5 6, _ _}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, _ _, 1 2}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, _ _, 3 4}" !~ "{1 2, 3 4, 5 6}");
        assert_match!("{_ _, _ _, 5 6}" !~ "{1 2, 3 4, 5 6}");

        assert_match!("{1 2, _ _, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{3 4, _ _, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{5 6, _ _, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, 1 2, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, 3 4, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, 5 6, _ _}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, _ _, 1 2}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, _ _, 3 4}" !~ "{3 4, 1 2, 5 6}");
        assert_match!("{_ _, _ _, 5 6}" !~ "{3 4, 1 2, 5 6}");
    }

    #[test]
    fn test_match_placeholder_simple() {
        assert_match!("?x" =~ "nil");
        assert_match!("?x" =~ "true");
        assert_match!("?x" =~ "1");
        assert_match!("?x" =~ "1N");
        assert_match!("?x" =~ "1.0");
        assert_match!("?x" =~ "\"a\"");
        assert_match!("?x" =~ "_");
        assert_match!("?x" =~ "symbol");
        assert_match!("?x" =~ "ns/symbol");
        assert_match!("?x" =~ ":keyword");
        assert_match!("?x" =~ ":ns/keyword");
        assert_match!("?x" =~ "[nil, true, 1, \"foo\", bar, :baz]");
        assert_match!("?x" =~ "(nil, true, 1, \"foo\", bar, :baz)");
        assert_match!("?x" =~ "#{nil, true, 1, \"foo\", bar, :baz}");
        assert_match!("?x" =~ "{nil true, 1 \"foo\", bar :baz}");
    }

    #[test]
    fn test_match_placeholder_in_same_collection_type_simple() {
        assert_match!("[?x]" =~ "[1]");
        assert_match!("(?x)" =~ "(2)");
        assert_match!("#{?x}" =~ "#{3}");
        assert_match!("{?x ?x}" =~ "{4 4}");
        assert_match!("{?x ?x}" !~ "{4 5}");
        assert_match!("{?x ?y}" =~ "{4 4}");
        assert_match!("{?x ?y}" =~ "{4 5}");
    }

    #[test]
    fn test_match_placeholder_in_different_collection_type_simple() {
        assert_match!("[?x]" !~ "(1)");
        assert_match!("(?x)" !~ "#{2}");
        assert_match!("#{?x}" !~ "[3]");
        assert_match!("{?x ?x}" !~ "[4 5]");
        assert_match!("{?x ?x}" !~ "(6 7)");
        assert_match!("{?x ?x}" !~ "#{8 9}");
    }

    #[test]
    fn test_match_placeholder_in_vector_with_multiple_values() {
        assert_match!("[?x ?y]" =~ "[1 2]");
        assert_match!("[?x ?y]" =~ "[1 1]");
        assert_match!("[?x ?x]" !~ "[1 2]");
        assert_match!("[?x ?x]" =~ "[1 1]");

        assert_match!("[1 ?x 3 ?y]" =~ "[1 2 3 4]");
        assert_match!("[1 ?x 3 ?y]" =~ "[1 2 3 2]");
        assert_match!("[1 ?x 3 ?x]" !~ "[1 2 3 4]");
        assert_match!("[1 ?x 3 ?x]" =~ "[1 2 3 2]");

        assert_match!("[1 [2 [3 ?x]] 5 [?y 7]]" =~ "[1 [2 [3 4]] 5 [6 7]]");
        assert_match!("[1 [2 [3 ?x]] 5 [?y 7]]" =~ "[1 [2 [3 4]] 5 [4 7]]");
        assert_match!("[1 [2 [3 ?x]] 5 [?x 7]]" !~ "[1 [2 [3 4]] 5 [6 7]]");
        assert_match!("[1 [2 [3 ?x]] 5 [?y 7]]" =~ "[1 [2 [3 4]] 5 [4 7]]");

        assert_match!("[?x ?y ?x ?y]" =~ "[1 2 1 2]");
        assert_match!("[?x ?y ?x ?y]" !~ "[1 2 2 1]");

        assert_match!("[[?x ?y] [?x ?y]]" =~ "[[1 2] [1 2]]");
        assert_match!("[[?x ?y] [?x ?y]]" !~ "[[1 2] [2 1]]");
    }

    #[test]
    fn test_match_placeholder_in_list_with_multiple_values() {
        assert_match!("(?x ?y)" =~ "(1 2)");
        assert_match!("(?x ?y)" =~ "(1 1)");
        assert_match!("(?x ?x)" !~ "(1 2)");
        assert_match!("(?x ?x)" =~ "(1 1)");

        assert_match!("(1 ?x 3 ?y)" =~ "(1 2 3 4)");
        assert_match!("(1 ?x 3 ?y)" =~ "(1 2 3 2)");
        assert_match!("(1 ?x 3 ?x)" !~ "(1 2 3 4)");
        assert_match!("(1 ?x 3 ?x)" =~ "(1 2 3 2)");

        assert_match!("(1 (2 (3 ?x)) 5 (?y 7))" =~ "(1 (2 (3 4)) 5 (6 7))");
        assert_match!("(1 (2 (3 ?x)) 5 (?y 7))" =~ "(1 (2 (3 4)) 5 (4 7))");
        assert_match!("(1 (2 (3 ?x)) 5 (?x 7))" !~ "(1 (2 (3 4)) 5 (6 7))");
        assert_match!("(1 (2 (3 ?x)) 5 (?y 7))" =~ "(1 (2 (3 4)) 5 (4 7))");

        assert_match!("(?x ?y ?x ?y)" =~ "(1 2 1 2)");
        assert_match!("(?x ?y ?x ?y)" !~ "(1 2 2 1)");

        assert_match!("((?x ?y) (?x ?y))" =~ "((1 2) (1 2))");
        assert_match!("((?x ?y) (?x ?y))" !~ "((1 2) (2 1))");
    }

    #[test]
    fn test_match_placeholder_in_set_with_multiple_values() {
        assert_match!("#{?x ?y}" =~ "#{1 2}");
        assert_match!("#{?x ?y}" !~ "#{1 1}");
        assert_match!("#{?x ?x}" !~ "#{1 2}");
        assert_match!("#{?x ?x}" =~ "#{1 1}");

        assert_match!("#{1 ?x 3 ?y}" =~ "#{1 2 3 4}");
        assert_match!("#{1 ?x 3 ?y}" !~ "#{1 2 3 2}");
        assert_match!("#{1 ?x 3 ?x}" !~ "#{1 2 3 4}");
        assert_match!("#{1 ?x 3 ?x}" =~ "#{1 2 3 2}");

        assert_match!("#{1 #{2 #{3 ?x}} 5 #{?y 7}}" =~ "#{1 #{2 #{3 4}} 5 #{6 7}}");
        assert_match!("#{1 #{2 #{3 ?x}} 5 #{?y 7}}" =~ "#{1 #{2 #{3 4}} 5 #{4 7}}");
        assert_match!("#{1 #{2 #{3 ?x}} 5 #{?x 7}}" !~ "#{1 #{2 #{3 4}} 5 #{6 7}}");
        assert_match!("#{1 #{2 #{3 ?x}} 5 #{?y 7}}" =~ "#{1 #{2 #{3 4}} 5 #{4 7}}");

        assert_match!("#{?x ?y ?x ?y}" =~ "#{1 2 1 2}");
        assert_match!("#{?x ?y ?x ?y}" =~ "#{1 2 2 1}");

        assert_match!("#{#{?x ?y} #{?x ?y}}" =~ "#{#{1 2} #{1 2}}");
        assert_match!("#{#{?x ?y} #{?x ?y}}" =~ "#{#{1 2} #{2 1}}");
    }

    #[test]
    fn test_match_placeholder_in_map_with_multiple_values() {
        assert_match!("{?x ?y}" =~ "{1 2}");
        assert_match!("{?x ?y}" =~ "{1 1}");
        assert_match!("{?x ?x}" !~ "{1 2}");
        assert_match!("{?x ?x}" =~ "{1 1}");

        assert_match!("{1 ?x, 3 ?y}" =~ "{1 2, 3 4}");
        assert_match!("{1 ?x, 3 ?y}" =~ "{1 2, 3 2}");
        assert_match!("{1 ?x, 3 ?x}" !~ "{1 2, 3 4}");
        assert_match!("{1 ?x, 3 ?x}" =~ "{1 2, 3 2}");

        assert_match!("{1 {2 {3 ?x}}, 5 {?y 7}}" =~ "{1 {2 {3 4}}, 5 {6 7}}");
        assert_match!("{1 {2 {3 ?x}}, 5 {?y 7}}" =~ "{1 {2 {3 4}}, 5 {4 7}}");
        assert_match!("{1 {2 {3 ?x}}, 5 {?x 7}}" !~ "{1 {2 {3 4}}, 5 {6 7}}");
        assert_match!("{1 {2 {3 ?x}}, 5 {?y 7}}" =~ "{1 {2 {3 4}}, 5 {4 7}}");

        assert_match!("{?x ?y, ?x ?y}" =~ "{1 2, 1 2}");
        assert_match!("{?x ?y, ?x ?y}" !~ "{1 2, 2 1}");

        assert_match!("{{?x ?y}, {?x ?y}}" =~ "{{1 2}, {1 2}}");
        assert_match!("{{?x ?y}, {?x ?y}}" !~ "{{1 2}, {2 1}}");
    }

    #[test]
    fn test_match_placeholder_in_different_value_types() {
        assert_match!("{1 {2 [3 ?x]}, 5 (?y 7)}" =~ "{1 {2 [3 4]}, 5 (6 7)}");
        assert_match!("{1 {2 [3 ?x]}, 5 (?y 7)}" =~ "{1 {2 [3 4]}, 5 (4 7)}");
        assert_match!("{1 {2 [3 ?x]}, 5 (?x 7)}" !~ "{1 {2 [3 4]}, 5 (6 7)}");
        assert_match!("{1 {2 [3 ?x]}, 5 (?y 7)}" =~ "{1 {2 [3 4]}, 5 (4 7)}");

        assert_match!("{?x {?x [?x ?x]}, ?x (?x ?x)}" !~ "{1 {2 [3 4]}, 5 (6 7)}");
        assert_match!("{?x {?x [?x ?x]}, ?x (?x ?x)}" =~ "{1 {1 [1 1]}, 1 (1 1)}");

        assert_match!("[#{?x ?y} ?x]" =~ "[#{1 2} 1]");
        assert_match!("[#{?x ?y} ?y]" =~ "[#{1 2} 2]");
    }
}
