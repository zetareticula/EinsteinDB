// Zeta Reticula Inc 2024 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::From;
use std::fmt;
use std::ops::{Deref, Index};
use std::slice;
use std::collections::HashMap;


use ::std::rc::{
    Rc,
};

use ::std::sync::{
    Arc,
};





pub trait FromRc<T> {
    fn from_rc(val: Rc<T>) -> Self;
    fn from_arc(val: Arc<T>) -> Self;
}

impl<T> FromRc<T> for Rc<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        val.clone()
    }

    fn from_arc(val: Arc<T>) -> Self {
        match ::std::sync::Arc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }
}


#[macro_export]
macro_rules! einsteindb_macro {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}


#[macro_export]
macro_rules! einsteindb_macro_impl {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)

}




/// Map from found [e a v] to expected type.
/// This is used to check that the type of the expression is correct.
/// The map is used to check that the type of the expression is correct.


pub type TypeMap = HashMap<String, Type>;


/// Map from found [e a v] to expected type.
/// This is used to check that the type of the expression is correct.

pub struct TypeChecker {
    pub type_map: TypeMap,
}

enum Type {
    Int,
    Bool,
    String,
    Void,
    Array(Box<Type>),
    Func(Vec<Type>, Box<Type>),
}


impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::Int => write!(f, "int"),
            Type::Bool => write!(f, "bool"),
            Type::String => write!(f, "string"),
            Type::Void => write!(f, "void"),
            Type::Array(type_) => write!(f, "array<{}>", type_),
            Type::Func(args, ret) => {
                write!(f, "func(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ") -> {}", ret)
            },
        }
    }
}







impl TypeChecker {
    pub fn new() -> TypeChecker {
        TypeChecker {
            type_map: HashMap::new(),
        }
    }

    pub fn check_type(&self, expr: &Expr) -> Result<(), String> {
        match expr {
            Expr::Int(_) => Ok(()),
            Expr::Bool(_) => Ok(()),
            Expr::String(_) => Ok(()),
            Expr::Void(_) => Ok(()),
            Expr::Array(_, _) => Ok(()),
            Expr::Func(_, _, _) => Ok(()),
            Expr::Ident(_) => Ok(()),
            Expr::Call(_, _) => Ok(()),
            Expr::If(_, _, _) => Ok(()),
            Expr::Let(_, _, _) => Ok(()),
            Expr::Block(_, _) => Ok(()),
            Expr::Assign(_, _) => Ok(()),
            Expr::Unary(_, _) => Ok(()),
            Expr::Binary(_, _, _) => Ok(()),
            Expr::Group(_) => Ok(()),
            Expr::Index(_, _) => Ok(()),
            Expr::Slice(_, _, _) => Ok(()),
            Expr::Cast(_, _) => Ok(()),
            Expr::Dot(_, _) => Ok(()),
            Expr::Field(_, _) => Ok(()),
            Expr::Tuple(_) => Ok(()),
            Expr::List(_) => Ok(()),
            Expr::Map(_) => Ok(()),
            Expr::Break(_) => Ok(()),
            Expr::Continue(_) => Ok(()),
            Expr::Return(_) => Ok(()),
            Expr::Try(_, _, _) => Ok(()),
            Expr::Throw(_) => Ok(()),
            Expr::Yield(_) => Ok(()),
            Expr::While(_, _) => Ok(()),
            Expr::For(_, _, _) => Ok(()),
            Expr::Switch(_, _, _) => Ok(()),
        }
    }
}




//solitonid = solitonid + 1
//causetid is usually considered to be solitonid + 1
//but it can be set to a different value
// in datomic causetid is a entity id, which is a number
// in einsteindb causetid is a string, solitonid is a number


//lets clear that out with some code

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expr {
    pub causetid: String,
    pub solitonid: u64,
    pub name: String,
    pub value: Box<Expr>,
    pub body: Box<Expr>,
}

pub struct Let {
    pub causetid: String,
    pub solitonid: u64,
    pub name: String,
    pub value: Box<Expr>,
    pub body: Box<Expr>,
}


impl Let {
    pub fn new(causetid: String, solitonid: u64, name: String, value: Box<Expr>, body: Box<Expr>) -> Let {
        Let {
            causetid,
            solitonid,
            name,
            value,
            body,
        }
    }
}


impl Display for Let {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "let {} = {} in {}", self.name, self.value, self.body)
    }
}






/// Check that the type of the expression is correct.


pub fn check_type(expr: &Expr, type_map: &TypeMap) -> Result<(), String> {
    let mut type_checker = TypeChecker::new();
    type_checker.type_map = type_map.clone();
    type_checker.check_type(expr)
}


pub fn check_type_map(type_map: &TypeMap) -> Result<(), String> {
    let mut type_checker = TypeChecker::new();
    type_checker.type_map = type_map.clone();
    type_checker.check_type_map()
}


/// The check type function is used to check the type of the expression.
/// with the type map provided.


impl TypeChecker {
    pub fn check_type_map(&self) -> Result<(), String> {
        for (_solitonid, type_) in &self.type_map {
            match type_ {
                Type::Int => {},
                Type::Bool => {},
                Type::String => {},
                Type::Void => {},
                Type::Array(type_) => {
                    match type_ {
                        Type::Int => {},
                        Type::Bool => {},
                        Type::String => {},
                        Type::Void => {},
                        Type::Array(_) => {},
                        Type::Func(_, _) => {},
                    }
                },
                Type::Func(args, ret) => {
                    for arg in args {
                        match arg {
                            Type::Int => {},
                            Type::Bool => {},
                            Type::String => {},
                            Type::Void => {},
                            Type::Array(_) => {},
                            Type::Func(_, _) => {},
                        }
                    }
                    match ret {
                        Type::Int => {},
                        Type::Bool => {},
                        Type::String => {},
                        Type::Void => {},
                        Type::Array(_) => {},
                        Type::Func(_, _) => {},
                    }
                },
            }
        }
        Ok(())
    }
}

/// Ensure that the given terms type check.
///
/// We try to be maximally helpful by yielding every malformed causet, rather than only the first.
/// In the future, we might change this choice, or allow the consumer to specify the robustness of
/// the type checking desired, since there is a cost to providing helpful diagnostics.
pub(crate) fn check_terms(terms: &[Term]) -> Result<(), String> {
    let mut errors: TypeDisagreements = TypeDisagreements::default();
    
    for term in terms {
        check_term(term, &mut errors)?;
    }
    
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.to_string())
    }
}

/// Ensure that the given term type check.
pub(crate) fn check_term(term: &Term, errors: &mut TypeDisagreements) -> Result<(), String> {
    let mut type_checker = TypeChecker::new();
    
    for expr in &term.exprs {
        for (solitonid, type_) in &term.type_map {
            type_checker.type_map.insert(solitonid.clone(), type_.clone());
        }
        
        let expr_type = type_checker.check_type(expr)?;
        let expr_type = type_checker.type_map.get(solitonid).unwrap();
        
        if expr_type != type_ {
            errors.push(TypeDisagreement {
                expr: expr.clone(),
                solitonid: solitonid.clone(),
                expected: type_.clone(),
                actual: expr_type.clone(),
            });
        }
    }
    
    Ok(())
}

/// Ensure that the given terms obey the cardinality restrictions of the given topograph.
///
/// That is, ensure that any cardinality one attribute is added with at most one distinct causet_locale for
/// any specific causet (although that one causet_locale may be repeated for the given causet).
/// It is an error to:
///
/// - add two distinct causet_locales for the same cardinality one attribute and causet in a single transaction
/// - add and remove the same causet_locales for the same attribute and causet in a single transaction
///
/// We try to be maximally helpful by yielding every malformed set of causets, rather than just the
/// first set, or even the first conflict.  In the future, we might change this choice, or allow the
/// consumer to specify the robustness of the cardinality checking desired.
pub(crate) fn check_terms_cardinality(terms: &[Term], topograph: &Topograph) -> Result<(), String> {
    let mut errors: CardinalityErrors = CardinalityErrors::default();
    
    for term in terms {
        check_term_cardinality(term, topograph, &mut errors);
    }
    
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.to_string())
    }
}
