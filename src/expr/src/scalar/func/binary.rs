// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for binary functions.

use mz_repr::{ColumnType, Datum, DatumType, RowArena};

use crate::{EvalError, MirScalarExpr};

/// A description of an SQL binary function that has the ability to lazy evaluate its arguments
// This trait will eventually be annotated with #[enum_dispatch] to autogenerate the UnaryFunc enum
pub(crate) trait LazyBinaryFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        b: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError>;

    /// The output ColumnType of this function.
    fn output_type(&self, input_type: ColumnType) -> ColumnType;

    /// Whether this function will produce NULL on NULL input.
    fn propagates_nulls(&self) -> bool;

    /// Whether this function will produce NULL on non-NULL input.
    fn introduces_nulls(&self) -> bool;

    /// Whether this function might error on non-error input.
    fn could_error(&self) -> bool {
        // NB: override this for functions that never error.
        true
    }

    /// Whether this function preserves uniqueness.
    ///
    /// Uniqueness is preserved when `if f(x) = f(y) then x = y` is true. This
    /// is used by the optimizer when a guarantee can be made that a collection
    /// with unique items will stay unique when mapped by this function.
    ///
    /// Note that error results are not covered: Even with `preserves_uniqueness = true`, it can
    /// happen that two different inputs produce the same error result. (e.g., in case of a
    /// narrowing cast)
    ///
    /// Functions should conservatively return `false` unless they are certain
    /// the above property is true.
    fn preserves_uniqueness(&self) -> bool;

    /// The [inverse] of this function, if it has one and we have determined it.
    ///
    /// The optimizer _can_ use this information when selecting indexes, e.g. an
    /// indexed column has a cast applied to it, by moving the right inverse of
    /// the cast to another value, we can select the indexed column.
    ///
    /// Note that a value of `None` does not imply that the inverse does not
    /// exist; it could also mean we have not yet invested the energy in
    /// representing it. For example, in the case of complex casts, such as
    /// between two list types, we could determine the right inverse, but doing
    /// so is not immediately necessary as this information is only used by the
    /// optimizer.
    ///
    /// ## Right vs. left vs. inverses
    /// - Right inverses are when the inverse function preserves uniqueness.
    ///   These are the functions that the optimizer uses to move casts between
    ///   expressions.
    /// - Left inverses are when the function itself preserves uniqueness.
    /// - Inverses are when a function is both a right and a left inverse (e.g.,
    ///   bit_not_int64 is both a right and left inverse of itself).
    ///
    /// We call this function `inverse` for simplicity's sake; it doesn't always
    /// correspond to the mathematical notion of "inverse." However, in
    /// conjunction with checks to `preserves_uniqueness` you can determine
    /// which type of inverse we return.
    ///
    /// [inverse]: https://en.wikipedia.org/wiki/Inverse_function
    fn inverse(&self) -> Option<crate::BinaryFunc>;

    /// Returns true if the function is monotone. (Non-strict; either increasing or decreasing.)
    /// Monotone functions map ranges to ranges: ie. given a range of possible inputs, we can
    /// determine the range of possible outputs just by mapping the endpoints.
    ///
    /// This property describes the behaviour of the function over ranges where the function is defined:
    /// ie. the argument and the result are non-error datums.
    fn is_monotone(&self) -> bool;

    /// Yep, I guess this returns true for infix operators.
    fn is_infix_op(&self) -> bool;
}

pub(crate) trait EagerBinaryFunc<'a> {
    type Input1: DatumType<'a, EvalError>;
    type Input2: DatumType<'a, EvalError>;
    type Output: DatumType<'a, EvalError>;

    fn call(&self, a: Self::Input1, b: Self::Input2, temp_storage: &'a RowArena) -> Self::Output;

    /// The output ColumnType of this function
    fn output_type(&self, input_type: ColumnType) -> ColumnType;

    /// Whether this function will produce NULL on NULL input
    fn propagates_nulls(&self) -> bool {
        // If the input is not nullable then nulls are propagated
        !Self::Input1::nullable() && !Self::Input2::nullable()
    }

    /// Whether this function will produce NULL on non-NULL input
    fn introduces_nulls(&self) -> bool {
        // If the output is nullable then nulls can be introduced
        Self::Output::nullable()
    }

    /// Whether this function could produce an error
    fn could_error(&self) -> bool {
        Self::Output::fallible()
    }

    /// Whether this function preserves uniqueness
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    fn inverse(&self) -> Option<crate::BinaryFunc> {
        None
    }

    fn is_monotone(&self) -> bool {
        false
    }

    fn is_infix_op(&self) -> bool {
        // TODO
        false
    }
}

impl<T: for<'a> EagerBinaryFunc<'a>> LazyBinaryFunc for T {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        a: &'a MirScalarExpr,
        b: &'a MirScalarExpr,
    ) -> Result<Datum<'a>, EvalError> {
        let a = match T::Input1::try_from_result(a.eval(datums, temp_storage)) {
            // If we can convert to the input type then we call the function
            Ok(input) => input,
            // If we can't and we got a non-null datum something went wrong in the planner
            Err(Ok(datum)) if !datum.is_null() => {
                return Err(EvalError::Internal("invalid input type".into()))
            }
            // Otherwise we just propagate NULLs and errors
            Err(res) => return res,
        };
        let b = match T::Input2::try_from_result(b.eval(datums, temp_storage)) {
            // If we can convert to the input type then we call the function
            Ok(input) => input,
            // If we can't and we got a non-null datum something went wrong in the planner
            Err(Ok(datum)) if !datum.is_null() => {
                return Err(EvalError::Internal("invalid input type".into()))
            }
            // Otherwise we just propagate NULLs and errors
            Err(res) => return res,
        };
        self.call(a, b, temp_storage).into_result(temp_storage)
    }

    fn output_type(&self, input_type: ColumnType) -> ColumnType {
        self.output_type(input_type)
    }

    fn propagates_nulls(&self) -> bool {
        self.propagates_nulls()
    }

    fn introduces_nulls(&self) -> bool {
        self.introduces_nulls()
    }

    fn could_error(&self) -> bool {
        self.could_error()
    }

    fn preserves_uniqueness(&self) -> bool {
        self.preserves_uniqueness()
    }

    fn inverse(&self) -> Option<crate::BinaryFunc> {
        self.inverse()
    }

    fn is_monotone(&self) -> bool {
        self.is_monotone()
    }

    fn is_infix_op(&self) -> bool {
        self.is_infix_op()
    }
}
