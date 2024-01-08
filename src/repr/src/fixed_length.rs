// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides fixed-length representations for data composed of `Datum`s of fixed-length types.
//! These representations are aimed at being more efficient in memory usage than `Row` by
//! relying on statically selected container lengths. Traits are provided that allow these
//! representations to be made into instances of `Row` or created from `Row`s. The traits are
//! trivially implemented for `Row` itself, providing a uniform interface to describe `Row`s
//! or fixed-length containers standing in for them.

use std::borrow::Borrow;
use std::iter::{empty, Empty};

use crate::row::DatumListIter;
use crate::{ColumnType, Datum, Row};

/// A helper trait to get references to `Row` based on type information that only manifests
/// at runtime (typically originating from inferred schemas).
pub trait IntoRowByTypes<'a> {
    /// An iterator type for use in `into_datum_iter`.
    type DatumIter: IntoIterator<Item = Datum<'a>>;

    /// Obtains an iterator of datums out of an instance of `Self`, given a schema provided
    /// by `types`.
    ///
    /// Implementations are free to place specific requirements on the given schema.
    fn into_datum_iter(self, types: Option<&[ColumnType]>) -> Self::DatumIter;
}

// Blanket identity implementation for Row.
impl<'b> IntoRowByTypes<'b> for &'b Row {
    /// Datum iterator for `Row`.
    type DatumIter = DatumListIter<'b>;

    /// Borrows `self` and gets an iterator from it.
    ///
    /// This implementation panics if `types` other than `None` are provided. This is because
    /// `Row` is already self-describing and can use variable-length types, so we are
    /// explicitly not validating the given schema.
    #[inline]
    fn into_datum_iter(self, types: Option<&[ColumnType]>) -> Self::DatumIter {
        assert!(types.is_none());
        self.iter()
    }
}

/// A helper trait to construct target values from input `Row` instances based on type
/// information that only manifests at runtime (typically originating from inferred schemas).
/// Typically, the target type will be of fixed-length without tags per column or `Row` itself.
pub trait FromRowByTypes: Sized + Default {
    /// Obtains an instance of `Self` given an instance of `Row` and a schema provided
    /// by `types`.
    ///
    /// Implementations are free to place specific requirements on the given schema.
    #[inline]
    fn from_row(&mut self, row: Row, types: Option<&[ColumnType]>) -> Self {
        let iter = row.iter();
        self.from_datum_iter(iter, types)
    }

    /// Obtains an instance of `Self' given an iterator of borrowed datums and a schema
    /// provided by `types`.
    ///
    /// Implementations are free to place specific requirements on the given schema.
    fn from_datum_iter<'a, I, D>(&mut self, datum_iter: I, types: Option<&[ColumnType]>) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>;

    /// Obtains an instance of `Self' given an iterator of results of borrowed datums and a schema
    /// provided by `types`.
    ///
    /// Implementations are free to place specific requirements on the given schema.
    ///
    /// In the case the iterator produces an error, the pushing of datums is terminated and the
    /// error returned.
    fn try_from_datum_iter<'a, I, D, E>(
        &mut self,
        datum_iter: I,
        types: Option<&[ColumnType]>,
    ) -> Result<Self, E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>;
}

impl FromRowByTypes for Row {
    /// Returns the provided row itself.
    ///
    /// This implementation panics if `types` other than `None` provided. This is because
    /// `Row` is already self-describing and can use variable-length types, so we are
    /// explicitly not validating the given schema.
    #[inline]
    fn from_row(&mut self, row: Row, types: Option<&[ColumnType]>) -> Self {
        assert!(types.is_none());
        row
    }

    /// Packs into `self` the given iterator of datums and returns a clone.
    ///
    /// This implementation panics if non-empty `types` are provided. This is because
    /// `Row` is already self-describing and can use variable-length types, so we are
    /// explicitly not validating the given schema.
    #[inline]
    fn from_datum_iter<'a, I, D>(&mut self, datum_iter: I, types: Option<&[ColumnType]>) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        assert!(types.is_none());
        self.packer().extend(datum_iter);
        self.clone()
    }

    /// Packs into `self` by using the packer's `try_extend` method on the given iterator
    /// and returns a clone.
    ///
    /// This implementation panics if non-empty `types` are provided. This is because
    /// `Row` is already self-describing and can use variable-length types, so we are
    /// explicitly not validating the given schema.
    #[inline]
    fn try_from_datum_iter<'a, I, D, E>(
        &mut self,
        datum_iter: I,
        types: Option<&[ColumnType]>,
    ) -> Result<Self, E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>,
    {
        assert!(types.is_none());
        self.packer().try_extend(datum_iter)?;
        Ok(self.clone())
    }
}

impl<'b> IntoRowByTypes<'b> for &'b () {
    /// Empty iterator for unit.
    type DatumIter = Empty<Datum<'b>>;

    /// Returns an empty iterator.
    ///
    /// This implementation panics if `types` other than `Some(&[])` are provided. This is because
    /// unit values need to have an empty schema.
    #[inline]
    fn into_datum_iter(self, types: Option<&[ColumnType]>) -> Self::DatumIter {
        let Some(&[]) = types else {
            panic!("Non-empty schema with unit values")
        };
        empty()
    }
}

impl FromRowByTypes for () {
    /// Obtains a unit value from an empty `Row`.
    ///
    /// This implementation panics if `types` other than `Some(&[])` are provided. This is because
    /// unit values need to have an empty schema.
    #[inline]
    fn from_row(&mut self, row: Row, types: Option<&[ColumnType]>) -> Self {
        let Some(&[]) = types else {
            panic!("Non-empty schema with unit values")
        };
        debug_assert!(row.is_empty());
        ()
    }

    /// Obtains a unit value from an empty datum iterator.
    ///
    /// This implementation panics if `types` other than `Some(&[])` are provided. This is because
    /// unit values need to have an empty schema.
    #[inline]
    fn from_datum_iter<'a, I, D>(&mut self, datum_iter: I, types: Option<&[ColumnType]>) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        let Some(&[]) = types else {
            panic!("Non-empty schema with unit values")
        };
        debug_assert!(datum_iter.into_iter().next().is_none());
        ()
    }

    /// Obtains a unit value from an empty iterator of results of datums.
    ///
    /// This implementation panics if `types` other than `Some(&[])` are provided. This is because
    /// unit values need to have an empty schema.
    #[inline]
    fn try_from_datum_iter<'a, I, D, E>(
        &mut self,
        datum_iter: I,
        types: Option<&[ColumnType]>,
    ) -> Result<Self, E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>,
    {
        let Some(&[]) = types else {
            panic!("Non-empty schema with unit values")
        };
        debug_assert!(datum_iter.into_iter().next().is_none());
        Ok(())
    }
}
