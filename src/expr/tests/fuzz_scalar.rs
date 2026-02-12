// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Type-aware fuzz tests for scalar function evaluation.
//!
//! Each function is tested only with datums of its expected input type. This
//! ensures we exercise actual computation paths rather than wasting iterations
//! on type rejection. Functions with polymorphic inputs (`Datum<'a>`,
//! `Range<Datum<'a>>`, etc.) are manually annotated with a representative type.

use std::panic::{AssertUnwindSafe, catch_unwind};

use chrono::NaiveDate;
use mz_expr::func::*;
use mz_expr::{BinaryFunc, MirScalarExpr, UnaryFunc};
use mz_pgtz::timezone::Timezone;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::adt::regex::Regex;
use mz_repr::{AsColumnType, Datum, RowArena, SqlColumnType, SqlScalarType};

// ---------------------------------------------------------------------------
// Helpers: compile-time type extraction
// ---------------------------------------------------------------------------

/// Extract the input SqlColumnType from an EagerUnaryFunc at compile time.
fn eu<T>(f: T) -> (UnaryFunc, SqlColumnType)
where
    T: EagerUnaryFunc<'static> + Into<UnaryFunc>,
    <T as EagerUnaryFunc<'static>>::Input: AsColumnType,
{
    let mut ct = <<T as EagerUnaryFunc<'static>>::Input as AsColumnType>::as_column_type();
    ct.nullable = true;
    (f.into(), ct)
}

/// Manual type annotation for unary funcs with polymorphic inputs.
fn eu_manual(f: impl Into<UnaryFunc>, ct: SqlColumnType) -> (UnaryFunc, SqlColumnType) {
    (f.into(), ct)
}

/// Extract input types from an EagerBinaryFunc at compile time.
fn eb<T>(f: T) -> (BinaryFunc, SqlColumnType, SqlColumnType)
where
    T: EagerBinaryFunc<'static> + Into<BinaryFunc>,
    <T as EagerBinaryFunc<'static>>::Input1: AsColumnType,
    <T as EagerBinaryFunc<'static>>::Input2: AsColumnType,
{
    let mut ct1 = <<T as EagerBinaryFunc<'static>>::Input1 as AsColumnType>::as_column_type();
    let mut ct2 = <<T as EagerBinaryFunc<'static>>::Input2 as AsColumnType>::as_column_type();
    ct1.nullable = true;
    ct2.nullable = true;
    (f.into(), ct1, ct2)
}

/// Manual type annotation for binary funcs with polymorphic inputs.
fn eb_manual(
    f: impl Into<BinaryFunc>,
    ct1: SqlColumnType,
    ct2: SqlColumnType,
) -> (BinaryFunc, SqlColumnType, SqlColumnType) {
    (f.into(), ct1, ct2)
}

/// Shorthand: nullable SqlColumnType from a SqlScalarType.
fn ct(st: SqlScalarType) -> SqlColumnType {
    st.nullable(true)
}

fn list(el: SqlScalarType) -> SqlColumnType {
    ct(SqlScalarType::List {
        element_type: Box::new(el),
        custom_id: None,
    })
}

fn array(el: SqlScalarType) -> SqlColumnType {
    ct(SqlScalarType::Array(Box::new(el)))
}

fn map(val: SqlScalarType) -> SqlColumnType {
    ct(SqlScalarType::Map {
        value_type: Box::new(val),
        custom_id: None,
    })
}

fn range(el: SqlScalarType) -> SqlColumnType {
    ct(SqlScalarType::Range {
        element_type: Box::new(el),
    })
}

// ---------------------------------------------------------------------------
// UnaryFunc: eager (type-correct testing via DatumType system)
// ---------------------------------------------------------------------------

fn eager_unary_funcs() -> Vec<(UnaryFunc, SqlColumnType)> {
    vec![
        // Logic / null — Datum<'a> input
        eu(Not),
        eu_manual(IsNull, ct(SqlScalarType::Int32)),
        eu_manual(IsTrue, ct(SqlScalarType::Bool)),
        eu_manual(IsFalse, ct(SqlScalarType::Bool)),
        // Bitwise NOT
        eu(BitNotInt16),
        eu(BitNotInt32),
        eu(BitNotInt64),
        eu(BitNotUint16),
        eu(BitNotUint32),
        eu(BitNotUint64),
        // Negation
        eu(NegInt16),
        eu(NegInt32),
        eu(NegInt64),
        eu(NegFloat32),
        eu(NegFloat64),
        eu(NegNumeric),
        eu(NegInterval),
        // Abs
        eu(AbsInt16),
        eu(AbsInt32),
        eu(AbsInt64),
        eu(AbsFloat32),
        eu(AbsFloat64),
        eu(AbsNumeric),
        // Math
        eu(SqrtFloat64),
        eu(SqrtNumeric),
        eu(CbrtFloat64),
        eu(CeilFloat32),
        eu(CeilFloat64),
        eu(CeilNumeric),
        eu(FloorFloat32),
        eu(FloorFloat64),
        eu(FloorNumeric),
        eu(RoundFloat32),
        eu(RoundFloat64),
        eu(RoundNumeric),
        eu(TruncFloat32),
        eu(TruncFloat64),
        eu(TruncNumeric),
        eu(Log10),
        eu(Log10Numeric),
        eu(Ln),
        eu(LnNumeric),
        eu(Exp),
        eu(ExpNumeric),
        // Trig
        eu(Cos),
        eu(Acos),
        eu(Cosh),
        eu(Acosh),
        eu(Sin),
        eu(Asin),
        eu(Sinh),
        eu(Asinh),
        eu(Tan),
        eu(Atan),
        eu(Tanh),
        eu(Atanh),
        eu(Cot),
        eu(Degrees),
        eu(Radians),
        // String
        eu(Ascii),
        eu(BitCountBytes),
        eu(BitLengthBytes),
        eu(BitLengthString),
        eu(ByteLengthBytes),
        eu(ByteLengthString),
        eu(CharLength),
        eu(Chr),
        eu(Upper),
        eu(Lower),
        eu(Initcap),
        eu(TrimWhitespace),
        eu(TrimLeadingWhitespace),
        eu(TrimTrailingWhitespace),
        eu(Reverse),
        eu(PgSizePretty),
        // Bool -> X casts
        eu(CastBoolToString),
        eu(CastBoolToStringNonstandard),
        eu(CastBoolToInt32),
        eu(CastBoolToInt64),
        // Int16 -> X
        eu(CastInt16ToFloat32),
        eu(CastInt16ToFloat64),
        eu(CastInt16ToInt32),
        eu(CastInt16ToInt64),
        eu(CastInt16ToUint16),
        eu(CastInt16ToUint32),
        eu(CastInt16ToUint64),
        eu(CastInt16ToString),
        eu(CastInt16ToNumeric(None)),
        // Int32 -> X
        eu(CastInt32ToBool),
        eu(CastInt32ToFloat32),
        eu(CastInt32ToFloat64),
        eu(CastInt32ToOid),
        eu(CastInt32ToPgLegacyChar),
        eu(CastInt32ToInt16),
        eu(CastInt32ToInt64),
        eu(CastInt32ToUint16),
        eu(CastInt32ToUint32),
        eu(CastInt32ToUint64),
        eu(CastInt32ToString),
        eu(CastInt32ToNumeric(None)),
        // Int64 -> X
        eu(CastInt64ToInt16),
        eu(CastInt64ToInt32),
        eu(CastInt64ToUint16),
        eu(CastInt64ToUint32),
        eu(CastInt64ToUint64),
        eu(CastInt64ToBool),
        eu(CastInt64ToNumeric(None)),
        eu(CastInt64ToFloat32),
        eu(CastInt64ToFloat64),
        eu(CastInt64ToOid),
        eu(CastInt64ToString),
        // Uint16 -> X
        eu(CastUint16ToUint32),
        eu(CastUint16ToUint64),
        eu(CastUint16ToInt16),
        eu(CastUint16ToInt32),
        eu(CastUint16ToInt64),
        eu(CastUint16ToNumeric(None)),
        eu(CastUint16ToFloat32),
        eu(CastUint16ToFloat64),
        eu(CastUint16ToString),
        // Uint32 -> X
        eu(CastUint32ToUint16),
        eu(CastUint32ToUint64),
        eu(CastUint32ToInt16),
        eu(CastUint32ToInt32),
        eu(CastUint32ToInt64),
        eu(CastUint32ToNumeric(None)),
        eu(CastUint32ToFloat32),
        eu(CastUint32ToFloat64),
        eu(CastUint32ToString),
        // Uint64 -> X
        eu(CastUint64ToUint16),
        eu(CastUint64ToUint32),
        eu(CastUint64ToInt16),
        eu(CastUint64ToInt32),
        eu(CastUint64ToInt64),
        eu(CastUint64ToNumeric(None)),
        eu(CastUint64ToFloat32),
        eu(CastUint64ToFloat64),
        eu(CastUint64ToString),
        // Float32 -> X
        eu(CastFloat32ToInt16),
        eu(CastFloat32ToInt32),
        eu(CastFloat32ToInt64),
        eu(CastFloat32ToUint16),
        eu(CastFloat32ToUint32),
        eu(CastFloat32ToUint64),
        eu(CastFloat32ToFloat64),
        eu(CastFloat32ToString),
        eu(CastFloat32ToNumeric(None)),
        // Float64 -> X
        eu(CastFloat64ToInt16),
        eu(CastFloat64ToInt32),
        eu(CastFloat64ToInt64),
        eu(CastFloat64ToUint16),
        eu(CastFloat64ToUint32),
        eu(CastFloat64ToUint64),
        eu(CastFloat64ToFloat32),
        eu(CastFloat64ToString),
        eu(CastFloat64ToNumeric(None)),
        // Numeric -> X
        eu(CastNumericToFloat32),
        eu(CastNumericToFloat64),
        eu(CastNumericToInt16),
        eu(CastNumericToInt32),
        eu(CastNumericToInt64),
        eu(CastNumericToUint16),
        eu(CastNumericToUint32),
        eu(CastNumericToUint64),
        eu(CastNumericToString),
        // String -> X
        eu(CastStringToBool),
        eu(CastStringToPgLegacyChar),
        eu(CastStringToPgLegacyName),
        eu(CastStringToBytes),
        eu(CastStringToInt16),
        eu(CastStringToInt32),
        eu(CastStringToInt64),
        eu(CastStringToUint16),
        eu(CastStringToUint32),
        eu(CastStringToUint64),
        eu(CastStringToFloat32),
        eu(CastStringToFloat64),
        eu(CastStringToDate),
        eu(CastStringToTime),
        eu(CastStringToTimestamp(None)),
        eu(CastStringToTimestampTz(None)),
        eu(CastStringToInterval),
        eu(CastStringToNumeric(None)),
        eu(CastStringToUuid),
        eu(CastStringToJsonb),
        eu(CastStringToOid),
        // Bytes -> X
        eu(CastBytesToString),
        // Date -> X
        eu(CastDateToTimestamp(None)),
        eu(CastDateToTimestampTz(None)),
        eu(CastDateToString),
        // Time -> X
        eu(CastTimeToInterval),
        eu(CastTimeToString),
        // Timestamp -> X
        eu(CastTimestampToDate),
        eu(CastTimestampToTimestampTz {
            from: None,
            to: None,
        }),
        eu(CastTimestampToString),
        eu(CastTimestampToTime),
        // TimestampTz -> X
        eu(CastTimestampTzToDate),
        eu(CastTimestampTzToTimestamp {
            from: None,
            to: None,
        }),
        eu(CastTimestampTzToString),
        eu(CastTimestampTzToTime),
        // Interval -> X
        eu(CastIntervalToString),
        eu(CastIntervalToTime),
        // PgLegacyChar -> X
        eu(CastPgLegacyCharToString),
        eu(CastPgLegacyCharToChar),
        eu(CastPgLegacyCharToVarChar),
        eu(CastPgLegacyCharToInt32),
        // Char/VarChar -> String
        eu(CastCharToString),
        eu(CastVarCharToString),
        eu(PadChar { length: None }),
        // OID / Reg* casts
        eu(CastOidToInt32),
        eu(CastOidToInt64),
        eu(CastOidToString),
        eu(CastOidToRegClass),
        eu(CastRegClassToOid),
        eu(CastOidToRegProc),
        eu(CastRegProcToOid),
        eu(CastOidToRegType),
        eu(CastRegTypeToOid),
        // Jsonb -> X (now safe with typed Jsonb datums)
        eu(CastJsonbToString),
        eu(CastJsonbableToJsonb),
        eu(CastJsonbToInt16),
        eu(CastJsonbToInt32),
        eu(CastJsonbToInt64),
        eu(CastJsonbToFloat32),
        eu(CastJsonbToFloat64),
        eu(CastJsonbToNumeric(None)),
        eu(CastJsonbToBool),
        eu(JsonbArrayLength),
        eu(JsonbTypeof),
        eu(JsonbStripNulls),
        eu(JsonbPretty),
        // UUID -> X
        eu(CastUuidToString),
        // MzTimestamp
        eu(CastMzTimestampToString),
        eu(CastMzTimestampToTimestamp),
        eu(CastMzTimestampToTimestampTz),
        eu(CastStringToMzTimestamp),
        eu(CastUint64ToMzTimestamp),
        eu(CastUint32ToMzTimestamp),
        eu(CastInt64ToMzTimestamp),
        eu(CastInt32ToMzTimestamp),
        eu(CastNumericToMzTimestamp),
        eu(CastTimestampToMzTimestamp),
        eu(CastTimestampTzToMzTimestamp),
        eu(CastDateToMzTimestamp),
        eu(StepMzTimestamp),
        // Extract / DatePart
        eu(ExtractInterval(DateTimeUnits::Epoch)),
        eu(ExtractTime(DateTimeUnits::Epoch)),
        eu(ExtractTimestamp(DateTimeUnits::Epoch)),
        eu(ExtractTimestampTz(DateTimeUnits::Epoch)),
        eu(ExtractDate(DateTimeUnits::Epoch)),
        eu(DatePartInterval(DateTimeUnits::Epoch)),
        eu(DatePartTime(DateTimeUnits::Epoch)),
        eu(DatePartTimestamp(DateTimeUnits::Epoch)),
        eu(DatePartTimestampTz(DateTimeUnits::Epoch)),
        // DateTrunc
        eu(DateTruncTimestamp(DateTimeUnits::Day)),
        eu(DateTruncTimestampTz(DateTimeUnits::Day)),
        // Timezone
        eu(TimezoneTimestamp(Timezone::Tz(chrono_tz::UTC))),
        eu(TimezoneTimestampTz(Timezone::Tz(chrono_tz::UTC))),
        eu(TimezoneTime {
            tz: Timezone::Tz(chrono_tz::UTC),
            wall_time: NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        }),
        // Timestamp precision (from != to required by soft_assert)
        eu(AdjustTimestampPrecision {
            from: None,
            to: Some(3i64.try_into().unwrap()),
        }),
        eu(AdjustTimestampTzPrecision {
            from: None,
            to: Some(3i64.try_into().unwrap()),
        }),
        // Numeric scale
        eu(AdjustNumericScale(
            NumericMaxScale::try_from(0i64).unwrap(),
        )),
        // ToTimestamp
        eu(ToTimestamp),
        // Justify
        eu(JustifyDays),
        eu(JustifyHours),
        eu(JustifyInterval),
        // Range — Range<Datum<'a>> input
        eu_manual(RangeEmpty, range(SqlScalarType::Int32)),
        eu_manual(RangeLowerInc, range(SqlScalarType::Int32)),
        eu_manual(RangeUpperInc, range(SqlScalarType::Int32)),
        eu_manual(RangeLowerInf, range(SqlScalarType::Int32)),
        eu_manual(RangeUpperInf, range(SqlScalarType::Int32)),
        // ACL
        eu(MzAclItemGrantor),
        eu(MzAclItemGrantee),
        eu(MzAclItemPrivileges),
        eu(MzFormatPrivileges),
        eu(MzValidatePrivileges),
        eu(MzValidateRolePrivilege),
        eu(AclItemGrantor),
        eu(AclItemGrantee),
        eu(AclItemPrivileges),
        // Hashing
        eu(Crc32Bytes),
        eu(Crc32String),
        eu(KafkaMurmur2Bytes),
        eu(KafkaMurmur2String),
        eu(SeahashBytes),
        eu(SeahashString),
        // Misc — Datum<'a> / DatumList<'a> input
        eu_manual(PgColumnSize, ct(SqlScalarType::Int32)),
        eu_manual(MzRowSize, list(SqlScalarType::Int32)),
        eu(MzTypeName),
        eu(TryParseMonotonicIso8601Timestamp),
    ]
}

// ---------------------------------------------------------------------------
// UnaryFunc: lazy (hand-written LazyUnaryFunc — tested with type-correct datums)
// ---------------------------------------------------------------------------

fn lazy_unary_funcs() -> Vec<(UnaryFunc, SqlScalarType)> {
    vec![
        (QuoteIdent.into(), SqlScalarType::String),
        (
            ListLength.into(),
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::Int32),
                custom_id: None,
            },
        ),
        (
            MapLength.into(),
            SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::String),
                custom_id: None,
            },
        ),
        (
            RecordGet(0).into(),
            SqlScalarType::Record {
                fields: vec![(
                    "f1".into(),
                    mz_repr::SqlColumnType {
                        scalar_type: SqlScalarType::Int32,
                        nullable: true,
                    },
                )]
                .into(),
                custom_id: None,
            },
        ),
        (
            CastRecordToString {
                ty: SqlScalarType::Record {
                    fields: vec![].into(),
                    custom_id: None,
                },
            }
            .into(),
            SqlScalarType::Record {
                fields: vec![].into(),
                custom_id: None,
            },
        ),
        (
            CastArrayToString {
                ty: SqlScalarType::Array(Box::new(SqlScalarType::Int32)),
            }
            .into(),
            SqlScalarType::Array(Box::new(SqlScalarType::Int32)),
        ),
        (
            CastListToString {
                ty: SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::Int32),
                    custom_id: None,
                },
            }
            .into(),
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::Int32),
                custom_id: None,
            },
        ),
        (
            CastMapToString {
                ty: SqlScalarType::Map {
                    value_type: Box::new(SqlScalarType::String),
                    custom_id: None,
                },
            }
            .into(),
            SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::String),
                custom_id: None,
            },
        ),
        (
            CastRangeToString {
                ty: SqlScalarType::Range {
                    element_type: Box::new(SqlScalarType::Int32),
                },
            }
            .into(),
            SqlScalarType::Range {
                element_type: Box::new(SqlScalarType::Int32),
            },
        ),
        (
            RangeLower.into(),
            SqlScalarType::Range {
                element_type: Box::new(SqlScalarType::Int32),
            },
        ),
        (
            RangeUpper.into(),
            SqlScalarType::Range {
                element_type: Box::new(SqlScalarType::Int32),
            },
        ),
        (CastInt2VectorToArray.into(), SqlScalarType::Int2Vector),
        (CastInt2VectorToString.into(), SqlScalarType::Int2Vector),
        (CastStringToInt2Vector.into(), SqlScalarType::String),
    ]
}

// ---------------------------------------------------------------------------
// BinaryFunc: all non-side-effecting variants (type-aware)
// ---------------------------------------------------------------------------

fn all_binary_funcs() -> Vec<(BinaryFunc, SqlColumnType, SqlColumnType)> {
    vec![
        // Arithmetic
        eb(AddInt16),
        eb(AddInt32),
        eb(AddInt64),
        eb(AddUint16),
        eb(AddUint32),
        eb(AddUint64),
        eb(AddFloat32),
        eb(AddFloat64),
        eb_manual(AddNumeric, ct(SqlScalarType::Numeric { max_scale: None }), ct(SqlScalarType::Numeric { max_scale: None })),
        eb(AddInterval),
        eb(SubInt16),
        eb(SubInt32),
        eb(SubInt64),
        eb(SubUint16),
        eb(SubUint32),
        eb(SubUint64),
        eb(SubFloat32),
        eb(SubFloat64),
        eb_manual(SubNumeric, ct(SqlScalarType::Numeric { max_scale: None }), ct(SqlScalarType::Numeric { max_scale: None })),
        eb(SubInterval),
        eb(MulInt16),
        eb(MulInt32),
        eb(MulInt64),
        eb(MulUint16),
        eb(MulUint32),
        eb(MulUint64),
        eb(MulFloat32),
        eb(MulFloat64),
        eb_manual(MulNumeric, ct(SqlScalarType::Numeric { max_scale: None }), ct(SqlScalarType::Numeric { max_scale: None })),
        eb(MulInterval),
        eb(DivInt16),
        eb(DivInt32),
        eb(DivInt64),
        eb(DivUint16),
        eb(DivUint32),
        eb(DivUint64),
        eb(DivFloat32),
        eb(DivFloat64),
        eb_manual(DivNumeric, ct(SqlScalarType::Numeric { max_scale: None }), ct(SqlScalarType::Numeric { max_scale: None })),
        eb(DivInterval),
        eb(ModInt16),
        eb(ModInt32),
        eb(ModInt64),
        eb(ModUint16),
        eb(ModUint32),
        eb(ModUint64),
        eb(ModFloat32),
        eb(ModFloat64),
        eb_manual(ModNumeric, ct(SqlScalarType::Numeric { max_scale: None }), ct(SqlScalarType::Numeric { max_scale: None })),
        // Bitwise
        eb(BitAndInt16),
        eb(BitAndInt32),
        eb(BitAndInt64),
        eb(BitAndUint16),
        eb(BitAndUint32),
        eb(BitAndUint64),
        eb(BitOrInt16),
        eb(BitOrInt32),
        eb(BitOrInt64),
        eb(BitOrUint16),
        eb(BitOrUint32),
        eb(BitOrUint64),
        eb(BitXorInt16),
        eb(BitXorInt32),
        eb(BitXorInt64),
        eb(BitXorUint16),
        eb(BitXorUint32),
        eb(BitXorUint64),
        eb(BitShiftLeftInt16),
        eb(BitShiftLeftInt32),
        eb(BitShiftLeftInt64),
        eb(BitShiftLeftUint16),
        eb(BitShiftLeftUint32),
        eb(BitShiftLeftUint64),
        eb(BitShiftRightInt16),
        eb(BitShiftRightInt32),
        eb(BitShiftRightInt64),
        eb(BitShiftRightUint16),
        eb(BitShiftRightUint32),
        eb(BitShiftRightUint64),
        // Comparison — ExcludeNull<Datum<'a>> inputs
        eb_manual(Eq, ct(SqlScalarType::Int32), ct(SqlScalarType::Int32)),
        eb_manual(NotEq, ct(SqlScalarType::Int32), ct(SqlScalarType::Int32)),
        eb_manual(Lt, ct(SqlScalarType::Int32), ct(SqlScalarType::Int32)),
        eb_manual(Lte, ct(SqlScalarType::Int32), ct(SqlScalarType::Int32)),
        eb_manual(Gt, ct(SqlScalarType::Int32), ct(SqlScalarType::Int32)),
        eb_manual(Gte, ct(SqlScalarType::Int32), ct(SqlScalarType::Int32)),
        // Timestamp
        eb(AddTimestampInterval),
        eb(AddTimestampTzInterval),
        eb(AddDateInterval),
        eb(AddDateTime),
        eb(AddTimeInterval),
        eb(SubTimestamp),
        eb(SubTimestampTz),
        eb(SubTimestampInterval),
        eb(SubTimestampTzInterval),
        eb(SubDate),
        eb(SubDateInterval),
        eb(SubTime),
        eb(SubTimeInterval),
        eb(AgeTimestamp),
        eb(AgeTimestampTz),
        // Round numeric
        eb_manual(RoundNumericBinary, ct(SqlScalarType::Numeric { max_scale: None }), ct(SqlScalarType::Int64)),
        // String
        eb(TextConcatBinary),
        eb(Left),
        eb(Right),
        eb(Position),
        eb(RepeatString),
        eb(Trim),
        eb(TrimLeading),
        eb(TrimTrailing),
        eb(LikeEscape),
        eb(StartsWith),
        eb(Normalize),
        eb(EncodedBytesCharLength),
        eb(ConvertFrom),
        // JSON — now safe with typed Jsonb datums
        eb(JsonbGetInt64),
        eb(JsonbGetInt64Stringify),
        eb(JsonbGetString),
        eb(JsonbGetStringStringify),
        eb_manual(
            JsonbGetPath,
            ct(SqlScalarType::Jsonb),
            list(SqlScalarType::String),
        ),
        eb_manual(
            JsonbGetPathStringify,
            ct(SqlScalarType::Jsonb),
            list(SqlScalarType::String),
        ),
        eb_manual(JsonbContainsString, ct(SqlScalarType::Jsonb), ct(SqlScalarType::String)),
        eb_manual(JsonbConcat, ct(SqlScalarType::Jsonb), ct(SqlScalarType::Jsonb)),
        eb_manual(JsonbContainsJsonb, ct(SqlScalarType::Jsonb), ct(SqlScalarType::Jsonb)),
        eb_manual(JsonbDeleteInt64, ct(SqlScalarType::Jsonb), ct(SqlScalarType::Int64)),
        eb_manual(JsonbDeleteString, ct(SqlScalarType::Jsonb), ct(SqlScalarType::String)),
        // Map — DatumMap<'a> inputs
        eb_manual(
            MapContainsKey,
            map(SqlScalarType::String),
            ct(SqlScalarType::String),
        ),
        eb_manual(
            MapGetValue,
            map(SqlScalarType::String),
            ct(SqlScalarType::String),
        ),
        eb_manual(
            MapContainsAllKeys,
            map(SqlScalarType::String),
            list(SqlScalarType::String),
        ),
        eb_manual(
            MapContainsAnyKeys,
            map(SqlScalarType::String),
            list(SqlScalarType::String),
        ),
        eb_manual(
            MapContainsMap,
            map(SqlScalarType::String),
            map(SqlScalarType::String),
        ),
        // Array — Array<'a> inputs
        eb_manual(
            ArrayContains,
            array(SqlScalarType::Int32),
            ct(SqlScalarType::Int32),
        ),
        eb_manual(
            ArrayContainsArray,
            array(SqlScalarType::Int32),
            array(SqlScalarType::Int32),
        ),
        eb_manual(
            ArrayContainsArrayRev,
            array(SqlScalarType::Int32),
            array(SqlScalarType::Int32),
        ),
        eb_manual(
            ArrayLength,
            array(SqlScalarType::Int32),
            ct(SqlScalarType::Int64),
        ),
        eb_manual(
            ArrayLower,
            array(SqlScalarType::Int32),
            ct(SqlScalarType::Int64),
        ),
        eb_manual(
            ArrayUpper,
            array(SqlScalarType::Int32),
            ct(SqlScalarType::Int64),
        ),
        eb_manual(
            ArrayRemove,
            array(SqlScalarType::Int32),
            ct(SqlScalarType::Int32),
        ),
        eb_manual(
            ArrayArrayConcat,
            array(SqlScalarType::Int32),
            array(SqlScalarType::Int32),
        ),
        // List — DatumList<'a> inputs
        eb_manual(
            ListListConcat,
            list(SqlScalarType::Int32),
            list(SqlScalarType::Int32),
        ),
        eb_manual(
            ListElementConcat,
            list(SqlScalarType::Int32),
            ct(SqlScalarType::Int32),
        ),
        eb_manual(
            ElementListConcat,
            ct(SqlScalarType::Int32),
            list(SqlScalarType::Int32),
        ),
        eb_manual(
            ListRemove,
            list(SqlScalarType::Int32),
            ct(SqlScalarType::Int32),
        ),
        eb_manual(
            ListLengthMax { max_layer: 3 },
            list(SqlScalarType::Int32),
            ct(SqlScalarType::Int64),
        ),
        eb_manual(
            ListContainsList,
            list(SqlScalarType::Int32),
            list(SqlScalarType::Int32),
        ),
        eb_manual(
            ListContainsListRev,
            list(SqlScalarType::Int32),
            list(SqlScalarType::Int32),
        ),
        // Crypto
        eb(DigestString),
        eb(DigestBytes),
        eb(Encode),
        eb(Decode),
        eb(ConstantTimeEqBytes),
        eb(ConstantTimeEqString),
        // Math
        eb(Power),
        eb_manual(PowerNumeric, ct(SqlScalarType::Numeric { max_scale: None }), ct(SqlScalarType::Numeric { max_scale: None })),
        eb_manual(LogBaseNumeric, ct(SqlScalarType::Numeric { max_scale: None }), ct(SqlScalarType::Numeric { max_scale: None })),
        eb(GetBit),
        eb(GetByte),
        // Range element-contains — Range<Datum<'a>> inputs
        eb_manual(
            RangeContainsI32,
            range(SqlScalarType::Int32),
            ct(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeContainsI32Rev,
            ct(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeContainsI64,
            range(SqlScalarType::Int64),
            ct(SqlScalarType::Int64),
        ),
        eb_manual(
            RangeContainsI64Rev,
            ct(SqlScalarType::Int64),
            range(SqlScalarType::Int64),
        ),
        eb_manual(
            RangeContainsDate,
            range(SqlScalarType::Date),
            ct(SqlScalarType::Date),
        ),
        eb_manual(
            RangeContainsDateRev,
            ct(SqlScalarType::Date),
            range(SqlScalarType::Date),
        ),
        eb_manual(
            RangeContainsNumeric,
            range(SqlScalarType::Numeric { max_scale: None }),
            ct(SqlScalarType::Numeric { max_scale: None }),
        ),
        eb_manual(
            RangeContainsNumericRev,
            ct(SqlScalarType::Numeric { max_scale: None }),
            range(SqlScalarType::Numeric { max_scale: None }),
        ),
        eb_manual(
            RangeContainsTimestamp,
            range(SqlScalarType::Timestamp { precision: None }),
            ct(SqlScalarType::Timestamp { precision: None }),
        ),
        eb_manual(
            RangeContainsTimestampRev,
            ct(SqlScalarType::Timestamp { precision: None }),
            range(SqlScalarType::Timestamp { precision: None }),
        ),
        eb_manual(
            RangeContainsTimestampTz,
            range(SqlScalarType::TimestampTz { precision: None }),
            ct(SqlScalarType::TimestampTz { precision: None }),
        ),
        eb_manual(
            RangeContainsTimestampTzRev,
            ct(SqlScalarType::TimestampTz { precision: None }),
            range(SqlScalarType::TimestampTz { precision: None }),
        ),
        // Range-range ops — Range<Datum<'a>> × Range<Datum<'a>>
        eb_manual(
            RangeContainsRange,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeContainsRangeRev,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeOverlaps,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeAfter,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeBefore,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeOverleft,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeOverright,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeAdjacent,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeUnion,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeIntersection,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        eb_manual(
            RangeDifference,
            range(SqlScalarType::Int32),
            range(SqlScalarType::Int32),
        ),
        // Other
        eb(UuidGenerateV5),
        eb(MzAclItemContainsPrivilege),
        eb(ParseIdent),
        eb(PrettySql),
        eb_manual(
            MzRenderTypmod,
            ct(SqlScalarType::Oid),
            ct(SqlScalarType::Int32),
        ),
        // Regex / like
        eb(IsLikeMatchCaseSensitive),
        eb(IsLikeMatchCaseInsensitive),
        eb(IsRegexpMatchCaseSensitive),
        eb(IsRegexpMatchCaseInsensitive),
        eb(RegexpReplace {
            regex: Regex::new(".", false).unwrap(),
            limit: 0,
        }),
    ]
}

// ---------------------------------------------------------------------------
// Test: no eager UnaryFunc panics on type-correct datums
// ---------------------------------------------------------------------------

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn fuzz_unary_eager_type_correct() {
    let funcs = eager_unary_funcs();
    let expr = MirScalarExpr::column(0);
    let mut failures = Vec::new();

    for (func, input_ct) in &funcs {
        // Test with Null
        {
            let arena = RowArena::new();
            let result = catch_unwind(AssertUnwindSafe(|| {
                func.eval(&[Datum::Null], &arena, &expr)
            }));
            if result.is_err() {
                failures.push(format!("{func}: panicked on Datum::Null"));
            }
        }

        // Test with interesting datums of the correct type
        let arena = RowArena::new();
        for datum in input_ct.scalar_type.interesting_datums() {
            let result = catch_unwind(AssertUnwindSafe(|| {
                func.eval(&[datum], &arena, &expr)
            }));
            if result.is_err() {
                failures.push(format!("{func}: panicked on {datum:?}"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "Functions that panicked:\n{}",
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Test: no BinaryFunc panics on type-correct datum pairs
// ---------------------------------------------------------------------------

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn fuzz_binary_type_correct() {
    let funcs = all_binary_funcs();
    let a_expr = MirScalarExpr::column(0);
    let b_expr = MirScalarExpr::column(1);
    let mut failures = Vec::new();

    for (func, ct1, ct2) in &funcs {
        let datums1: Vec<Datum<'static>> = ct1.scalar_type.interesting_datums().collect();
        let datums2: Vec<Datum<'static>> = ct2.scalar_type.interesting_datums().collect();

        // Null × Null
        {
            let arena = RowArena::new();
            let result = catch_unwind(AssertUnwindSafe(|| {
                func.eval(&[Datum::Null, Datum::Null], &arena, &a_expr, &b_expr)
            }));
            if result.is_err() {
                failures.push(format!("{func}: panicked on (Null, Null)"));
            }
        }

        // d1 × d2 for all interesting datums of correct types
        for d1 in &datums1 {
            // d1 × Null
            {
                let arena = RowArena::new();
                let result = catch_unwind(AssertUnwindSafe(|| {
                    func.eval(&[*d1, Datum::Null], &arena, &a_expr, &b_expr)
                }));
                if result.is_err() {
                    failures.push(format!("{func}: panicked on ({d1:?}, Null)"));
                }
            }

            for d2 in &datums2 {
                let arena = RowArena::new();
                let result = catch_unwind(AssertUnwindSafe(|| {
                    func.eval(&[*d1, *d2], &arena, &a_expr, &b_expr)
                }));
                if result.is_err() {
                    failures.push(format!("{func}: panicked on ({d1:?}, {d2:?})"));
                }
            }
        }

        // Null × d2
        for d2 in &datums2 {
            let arena = RowArena::new();
            let result = catch_unwind(AssertUnwindSafe(|| {
                func.eval(&[Datum::Null, *d2], &arena, &a_expr, &b_expr)
            }));
            if result.is_err() {
                failures.push(format!("{func}: panicked on (Null, {d2:?})"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "Functions that panicked:\n{}",
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Test: lazy UnaryFunc with type-correct datums
// ---------------------------------------------------------------------------

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn fuzz_unary_lazy_type_correct() {
    let funcs = lazy_unary_funcs();
    let expr = MirScalarExpr::column(0);
    let mut failures = Vec::new();

    for (func, input_type) in &funcs {
        // Test with Null (all funcs should handle null)
        {
            let arena = RowArena::new();
            let result = catch_unwind(AssertUnwindSafe(|| {
                func.eval(&[Datum::Null], &arena, &expr)
            }));
            if result.is_err() {
                failures.push(format!("{func}: panicked on Datum::Null"));
            }
        }

        // Test with interesting datums of the correct type
        let arena = RowArena::new();
        for datum in input_type.interesting_datums() {
            let result = catch_unwind(AssertUnwindSafe(|| {
                func.eval(&[datum], &arena, &expr)
            }));
            if result.is_err() {
                failures.push(format!("{func}: panicked on {datum:?}"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "Lazy functions that panicked on correct types:\n{}",
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Proptest: random type-correct datums against all eager functions
// ---------------------------------------------------------------------------

#[cfg(test)]
mod proptest_fuzz {
    use super::*;
    use mz_repr::arb_datum_for_column;
    use proptest::prelude::*;
    use proptest::test_runner::{TestCaseError, TestRunner};

    #[test]
    #[cfg_attr(miri, ignore)]
    fn fuzz_unary_random() {
        let mut runner = TestRunner::new(ProptestConfig::with_cases(200));
        let funcs = eager_unary_funcs();
        let expr = MirScalarExpr::column(0);

        for (func, input_ct) in &funcs {
            let strat = arb_datum_for_column(input_ct.clone());
            runner
                .run(&strat, |prop_datum| {
                    let arena = RowArena::new();
                    let d: Datum = (&prop_datum).into();
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        func.eval(&[d], &arena, &expr)
                    }));
                    if result.is_err() {
                        return Err(TestCaseError::fail(format!(
                            "{func} panicked on {d:?}"
                        )));
                    }
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("{func}: {e}"));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn fuzz_binary_random() {
        let mut runner = TestRunner::new(ProptestConfig::with_cases(200));
        let funcs = all_binary_funcs();
        let a_expr = MirScalarExpr::column(0);
        let b_expr = MirScalarExpr::column(1);

        for (func, ct1, ct2) in &funcs {
            let strat = (
                arb_datum_for_column(ct1.clone()),
                arb_datum_for_column(ct2.clone()),
            );
            runner
                .run(&strat, |(pd1, pd2)| {
                    let arena = RowArena::new();
                    let da: Datum = (&pd1).into();
                    let db: Datum = (&pd2).into();
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        func.eval(&[da, db], &arena, &a_expr, &b_expr)
                    }));
                    if result.is_err() {
                        return Err(TestCaseError::fail(format!(
                            "{func} panicked on ({da:?}, {db:?})"
                        )));
                    }
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("{func}: {e}"));
        }
    }
}
