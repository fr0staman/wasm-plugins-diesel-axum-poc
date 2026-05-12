use diesel::pg::{Pg, PgValue, TypeOidLookup};
use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};
use std::num::NonZeroU32;

/// A placeholder OID used when constructing `PgValue`.
/// Diesel's `FromSql` impls for built-in types (integers, text, dates,
/// timestamps) only read the raw bytes and never validate the OID at
/// deserialization time, so any non-zero value is acceptable here.
struct AnyOid;
impl TypeOidLookup for AnyOid {
    fn lookup(&self) -> NonZeroU32 {
        NonZeroU32::MIN
    }
}

/// A single column value backed by raw Postgres binary bytes.
pub struct RawField<'a>(&'a [u8]);

impl<'a> Field<'a, Pg> for RawField<'a> {
    fn field_name(&self) -> Option<&str> {
        None
    }

    fn value(&self) -> Option<PgValue<'_>> {
        static OID: AnyOid = AnyOid;
        Some(PgValue::new(self.0, &OID))
    }
}

/// A Diesel [`Row<'_, Pg>`] backed by one row from `host_api::db_query`.
///
/// `db_query` returns `list<list<list<u8>>>` (rows × columns × bytes).
/// Construct `RawRow` from a single `&[Vec<u8>]` row and pass it to
/// [`diesel::deserialize::FromSqlRow::build_from_row`] to deserialize into
/// any struct that derives `Queryable + Selectable`.
///
/// # Example
/// ```ignore
/// use diesel::deserialize::FromSqlRow;
/// use diesel::pg::Pg;
/// use diesel::sql_types::{BigInt, Text};
/// use diesel_wasm_bridge::RawRow;
///
/// type MySqlTy = (BigInt, Text);
///
/// for raw_row in result.iter() {
///     let model = <MyModel as FromSqlRow<MySqlTy, Pg>>::build_from_row(&RawRow(raw_row))?;
/// }
/// ```
pub struct RawRow<'a>(pub &'a [Vec<u8>]);
impl RowSealed for RawRow<'_> {}

impl<'a> Row<'a, Pg> for RawRow<'a> {
    type Field<'f>
        = RawField<'f>
    where
        Self: 'f,
        'a: 'f;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.0.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let i = self.idx(idx)?;
        Some(RawField(&self.0[i]))
    }

    fn partial_row(&self, range: std::ops::Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for RawRow<'_> {
    fn idx(&self, idx: usize) -> Option<usize> {
        (idx < self.0.len()).then_some(idx)
    }
}

impl<'a> RowIndex<&'a str> for RawRow<'_> {
    fn idx(&self, _: &'a str) -> Option<usize> {
        None
    }
}
