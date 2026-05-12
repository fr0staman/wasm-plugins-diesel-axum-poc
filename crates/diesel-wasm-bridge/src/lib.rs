mod row;
pub use row::RawRow;

/// Compile-time representation of a single plugin migration, created via [`migration!`].
pub struct MigrationSpec {
    pub version: &'static str,
    pub up_sql: &'static str,
    pub down_sql: &'static str,
}

/// Build a [`MigrationSpec`] with SQL embedded at compile time.
///
/// Paths are resolved relative to the file that invokes the macro.
///
/// ```ignore
/// diesel_wasm_bridge::migration!(
///     "V0001__my_table",
///     "../migrations/V0001__my_table/up.sql",
///     "../migrations/V0001__my_table/down.sql",
/// )
/// ```
#[macro_export]
macro_rules! migration {
    ($version:literal, $up:literal, $down:literal $(,)?) => {
        $crate::MigrationSpec {
            version: $version,
            up_sql: ::core::include_str!($up),
            down_sql: ::core::include_str!($down),
        }
    };
}

use diesel::deserialize::FromSqlRow;
use diesel::expression::Expression;
use diesel::pg::Pg;
use diesel::prelude::Selectable;
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{QueryBuilder, QueryFragment};

#[derive(Debug)]
pub enum Error {
    DecodeError,
    DieselError(diesel::result::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("decode error")
    }
}

impl std::error::Error for Error {}
/// Wire representation of a rendered query, matching the WIT `rendered-query` record.
pub struct RenderedQuery {
    pub sql: String,
    pub binds: Vec<Vec<u8>>,
    /// Postgres type OIDs, one per bind value.
    pub bind_types: Vec<u32>,
}

/// Render a Diesel query to its wire representation.
///
/// Only built-in Postgres types are supported; custom type metadata lookup is a no-op.
pub fn render_query<Q>(query: Q) -> Result<RenderedQuery, Error>
where
    Q: QueryFragment<Pg>,
{
    let mut query_builder = diesel::pg::PgQueryBuilder::default();
    let mut bind_collector = RawBytesBindCollector::<Pg>::new();
    let mut metadata_lookup = NoopPgMetadataLookup;

    query
        .to_sql(&mut query_builder, &Pg)
        .map_err(Error::DieselError)?;
    query
        .collect_binds(
            &mut bind_collector,
            &mut metadata_lookup as &mut dyn diesel::pg::PgMetadataLookup,
            &Pg,
        )
        .map_err(Error::DieselError)?;

    let sql = query_builder.finish();
    let binds = bind_collector
        .binds
        .into_iter()
        .map(|b| b.unwrap_or_default())
        .collect();
    let bind_types = bind_collector
        .metadata
        .into_iter()
        .map(|m| m.oid().unwrap_or(0))
        .collect();

    Ok(RenderedQuery {
        sql,
        binds,
        bind_types,
    })
}

/// Decode a `db_query` result into a `Vec<T>` using Diesel's `FromSqlRow` machinery.
///
/// The SQL type is inferred automatically from `T`'s [`Selectable<Pg>`] impl,
/// so no manual SQL-type annotation is needed at the call site:
///
/// ```ignore
/// let rows: Vec<MyModel> = diesel_wasm_bridge::decode(
///     host_api::db_query(to_wire(rendered)).await
/// )?;
/// ```
///
/// `E` is the error type returned by `db_query` (e.g. `PluginError`). Rows
/// that fail to deserialize are silently skipped; only the `db_query` error
/// itself is propagated via `?`.
pub fn decode<T>(rows: Vec<Vec<Vec<u8>>>) -> Result<Vec<T>, Error>
where
    T: Selectable<Pg>,
    T: for<'a> FromSqlRow<<<T as Selectable<Pg>>::SelectExpression as Expression>::SqlType, Pg>,
{
    let mut out = Vec::with_capacity(rows.len());

    for row in rows {
        out.push(T::build_from_row(&RawRow(&row)).map_err(|_| Error::DecodeError)?);
    }

    Ok(out)
}

struct NoopPgMetadataLookup;

impl diesel::pg::PgMetadataLookup for NoopPgMetadataLookup {
    fn lookup_type(
        &mut self,
        _type_name: &str,
        _schema: Option<&str>,
    ) -> diesel::pg::PgTypeMetadata {
        diesel::pg::PgTypeMetadata::new(0, 0)
    }
}
