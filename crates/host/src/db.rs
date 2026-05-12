use anyhow::Result;
use diesel::row::{Field, Row};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::{Object, Pool};
use diesel_async::{AsyncConnectionCore, AsyncPgConnection, RunQueryDsl};
use futures_util::TryStreamExt;

pub struct DbPool {
    pool: Pool<AsyncPgConnection>,
}

impl DbPool {
    pub async fn new(database_url: &str) -> Result<Self> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
        let pool = Pool::builder(manager).build()?;
        let _conn = pool.get().await?;
        Ok(Self { pool })
    }

    pub async fn get_conn(&self) -> Result<Object<AsyncPgConnection>> {
        self.pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!("pool: {e}"))
    }

    pub async fn execute_raw(
        &self,
        sql: &str,
        // TODO: local roles for plugins
        #[allow(unused)] plugin_name: &str,
        binds: Vec<Vec<u8>>,
        bind_types: Vec<u32>,
    ) -> Result<u64> {
        let sql = inline_binds(sql, &binds, &bind_types);
        let mut conn = self.get_conn().await?;
        let n = diesel::sql_query(&sql).execute(&mut conn).await?;
        Ok(n as u64)
        /*
        let role = format!("SET LOCAL ROLE \"plugin_{}\"", plugin_name);
        (&mut *conn)
            .transaction(async move |conn| {
                diesel::sql_query(&role).execute(conn).await?;
                let n = diesel::sql_query(&sql).execute(conn).await?;
                Ok::<_, diesel::result::Error>(n as u64)
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
        */
    }

    pub async fn query_raw(
        &self,
        sql: &str,
        // TODO: local roles for plugins
        #[allow(unused)] plugin_name: &str,
        binds: Vec<Vec<u8>>,
        bind_types: Vec<u32>,
    ) -> Result<Vec<Vec<Vec<u8>>>> {
        let sql = inline_binds(sql, &binds, &bind_types);
        let mut conn = self.get_conn().await?;
        let stream = AsyncConnectionCore::load(&mut conn, diesel::sql_query(&sql)).await?;
        let rows = stream
            .map_ok(|row| {
                (0..row.field_count())
                    .map(|i| {
                        row.get(i)
                            .and_then(|f| f.value().map(|v| v.as_bytes().to_vec()))
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .try_collect()
            .await?;
        Ok(rows)
        /*
        let role = format!("SET LOCAL ROLE \"plugin_{}\"", plugin_name);
        (&mut *conn)
            .transaction(async move |conn: &mut AsyncPgConnection| {
                diesel::sql_query(&role).execute(conn).await?;
                let stream = AsyncConnectionCore::load(conn, diesel::sql_query(&sql)).await?;
                let rows = stream
                    .map_ok(|row| {
                        (0..row.field_count())
                            .map(|i| {
                                row.get(i)
                                    .and_then(|f| f.value().map(|v| v.as_bytes().to_vec()))
                                    .unwrap_or_default()
                            })
                            .collect::<Vec<_>>()
                    })
                    .try_collect::<Vec<_>>()
                    .await?;
                Ok::<_, diesel::result::Error>(rows)
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
        */
    }
}

/// Decode a single PostgreSQL binary-format bind value to a SQL literal.
///
/// Supports the OIDs that diesel emits for the types used in plugin queries.
/// Falls back to NULL for unknown or malformed values.
fn decode_bind(bytes: &[u8], oid: u32) -> String {
    match oid {
        20 => {
            // int8 — big-endian i64
            bytes
                .try_into()
                .ok()
                .map(i64::from_be_bytes)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        }
        23 => {
            // int4 — big-endian i32
            bytes
                .try_into()
                .ok()
                .map(i32::from_be_bytes)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "NULL".to_string())
        }
        25 | 1043 => {
            // text / varchar — raw UTF-8, wrap in single quotes, escape existing quotes
            match std::str::from_utf8(bytes) {
                Ok(s) => format!("'{}'", s.replace('\'', "''")),
                Err(_) => "NULL".to_string(),
            }
        }
        16 => {
            // bool — single byte, 0 = false
            if bytes.first().copied().unwrap_or(0) == 0 {
                "FALSE".to_string()
            } else {
                "TRUE".to_string()
            }
        }
        _ => "NULL".to_string(),
    }
}

/// Replace `$1`, `$2`, … placeholders in `sql` with decoded literal values.
///
/// Iterates from the highest index down so that `$10` is handled before
/// `$1` and won't be double-substituted.
fn inline_binds(sql: &str, binds: &[Vec<u8>], bind_types: &[u32]) -> String {
    if binds.is_empty() {
        return sql.to_string();
    }
    let mut result = sql.to_string();
    for i in (0..binds.len()).rev() {
        let placeholder = format!("${}", i + 1);
        let oid = bind_types.get(i).copied().unwrap_or(0);
        let literal = decode_bind(&binds[i], oid);
        result = result.replace(&placeholder, &literal);
    }
    result
}
