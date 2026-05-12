use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;
use diesel::{Expression, Selectable, deserialize::FromSqlRow};

use crate::bindings::myapp::plugin::host_api;
use crate::bindings::myapp::plugin::types::RenderedQuery as WitQuery;
use crate::error::AppError;

pub(crate) fn render<Q: QueryFragment<Pg>>(
    query: Q,
) -> Result<diesel_wasm_bridge::RenderedQuery, AppError> {
    diesel_wasm_bridge::render_query(query).map_err(|e| AppError::Internal(e.to_string()))
}

pub(crate) async fn execute<Q: QueryFragment<Pg>>(query: Q) -> Result<u64, AppError> {
    host_api::db_execute(to_wire(render(query)?))
        .await
        .map_err(AppError::from)
}

pub(crate) async fn query<Q: QueryFragment<Pg>, T>(query: Q) -> Result<Vec<T>, AppError>
where
    T: Selectable<Pg>,
    T: for<'a> FromSqlRow<<<T as Selectable<Pg>>::SelectExpression as Expression>::SqlType, Pg>,
{
    let bytes = host_api::db_query(to_wire(render(query)?))
        .await
        .map_err(AppError::from)?;

    Ok(diesel_wasm_bridge::decode(bytes)?)
}

fn to_wire(r: diesel_wasm_bridge::RenderedQuery) -> WitQuery {
    WitQuery {
        sql: r.sql,
        binds: r.binds,
        bind_types: r.bind_types,
    }
}
