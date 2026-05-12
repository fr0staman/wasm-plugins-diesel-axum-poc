use axum::{Json, http::StatusCode};

pub type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

use serde::Serialize;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub struct OkResponse {
    pub ok: bool,
}
