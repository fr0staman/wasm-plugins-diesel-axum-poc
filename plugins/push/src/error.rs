use crate::bindings::myapp::plugin::types::PluginError;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Internal(String),
    #[error(transparent)]
    Db(#[from] diesel_wasm_bridge::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        #[derive(serde::Serialize)]
        struct ErrorBody {
            error: String,
        }

        let (status, msg) = match self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Self::Conflict(m) => (StatusCode::CONFLICT, m),
            Self::NotFound(m) => (StatusCode::NOT_FOUND, m),
            Self::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
            Self::Db(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        };
        (status, Json(ErrorBody { error: msg })).into_response()
    }
}

impl From<PluginError> for AppError {
    fn from(e: PluginError) -> Self {
        match e {
            PluginError::DbError(m) => Self::Internal(format!("db: {m}")),
            PluginError::InvalidInput(m) => Self::BadRequest(m),
            PluginError::NotFound => Self::NotFound("not found".to_string()),
            PluginError::Internal(m) => Self::Internal(m),
        }
    }
}

impl From<AppError> for PluginError {
    fn from(e: AppError) -> Self {
        match e {
            AppError::BadRequest(m) | AppError::Conflict(m) => Self::InvalidInput(m),
            AppError::NotFound(_) => Self::NotFound,
            AppError::Internal(m) => Self::Internal(m),
            AppError::Db(e) => Self::Internal(e.to_string()),
        }
    }
}
