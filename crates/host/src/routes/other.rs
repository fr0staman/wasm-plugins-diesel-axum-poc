use axum::{Json, extract::State};

use crate::{api::AppState, types::OkResponse};

#[utoipa::path(
    get,
    path = "/api-docs/openapi.json",
    responses((status = 200, description = "OpenAPI"))
)]
pub async fn openapi_json(
    State(app): State<AppState>,
    openapi: utoipa::openapi::OpenApi,
) -> Json<utoipa::openapi::OpenApi> {
    let mut spec = openapi;
    let rt = &app.runtime;
    for plugin in &rt.plugins {
        if plugin.openapi_json.is_empty() {
            continue;
        }
        match serde_json::from_str(&plugin.openapi_json) {
            Ok(plugin_spec) => spec.merge(plugin_spec),
            Err(e) => tracing::warn!(
                plugin = %plugin.name,
                error = %e,
                "ignoring invalid OpenAPI fragment from plugin"
            ),
        }
    }
    Json(spec)
}

#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, description = "Service is up", body = OkResponse))
)]
pub async fn health() -> Json<OkResponse> {
    Json(OkResponse { ok: true })
}
