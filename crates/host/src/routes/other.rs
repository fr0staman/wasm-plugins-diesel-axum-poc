use axum::{Json, extract::State};
use serde_json::Value;

use crate::{api::AppState, types::OkResponse};

fn prefix_plugin_paths(openapi_json: &str, plugin_name: &str) -> Result<Value, serde_json::Error> {
    let mut value: Value = serde_json::from_str(openapi_json)?;
    if let Some(paths) = value.get("paths").and_then(|p| p.as_object()) {
        let http_prefix = format!("/p/{}", plugin_name);
        let ws_prefix = format!("/ws/p/{}", plugin_name);
        let sse_prefix = format!("/sse/p/{}", plugin_name);
        let prefixed: serde_json::Map<String, Value> = paths
            .iter()
            .map(|(k, v)| {
                let prefix = if let Some(get_op) = v["get"].as_object() {
                    let responses = &get_op["responses"];
                    if responses["101"].is_object() {
                        &ws_prefix
                    } else if responses["200"]["content"]["text/event-stream"].is_object() {
                        &sse_prefix
                    } else {
                        &http_prefix
                    }
                } else {
                    &http_prefix
                };
                (format!("{}{}", prefix, k), v.clone())
            })
            .collect();
        value["paths"] = Value::Object(prefixed);
    }
    Ok(value)
}

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
        let prefixed = match prefix_plugin_paths(&plugin.openapi_json, &plugin.name) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    plugin = %plugin.name,
                    error = %e,
                    "ignoring invalid OpenAPI fragment from plugin"
                );
                continue;
            }
        };
        match serde_json::from_value(prefixed) {
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
