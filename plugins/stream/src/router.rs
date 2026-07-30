use axum::{Router, http::StatusCode};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

#[derive(OpenApi)]
#[openapi(tags((name = "stream", description = "Plugin-owned SSE streaming")))]
struct ApiDoc;

/// Documentation stub for the SSE endpoint.
/// The text/event-stream content type signals to the host that this is an SSE route.
/// Connections arrive through /sse/p/stream/generate?count=N — this handler is never called via HTTP.
#[utoipa::path(
    get,
    path = "/generate",
    tag = "stream",
    params(
        ("count" = Option<u32>, Query, description = "Number of chunks to emit (max 100, default 5)")
    ),
    responses((
        status = 200,
        description = "Server-Sent Events chunk stream",
        content_type = "text/event-stream",
        body = String,
    ))
)]
async fn sse_generate() -> StatusCode {
    StatusCode::OK
}

/// Documentation stub for the streaming download endpoint.
/// Handled directly in `lib.rs::handle_http`, which writes the body onto the
/// response stream rather than building it in memory.
#[utoipa::path(
    get,
    path = "/download",
    tag = "stream",
    params(
        ("bytes" = Option<u64>, Query, description = "Body size to generate (default 1 MiB)")
    ),
    responses((status = 200, description = "Generated body", body = String))
)]
async fn download() -> StatusCode {
    StatusCode::OK
}


/// The route table, stated once.
///
/// A plain function, not a `LazyLock`: the host creates a fresh instance per
/// call, so a `LazyLock` initializer would run every time it was used anyway.
/// This plugin dispatches in `handle_http` rather than through the returned
/// `Router`, but `manifest()` is the only caller, so it costs nothing per request.
fn router() -> (Router, utoipa::openapi::OpenApi) {
    OpenApiRouter::<()>::with_openapi(ApiDoc::openapi())
        .routes(routes!(sse_generate))
        .routes(routes!(download))
        .split_for_parts()
}

pub fn openapi_json() -> String {
    serde_json::to_string(&router().1).unwrap_or_default()
}
