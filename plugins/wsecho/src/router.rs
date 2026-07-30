use axum::{Router, http::StatusCode};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

#[derive(OpenApi)]
#[openapi(tags((name = "wsecho", description = "WebSocket echo")))]
struct ApiDoc;

/// Documentation stub for the WebSocket upgrade endpoint.
/// The 101 response signals to the host that this is a WS route, not an HTTP route.
/// This handler is never reached via HTTP — connections arrive through /ws/p/wsecho/chat.
#[utoipa::path(
    get,
    path = "/chat",
    tag = "wsecho",
    responses((status = 101, description = "WebSocket echo — upgrades connection"))
)]
async fn ws_chat() -> StatusCode {
    StatusCode::SWITCHING_PROTOCOLS
}


/// The route table, stated once.
///
/// A plain function, not a `LazyLock`: the host creates a fresh instance per
/// call, so a `LazyLock` initializer would run every time it was used anyway.
/// This plugin dispatches in `handle_http` rather than through the returned
/// `Router`, but `manifest()` is the only caller, so it costs nothing per request.
fn router() -> (Router, utoipa::openapi::OpenApi) {
    OpenApiRouter::<()>::with_openapi(ApiDoc::openapi())
        .routes(routes!(ws_chat))
        .split_for_parts()
}

pub fn openapi_json() -> String {
    serde_json::to_string(&router().1).unwrap_or_default()
}
