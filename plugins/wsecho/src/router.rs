use std::sync::LazyLock;

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

static ROUTER: LazyLock<(Router, utoipa::openapi::OpenApi)> = LazyLock::new(|| {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(ws_chat))
        .split_for_parts()
});

pub fn openapi_json() -> String {
    serde_json::to_string(&ROUTER.1).unwrap_or_default()
}
