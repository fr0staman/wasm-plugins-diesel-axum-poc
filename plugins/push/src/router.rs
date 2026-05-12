use std::sync::LazyLock;

use crate::bindings::myapp::plugin::types::{HttpHeader, HttpRequest, HttpResponse, PluginError};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::Request,
    response::Response,
};
use tower::util::ServiceExt;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

#[derive(OpenApi)]
#[openapi(
    tags((name = "push", description = "FCM push notification delivery and history"))
)]
struct ApiDoc;

static ROUTER: LazyLock<(Router, utoipa::openapi::OpenApi)> = LazyLock::new(|| {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(crate::handlers::get_status))
        .routes(routes!(crate::handlers::post_send))
        .routes(routes!(crate::handlers::get_notifications))
        .split_for_parts()
});

pub async fn dispatch(req: HttpRequest) -> Result<HttpResponse, PluginError> {
    let mut builder = Request::builder()
        .method(req.method.as_str())
        .uri(req.uri.as_str());

    for h in &req.headers {
        builder = builder.header(&h.name, &h.value);
    }

    let request = builder
        .body(Body::from(req.body.unwrap_or_default()))
        .unwrap();

    let response = ROUTER.0.clone().oneshot(request).await.unwrap();

    response_to_wit(response).await
}

async fn response_to_wit(resp: Response<Body>) -> Result<HttpResponse, PluginError> {
    let (parts, body) = resp.into_parts();
    let body = to_bytes(body, usize::MAX)
        .await
        .map_err(|e| PluginError::Internal(e.to_string()))?;

    Ok(HttpResponse {
        status: parts.status.as_u16(),
        headers: parts
            .headers
            .iter()
            .map(|(k, v)| HttpHeader {
                name: k.to_string(),
                value: v.to_str().unwrap_or("").to_string(),
            })
            .collect(),
        body: Some(body.to_vec()),
    })
}

pub fn openapi_json() -> String {
    serde_json::to_string(&ROUTER.1).unwrap_or_default()
}
