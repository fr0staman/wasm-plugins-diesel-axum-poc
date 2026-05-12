use std::sync::LazyLock;

use crate::bindings::myapp::plugin::types::{HttpHeader, HttpRequest, HttpResponse, PluginError};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::Request,
    response::Response,
};
use tower::util::ServiceExt;
use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};
use utoipa_axum::{router::OpenApiRouter, routes};

struct BearerAuth;

impl Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    tags((name = "bonus", description = "Tier-adjusted daily bonus ledger")),
    modifiers(&BearerAuth)
)]
struct ApiDoc;

static ROUTER: LazyLock<(Router, utoipa::openapi::OpenApi)> = LazyLock::new(|| {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(crate::handlers::get_status))
        .routes(routes!(crate::handlers::post_calculate))
        .routes(routes!(crate::handlers::get_ledger))
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
