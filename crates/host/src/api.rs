use axum::{Router, middleware, routing::get};
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::auth::{self, AuthConfig};
use crate::runtime::SharedRuntime;

#[derive(Clone)]
pub struct AppState {
    pub runtime: SharedRuntime,
    pub auth: Arc<AuthConfig>,
}

#[derive(OpenApi)]
#[openapi(info(
    title = "wasm-plugins-diesel-axum-poc",
    version = env!("CARGO_PKG_VERSION"),
    description = "Host runtime API. Plugins extend the `/p/` prefix by implementing `handle-http` in their WASM component."
))]
pub struct ApiDoc;

pub fn router(runtime: SharedRuntime, auth: Arc<AuthConfig>) -> Router {
    let state = AppState { runtime, auth };

    let public = OpenApiRouter::new()
        .routes(routes!(crate::routes::other::health))
        .routes(routes!(crate::routes::auth::issue_token));

    let protected = OpenApiRouter::new()
        .merge(crate::routes::ws::router())
        .merge(crate::routes::events::router())
        .merge(crate::routes::users::router())
        .merge(crate::routes::payments::router())
        .merge(crate::routes::plugins::router())
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::layer_require_auth,
        ));

    let plugins = OpenApiRouter::new().routes(routes!(crate::routes::plugins::plugin_handler));

    let (root_router, openapi) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(public)
        .merge(protected)
        .merge(plugins)
        .split_for_parts();

    let root_router = root_router
        // SPIKE: same plugins, reached through their standard wasi:http/handler
        // export instead of plugin-api's handle-http. Left outside the OpenAPI
        // router deliberately — it is an A/B path, not a documented endpoint.
        .route(
            "/h/{plugin_name}/{*path}",
            axum::routing::any(crate::routes::plugins::wasi_http_handler),
        )
        .route(
            "/api-docs/openapi.json",
            get(async |state| crate::routes::other::openapi_json(state, openapi).await),
        )
        .with_state(state);

    root_router
}
