use crate::bindings::myapp::plugin::types::{HttpHeader, HttpRequest, HttpResponse, PluginError};
use crate::bindings::wit_stream;
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

/// The route table, stated once and used by both `dispatch` and
/// `openapi_json`.
///
/// Deliberately a plain function rather than a `LazyLock`: the host creates a
/// fresh instance for every HTTP call, so a `LazyLock` initializer would run
/// once per request regardless and the once-init machinery bought nothing.
fn router() -> (Router, utoipa::openapi::OpenApi) {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(crate::handlers::get_status))
        .routes(routes!(crate::handlers::post_calculate))
        .routes(routes!(crate::handlers::get_ledger))
        .split_for_parts()
}

pub async fn dispatch(req: HttpRequest) -> Result<HttpResponse, PluginError> {
    let mut builder = Request::builder()
        .method(req.method.as_str())
        .uri(req.uri.as_str());

    for h in &req.headers {
        builder = builder.header(&h.name, &h.value);
    }

    // These handlers take small JSON bodies, so collecting is fine here. The
    // saving is on the host side: nothing is lowered into guest memory until
    // this read happens, and a route that ignores the body reads nothing.
    let hint = content_length(&req.headers);
    let body = read_body(req.body, hint).await;
    let request = builder.body(Body::from(body)).unwrap();

    let response = router().0.oneshot(request).await.unwrap();

    response_to_wit(response).await
}

/// Drains a `stream<u8>` request body into memory.
/// Drains a `stream<u8>` request body into memory.
///
/// `read` fills the spare capacity of the buffer it is given and hands it back,
/// so the buffer passed in *is* the output buffer — reading into a scratch buffer
/// and appending would copy every byte twice. `hint` (from content-length) sizes
/// the allocation up front, which both avoids reallocation and lets a whole body
/// arrive in a single crossing.
async fn read_body(mut body: wit_bindgen::StreamReader<u8>, hint: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(hint.max(READ_CHUNK));
    loop {
        // `read` is a no-op without spare capacity.
        if out.len() == out.capacity() {
            out.reserve(READ_CHUNK);
        }
        let (result, returned) = body.read(out).await;
        out = returned;
        if matches!(result, wit_bindgen::StreamResult::Dropped) {
            break;
        }
    }
    out
}

/// Spare capacity requested per read when content-length is absent or exhausted.
const READ_CHUNK: usize = 64 * 1024;

/// Parses content-length so the body buffer can be sized in one allocation.
fn content_length(headers: &[HttpHeader]) -> usize {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .and_then(|h| h.value.parse().ok())
        .unwrap_or(0)
}

async fn response_to_wit(resp: Response<Body>) -> Result<HttpResponse, PluginError> {
    let (parts, body) = resp.into_parts();
    let body = to_bytes(body, usize::MAX)
        .await
        .map_err(|e| PluginError::Internal(e.to_string()))?;

    // Hand back status+headers immediately and write the body on the stream;
    // the host starts responding without waiting for it to finish.
    let (mut body_tx, body_rx) = wit_stream::new::<u8>();
    wit_bindgen::spawn_local(async move {
        if !body.is_empty() {
            body_tx.write_all(body.to_vec()).await;
        }
        drop(body_tx);
    });

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
        body: body_rx,
    })
}

/// Load path only — called once per plugin load, so the full document with all
/// schemas is built here rather than on every request.
pub fn openapi_json() -> String {
    serde_json::to_string(&router().1).unwrap_or_default()
}
