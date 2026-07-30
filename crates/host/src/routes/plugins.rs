use axum::{
    Json,
    body::Bytes,
    extract::{Path, State, ws::WebSocketUpgrade},
    http::{HeaderMap, Method, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::runtime::event_subscription_name;
use crate::{
    auth,
    bindings::myapp::plugin::types::{HttpHeader, WsMessage},
};

use std::convert::Infallible;
use tokio::sync::mpsc;

use crate::api::AppState;

/// Depth of the host-side buffer between a plugin's stream and the socket.
/// Small on purpose: it exists to smooth scheduling jitter, not to absorb a
/// client that cannot keep up — that case must reach the plugin as backpressure.
const STREAM_BUFFER: usize = 32;

pub fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        //.routes(routes!(plugin_handler))
        .routes(routes!(ws_plugin_handler))
        .routes(routes!(sse_plugin_handler))
        .routes(routes!(list_plugins))
}

#[derive(Serialize, ToSchema)]
struct PluginInfo {
    /// Unique plugin name as declared by the WASM component.
    name: String,
    /// Semantic version declared by the plugin (`CARGO_PKG_VERSION`).
    version: String,
    /// Event kinds the plugin subscribes to (e.g. `"payment_made"`).
    subscribed_events: Vec<String>,
    /// URL path patterns the plugin handles under `/p/{name}/`.
    http_routes: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/plugins",
    responses((status = 200, description = "Loaded plugins", body = Vec<PluginInfo>))
)]
async fn list_plugins(State(app): State<AppState>) -> Json<Vec<PluginInfo>> {
    let plugins = app
        .runtime
        .plugins
        .iter()
        .map(|p| PluginInfo {
            name: p.name.clone(),
            version: p.version.clone(),
            subscribed_events: p
                .subscribed_events
                .iter()
                .map(event_subscription_name)
                .collect(),
            http_routes: p.http_routes.clone(),
        })
        .collect();
    Json(plugins)
}

/// Structured error returned when a plugin request fails.
#[derive(Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(dead_code)]
enum PluginErrorDto {
    /// The plugin's database operation failed.
    DbError { message: String },
    /// The request did not pass plugin-level validation.
    InvalidInput { message: String },
    /// The requested resource was not found inside the plugin.
    NotFound,
    /// An unexpected internal error occurred inside the plugin.
    Internal { message: String },
}

#[utoipa::path(
    method(get, post, put, delete, options, head, patch, trace),
    path = "/p/{plugin_name}/{*path}",
    params(
        ("plugin_name" = String, Path, description = "Name of the target plugin"),
        ("path" = String, Path, description = "Path forwarded to the plugin"),
    ),
    request_body = Vec<u8>,
    responses(
        (status = 200, description = "Plugin response (status determined by the plugin)"),
        (status = 401, description = "Missing or invalid JWT for a protected plugin route"),
        (status = 404, description = "No matching route registered by this plugin"),
        (status = 502, description = "Plugin returned an error", body = PluginErrorDto),
    )
)]
pub async fn plugin_handler(
    State(app): State<AppState>,
    Path((plugin_name, path)): Path<(String, String)>,
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Route matching uses the bare path; the plugin gets the query string too,
    // otherwise a handler using axum's `Query` extractor would never see it.
    let plugin_path = format!("/{path}");
    let plugin_uri = match uri.query() {
        Some(q) => format!("{plugin_path}?{q}"),
        None => plugin_path.clone(),
    };
    let method_upper = method.as_str().to_ascii_uppercase();

    // Single Vec scan: resolve route + auth flag before allocating any headers.
    // Returns 404 immediately if the route is not registered.
    let Some((executor, plugin_pre, is_protected)) =
        app.runtime
            .prepare_http_with_auth(&plugin_name, &method_upper, &plugin_path)
    else {
        return (
            StatusCode::NOT_FOUND,
            format!("no route {method} {plugin_path} in plugin {plugin_name}"),
        )
            .into_response();
    };

    // Always strip client-supplied X-Auth-* headers to prevent spoofing.
    let mut forwarded: Vec<HttpHeader> = headers
        .iter()
        .filter(|(k, _)| !k.as_str().to_ascii_lowercase().starts_with("x-auth-"))
        .filter_map(|(k, v)| {
            v.to_str().ok().map(|val| HttpHeader {
                name: k.to_string(),
                value: val.to_string(),
            })
        })
        .collect();

    if is_protected {
        let auth_header = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        let Some(token) = auth_header.and_then(auth::bearer_token) else {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "missing token"})),
            )
                .into_response();
        };

        let Ok(claims) = app.auth.verify(token) else {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "invalid token"})),
            )
                .into_response();
        };
        forwarded.push(HttpHeader {
            name: "x-auth-user-id".into(),
            value: claims.sub.clone(),
        });
        forwarded.push(HttpHeader {
            name: "x-auth-tenant-id".into(),
            value: claims.tenant_id.to_string(),
        });
        forwarded.push(HttpHeader {
            name: "x-auth-role".into(),
            value: claims.role.clone(),
        });
    }

    // Request body in, response body out — both streamed. The wasm task runs on
    // its own tokio task so axum can begin sending the response while the plugin
    // is still writing it.
    let (head_tx, head_rx) = tokio::sync::oneshot::channel::<(u16, Vec<HttpHeader>)>();
    let (resp_tx, mut resp_rx) = mpsc::channel::<Vec<u8>>(STREAM_BUFFER);

    let method_s = method.to_string();
    let plugin_for_task = plugin_name.clone();
    let call = tokio::spawn(async move {
        executor
            .call_http(
                &plugin_for_task,
                &plugin_pre,
                method_s,
                plugin_uri,
                forwarded,
                body.to_vec(),
                head_tx,
                resp_tx,
            )
            .await
    });

    let Ok((status, headers)) = head_rx.await else {
        // No head means the plugin failed before returning a response.
        let msg = match call.await {
            Ok(Err(e)) => e.to_string(),
            Ok(Ok(())) => "plugin returned no response".to_string(),
            Err(e) => e.to_string(),
        };
        return (StatusCode::BAD_GATEWAY, msg).into_response();
    };

    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder().status(status);
    for h in &headers {
        builder = builder.header(&h.name, &h.value);
    }

    let body_stream = async_stream::stream! {
        while let Some(chunk) = resp_rx.recv().await {
            yield Ok::<_, Infallible>(axum::body::Bytes::from(chunk));
        }
        if let Ok(Err(e)) = call.await {
            tracing::error!(plugin = %plugin_name, error = %e, "http plugin error");
        }
    };

    builder
        .body(axum::body::Body::from_stream(body_stream))
        .unwrap()
}

/// WebSocket passthrough — upgrades the connection and delegates it to the named plugin.
///
/// The plugin's `handle-websocket` runs for the lifetime of the connection.
/// Each message from the client becomes available via `ws-recv`; the plugin calls
/// `ws-send` to reply.  Connect: `wscat -c ws://localhost:3000/ws/p/{plugin}/{path}`
///

#[derive(Debug, Deserialize, IntoParams)]
pub struct WsPluginParams {
    /// Registered plugin name.
    ///
    /// Used to resolve the plugin websocket executor.
    #[param(example = "chat-plugin")]
    pub plugin_name: String,

    /// Wildcard websocket route path inside the plugin.
    ///
    /// This captures the remaining path segments after `{plugin_name}`.
    ///
    /// Examples:
    /// - `rooms/general`
    /// - `stream/live`
    /// - `agent/events`
    #[param(example = "rooms/general")]
    pub path: String,
}

/// Error response returned when websocket routing fails.
#[derive(Debug, Serialize, ToSchema)]
pub struct WsErrorResponse {
    /// Human readable error message.
    #[schema(example = "no ws route /rooms/general in plugin chat-plugin")]
    pub error: String,
}

#[utoipa::path(
    get,
    path = "/ws/p/{plugin_name}/{*path}",
    params(WsPluginParams),
    responses(
        (
            status = 101,
            description = "WebSocket protocol upgrade successful",

            headers(
                ("Upgrade" = String),
                ("Connection" = String),
                ("Sec-WebSocket-Accept" = String)
            )
        ),

        (
            status = 404,
            description = "Plugin or websocket route not found",
            body = WsErrorResponse,
            example = json!(
                {
                    "error": "no ws route /rooms/general in plugin chat-plugin"
                }
            )
        ),

        (
            status = 400,
            description = "Invalid websocket upgrade request",
            body = WsErrorResponse
        ),

        (
            status = 500,
            description = "Internal websocket dispatch error",
            body = WsErrorResponse
        )
    ),
)]
async fn ws_plugin_handler(
    ws: WebSocketUpgrade,
    Path(WsPluginParams { plugin_name, path }): Path<WsPluginParams>,
    State(app): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let ws_path = format!("/{path}");
    let Some((executor, pre, is_protected)) = app.runtime.prepare_websocket(&plugin_name, &ws_path)
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(WsErrorResponse {
                error: format!("no ws route {ws_path} in plugin {plugin_name}"),
            }),
        )
            .into_response();
    };

    if is_protected {
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(auth::bearer_token);
        let Some(token) = token else {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "missing token"})),
            )
                .into_response();
        };
        if app.auth.verify(token).is_err() {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "invalid token"})),
            )
                .into_response();
        }
    }

    let conn_id = crate::host_api::new_conn_id();
    ws.on_upgrade(move |socket| {
        handle_ws_plugin(socket, plugin_name, ws_path, conn_id, pre, executor)
    })
}

/// Parameters for SSE plugin route.
#[derive(Debug, Deserialize, IntoParams)]
pub struct SsePluginParams {
    /// Registered plugin name.
    #[param(example = "stream")]
    pub plugin_name: String,
    /// Wildcard SSE route path inside the plugin.
    #[param(example = "generate")]
    pub path: String,
}

/// SSE passthrough — opens an event stream and delegates chunk production to the named plugin.
///
/// The plugin's `handle-sse` runs for the lifetime of the connection, calling `sse-yield`
/// for each chunk. Returning `Ok(())` closes the stream.
/// Connect: `curl -N http://localhost:3000/sse/p/{plugin}/{path}`
#[utoipa::path(
    get,
    path = "/sse/p/{plugin_name}/{*path}",
    params(SsePluginParams),
    responses(
        (status = 200, description = "SSE stream", content_type = "text/event-stream", body = String),
        (status = 404, description = "Plugin or SSE route not found", body = WsErrorResponse),
    ),
)]
async fn sse_plugin_handler(
    Path(SsePluginParams { plugin_name, path }): Path<SsePluginParams>,
    uri: axum::http::Uri,
    State(app): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    use std::convert::Infallible;
    use tokio::sync::mpsc;

    let sse_path = format!("/{path}");
    let Some((executor, pre, is_protected)) = app.runtime.prepare_sse(&plugin_name, &sse_path)
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(WsErrorResponse {
                error: format!("no sse route {sse_path} in plugin {plugin_name}"),
            }),
        )
            .into_response();
    };

    if is_protected {
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(auth::bearer_token);
        let Some(token) = token else {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "missing token"})),
            )
                .into_response();
        };
        if app.auth.verify(token).is_err() {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "invalid token"})),
            )
                .into_response();
        }
    }

    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    let full_path = format!("{sse_path}{query}");

    let conn_id = crate::host_api::new_conn_id();
    // Bounded: the plugin's stream writes block once the client falls this far
    // behind, rather than the host buffering without limit. Items arrive in
    // batches, so capacity is STREAM_BUFFER batches, not chunks.
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<Vec<u8>>>(STREAM_BUFFER);

    tokio::spawn(async move {
        if let Err(e) = executor
            .call_sse(&plugin_name, &pre, full_path, conn_id, outbound_tx)
            .await
        {
            tracing::error!(plugin = %plugin_name, error = %e, "sse plugin error");
        }
    });

    let stream = async_stream::stream! {
        while let Some(batch) = outbound_rx.recv().await {
            for chunk in batch {
                yield Ok::<Event, Infallible>(
                    Event::default().data(String::from_utf8_lossy(&chunk).into_owned())
                );
            }
        }
    };

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn handle_ws_plugin(
    socket: axum::extract::ws::WebSocket,
    plugin_name: String,
    path: String,
    conn_id: u64,
    pre: crate::bindings::PluginPre<crate::host_api::PluginState>,
    executor: std::sync::Arc<crate::runtime::PluginExecutor>,
) {
    use axum::extract::ws::Message;
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::mpsc;

    let (mut sink, mut stream) = socket.split();
    let (inbound_tx, inbound_rx) = mpsc::channel::<WsMessage>(STREAM_BUFFER);
    // Bounded for the same reason as SSE: a slow client must not let the plugin
    // queue frames without limit.
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<WsMessage>>(STREAM_BUFFER);

    let inbound_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = stream.next().await {
            let ws_msg = match msg {
                Message::Text(t) => WsMessage::Text(t.to_string()),
                Message::Binary(b) => WsMessage::Binary(b.to_vec()),
                Message::Close(frame) => WsMessage::Close(frame.map(|f| f.reason.to_string())),
                Message::Ping(_) | Message::Pong(_) => continue,
            };
            if inbound_tx.send(ws_msg).await.is_err() {
                break;
            }
        }
    });

    let outbound_task = tokio::spawn(async move {
        'outer: while let Some(batch) = outbound_rx.recv().await {
            for msg in batch {
                let axum_msg = match msg {
                    WsMessage::Text(t) => Message::Text(t.into()),
                    WsMessage::Binary(b) => Message::Binary(b.into()),
                    WsMessage::Close(reason) => Message::Close(reason.map(|r| {
                        axum::extract::ws::CloseFrame {
                            code: 1000,
                            reason: r.into(),
                        }
                    })),
                };
                if sink.send(axum_msg).await.is_err() {
                    break 'outer;
                }
            }
        }
    });

    if let Err(e) = executor
        .call_websocket(&plugin_name, &pre, path, conn_id, inbound_rx, outbound_tx)
        .await
    {
        tracing::error!(plugin = %plugin_name, error = %e, "ws plugin error");
    }

    inbound_task.abort();
    outbound_task.abort();
}

/// SPIKE: passthrough to a plugin's `wasi:http/handler` export.
///
/// The entire body of this function is route lookup plus one call — all the
/// header/body/stream conversion that `plugin_handler` does by hand lives in
/// wasmtime-wasi-http instead.
pub async fn wasi_http_handler(
    State(app): State<AppState>,
    Path((plugin_name, path)): Path<(String, String)>,
    req: axum::extract::Request,
) -> Response {
    let Some((executor, plugin_pre)) = app.runtime.prepare_wasi_http(&plugin_name) else {
        return (StatusCode::NOT_FOUND, "no such plugin").into_response();
    };

    // Rewrite the URI to be plugin-relative: the plugin routes on what it sees,
    // and it should not see the host's /h/{plugin} prefix.
    let mut req = req;
    let plugin_uri = match req.uri().query() {
        Some(q) => format!("/{path}?{q}"),
        None => format!("/{path}"),
    };
    *req.uri_mut() = plugin_uri.parse().unwrap_or_else(|_| "/".parse().unwrap());

    match crate::runtime::call_wasi_http(&executor, &plugin_name, &plugin_pre, req).await {
        Ok(resp) => resp.into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}
