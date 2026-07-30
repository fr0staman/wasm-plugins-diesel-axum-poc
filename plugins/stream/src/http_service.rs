//! SPIKE: the same two endpoints as `plugin-api`'s `handle-http` / `handle-sse`,
//! but served through the standard `wasi:http/handler` export.
//!
//! Two things to notice versus the hand-rolled API:
//!
//! * There is no `handle-sse`. SSE is not a separate protocol — it is a response
//!   with `content-type: text/event-stream` and a body that keeps being written.
//!   Both endpoints below are ordinary HTTP responses.
//! * Requests and responses are *resources* with getters/setters, so building one
//!   is more ceremony than filling in a record.

use crate::bindings::exports::wasi::http::handler::Guest as HttpHandler;
use crate::bindings::wasi::clocks::monotonic_clock;
use crate::bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use crate::bindings::wit_future;
use crate::bindings::wit_stream;

/// Bytes per crossing on a response body stream.
const BODY_CHUNK: usize = 64 * 1024;

/// Items per crossing when emitting SSE frames.
const SSE_BATCH: usize = 32;

fn query_param(path: &str, key: &str) -> Option<u64> {
    path.split_once('?').and_then(|(_, qs)| {
        qs.split('&')
            .find_map(|kv| kv.strip_prefix(key))
            .and_then(|v| v.parse().ok())
    })
}

impl HttpHandler for crate::Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = Request::get_path_with_query(&request).unwrap_or_default();

        // The request body is never needed here. Consuming and dropping it is
        // enough — nothing is lowered into guest memory.
        let (body, _trailers) = {
            let (fut_tx, fut_rx) = wit_future::new::<Result<(), ErrorCode>>(|| Ok(()));
            wit_bindgen::spawn_local(async move {
                fut_tx.write(Ok(())).await;
            });
            Request::consume_body(request, fut_rx)
        };
        drop(body);

        if path.starts_with("/download") {
            return Ok(download(query_param(&path, "bytes=").unwrap_or(1024 * 1024)));
        }
        if path.starts_with("/generate") {
            return Ok(sse(
                query_param(&path, "count=").unwrap_or(5).min(100) as u32,
                query_param(&path, "interval_ms=").unwrap_or(0).min(10_000),
            ));
        }
        Err(ErrorCode::InternalError(Some("no such route".into())))
    }
}

/// Generates `total` bytes onto the response body stream.
fn download(total: u64) -> Response {
    let (mut body_tx, body_rx) = wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));

    wit_bindgen::spawn_local(async move {
        let mut sent = 0u64;
        while sent < total {
            let n = BODY_CHUNK.min((total - sent) as usize);
            body_tx.write_all(vec![b'x'; n]).await;
            sent += n as u64;
        }
        drop(body_tx);
        trailers_tx.write(Ok(None)).await;
    });

    let (resp, _sent) = Response::new(Fields::new(), Some(body_rx), trailers_rx);
    resp
}

/// The former `handle-sse`, as a plain response: `text/event-stream` plus a body
/// that keeps being written. No SSE-specific plumbing on either side.
fn sse(count: u32, interval_ms: u64) -> Response {
    let (mut body_tx, body_rx) = wit_stream::new::<u8>();
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));

    wit_bindgen::spawn_local(async move {
        let mut batch: Vec<u8> = Vec::with_capacity(SSE_BATCH * 48);
        for i in 0..count {
            let frame = format!("data: {{\"index\":{i},\"data\":\"item_{i}\"}}\n\n");
            if interval_ms > 0 {
                body_tx.write_all(frame.into_bytes()).await;
                monotonic_clock::wait_for(interval_ms * 1_000_000).await;
                continue;
            }
            batch.extend_from_slice(frame.as_bytes());
            if batch.len() >= SSE_BATCH * 48 {
                body_tx.write_all(core::mem::take(&mut batch)).await;
            }
        }
        if !batch.is_empty() {
            body_tx.write_all(batch).await;
        }
        drop(body_tx);
        trailers_tx.write(Ok(None)).await;
    });

    let headers = Fields::new();
    let _ = headers.append("content-type", b"text/event-stream");
    let (resp, _sent) = Response::new(headers, Some(body_rx), trailers_rx);
    resp
}
