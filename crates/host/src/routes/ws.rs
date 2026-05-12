use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use tokio::sync::broadcast;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::api::AppState;

pub fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new().routes(routes!(push_ws))
}

/// WebSocket endpoint — streams `reward_granted` push events to connected clients.
///
/// Each message is a JSON object:
/// ```json
/// {"type":"reward_granted","user_id":42,"reward_cents":500,"emitted_at":1746403200}
/// ```
/// Connect with any WebSocket client: `wscat -c ws://localhost:3000/ws/push`
#[utoipa::path(
    get,
    path = "/ws/push",
    responses((status = 101, description = "Switched protocol"))
)]
async fn push_ws(ws: WebSocketUpgrade, State(app): State<AppState>) -> impl IntoResponse {
    let rx = app.runtime.subscribe_push_events();
    ws.on_upgrade(move |socket| handle_push_ws(socket, rx))
}

async fn handle_push_ws(mut socket: WebSocket, mut rx: broadcast::Receiver<String>) {
    loop {
        match rx.recv().await {
            Ok(msg) => {
                if socket.send(Message::Text(msg.into())).await.is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(dropped = n, "push ws client lagged");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}
