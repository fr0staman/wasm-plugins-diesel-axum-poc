use crate::bindings::myapp::plugin::host_api;
use crate::bindings::myapp::plugin::types::LogLevel;
use crate::error::AppError;
use crate::repository::PushRepository;
use crate::types::{NotificationsResponse, SendRequest, SendResponse, StatusResponse};

use axum::{Json, extract::Path, http::StatusCode};

#[utoipa::path(
    get,
    path = "/status",
    tag = "push",
    responses(
        (status = 200, description = "Plugin is alive and ready to serve requests", body = StatusResponse),
    )
)]
pub async fn get_status() -> Json<StatusResponse> {
    Json(StatusResponse {
        plugin: env!("CARGO_PKG_NAME").to_string(),
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

#[utoipa::path(
    post,
    path = "/send",
    tag = "push",
    request_body(
        content = SendRequest,
        description = "Target user and notification text. At most one FCM bonus notification \
                       is sent per user per calendar day; subsequent requests for the same \
                       user on the same day return 409."
    ),
    responses(
        (status = 202, description = "Notification accepted and queued for delivery", body = SendResponse),
        (status = 400, description = "Missing or malformed request body"),
        (status = 409, description = "A push notification was already sent to this user today"),
        (status = 500, description = "Unexpected database error"),
    )
)]
pub async fn post_send(
    Json(input): Json<SendRequest>,
) -> Result<(StatusCode, Json<SendResponse>), AppError> {
    let uid = input.user_id as i64;

    match PushRepository::insert_notification(uid).await? {
        0 => Err(AppError::Conflict(
            "a push notification was already sent to this user today".to_string(),
        )),
        _ => {
            host_api::log(LogLevel::Info, &format!("push queued for user={uid}"));
            Ok((
                StatusCode::ACCEPTED,
                Json(SendResponse {
                    queued: true,
                    channel: "fcm".to_string(),
                    // stub: in production replace with the message ID returned by the push gateway (e.g. FCM)
                    notification_id: "placeholder-id".to_string(),
                }),
            ))
        }
    }
}

#[derive(serde::Deserialize, utoipa::IntoParams)]
pub struct NotificationsParams {
    /// Numeric ID of the user whose notification history to retrieve
    pub user_id: i64,
}

#[utoipa::path(
    get, path = "/notifications/{user_id}",
    tag = "push",
    params(NotificationsParams),
    responses(
        (status = 200, description = "Push notifications sent to the user, ordered by date descending", body = NotificationsResponse),
        (status = 400, description = "user_id is not a valid integer"),
        (status = 500, description = "Unexpected database error"),
    )
)]
pub async fn get_notifications(
    Path(NotificationsParams { user_id }): Path<NotificationsParams>,
) -> Result<Json<NotificationsResponse>, AppError> {
    let notifications = PushRepository::find_by_user(user_id).await?;
    Ok(Json(NotificationsResponse {
        user_id: user_id as u64,
        notifications,
    }))
}
