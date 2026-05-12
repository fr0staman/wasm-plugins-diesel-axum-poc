use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, ToSchema)]
pub struct SendRequest {
    /// Numeric ID of the user to notify
    #[schema(example = 42)]
    pub user_id: u64,
    /// Notification body text shown to the user
    #[schema(example = "You earned a bonus!")]
    pub message: String,
}

#[derive(Serialize, ToSchema)]
pub struct SendResponse {
    /// Whether the notification was accepted for delivery
    #[schema(example = true)]
    pub queued: bool,
    /// Delivery channel used
    #[schema(example = "fcm")]
    pub channel: String,
    /// Opaque notification identifier for idempotency tracking
    #[schema(example = "placeholder-id")]
    pub notification_id: String,
}

#[derive(Serialize, ToSchema)]
pub struct NotificationEntry {
    /// Notification row ID
    #[schema(example = 1)]
    pub id: i64,
    /// Delivery channel
    #[schema(example = "fcm")]
    pub channel: String,
    /// Notification category
    #[schema(example = "bonus")]
    pub notification_type: String,
    /// ISO-8601 date the notification was sent
    #[schema(example = "2026-05-04")]
    pub sent_date: String,
}

#[derive(Serialize, ToSchema)]
pub struct NotificationsResponse {
    /// User ID the notifications belong to
    #[schema(example = 42)]
    pub user_id: u64,
    /// Notifications sent to the user, ordered by date descending
    pub notifications: Vec<NotificationEntry>,
}

#[derive(Serialize, ToSchema)]
pub struct StatusResponse {
    /// Plugin package name
    #[schema(example = "push")]
    pub plugin: String,
    /// Liveness status
    #[schema(example = "ok")]
    pub status: String,
    /// Semver version string
    #[schema(example = "0.1.0")]
    pub version: String,
}
