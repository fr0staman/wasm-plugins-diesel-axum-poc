use axum::{Json, extract::State, http::StatusCode};
use serde::Deserialize;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::types::OkResponse;
use crate::{
    api::AppState,
    bindings::myapp::plugin::types::{
        CustomEvent, EventEnvelope, EventPayload, EventSource, PaymentMadeEvent,
        PaymentSnapshot, RewardGrantedEvent, SystemEvent, UserSnapshot, UserTier,
    },
};

pub fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        //.routes(routes!(plugin_handler))
        .routes(routes!(emit_event))
}

/// Emit a domain event into the plugin bus.
#[derive(Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PostEventBody {
    PaymentMade {
        user: UserDto,
        payment: PaymentDto,
    },
    RewardGranted {
        user: UserDto,
        reward_cents: i64,
        triggered_by_payment: u64,
    },
    /// Plugin-introduced custom event. `payload` is forwarded as raw bytes (JSON-serialized).
    Custom {
        name: String,
        payload: Option<serde_json::Value>,
    },
}

#[derive(Deserialize, ToSchema)]
struct UserDto {
    id: u64,
    tenant_id: u64,
    email: String,
    locale: String,
    /// One of `"free"`, `"pro"`, `"enterprise"`.
    tier: String,
}

#[derive(Deserialize, ToSchema)]
struct PaymentDto {
    id: u64,
    amount_cents: i64,
    currency: String,
    method: String,
    created_at: u64,
}

#[utoipa::path(
    post,
    path = "/events",
    request_body = PostEventBody,
    responses(
        (status = 200, description = "Event accepted", body = OkResponse),
        (status = 422, description = "Invalid payload"),
    )
)]
async fn emit_event(
    State(app): State<AppState>,
    Json(body): Json<PostEventBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let payload = match body {
        PostEventBody::PaymentMade { user, payment } => {
            EventPayload::System(SystemEvent::PaymentMade(PaymentMadeEvent {
                user: user_dto_to_snapshot(user),
                payment: PaymentSnapshot {
                    id: payment.id,
                    amount_cents: payment.amount_cents,
                    currency: payment.currency,
                    method: payment.method,
                    created_at: payment.created_at,
                },
            }))
        }
        PostEventBody::RewardGranted {
            user,
            reward_cents,
            triggered_by_payment,
        } => EventPayload::System(SystemEvent::RewardGranted(RewardGrantedEvent {
            user: user_dto_to_snapshot(user),
            reward_cents,
            triggered_by_payment,
        })),
        PostEventBody::Custom { name, payload } => {
            let bytes = payload
                .map(|v| serde_json::to_vec(&v).unwrap_or_default())
                .unwrap_or_default();
            EventPayload::Custom(CustomEvent {
                name,
                payload: bytes,
            })
        }
    };

    let envelope = EventEnvelope {
        id: crate::util::rand_id(),
        emitted_at: crate::util::unix_now(),
        chain_depth: 0,
        source: EventSource::Host,
        payload,
    };

    app.runtime.dispatch(envelope);
    Ok(Json(serde_json::json!({ "ok": true })))
}

fn user_dto_to_snapshot(u: UserDto) -> UserSnapshot {
    UserSnapshot {
        id: u.id,
        tenant_id: u.tenant_id,
        email: u.email,
        locale: u.locale,
        tier: match u.tier.as_str() {
            "pro" => UserTier::Pro,
            "enterprise" => UserTier::Enterprise,
            _ => UserTier::Free,
        },
    }
}
