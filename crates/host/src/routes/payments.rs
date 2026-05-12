use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::api::AppState;
use crate::models::NewPayment;
use crate::repository;
use crate::types::{ApiResult, OkResponse};

pub fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(api_list_payments, api_create_payment))
        .routes(routes!(api_get_payment, api_delete_payment))
}

fn db_err(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

fn not_found() -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, "not found".into())
}

#[derive(Serialize, ToSchema)]
pub struct PaymentResponse {
    id: i64,
    user_id: i64,
    amount_cents: i64,
    currency: String,
    method: String,
    created_at: u64,
}

impl From<crate::models::Payment> for PaymentResponse {
    fn from(p: crate::models::Payment) -> Self {
        Self {
            id: p.id,
            user_id: p.user_id,
            amount_cents: p.amount_cents,
            currency: p.currency,
            method: p.method,
            created_at: p.created_at.timestamp() as u64,
        }
    }
}

#[derive(Deserialize, ToSchema, utoipa::IntoParams)]
pub struct ListPaymentsParams {
    user_id: Option<i64>,
}

#[derive(Deserialize, ToSchema)]
pub struct CreatePaymentBody {
    user_id: i64,
    amount_cents: i64,
    #[serde(default = "default_currency")]
    currency: String,
    method: String,
}

fn default_currency() -> String {
    "USD".into()
}

#[derive(Deserialize, IntoParams)]
struct PaymentPath {
    id: i64,
}

#[utoipa::path(
    get,
    path = "/payments",
    params(ListPaymentsParams),
    responses(
        (status = 200, body = Vec<PaymentResponse>))
    )
]
async fn api_list_payments(
    State(app): State<AppState>,
    Query(params): Query<ListPaymentsParams>,
) -> ApiResult<Vec<PaymentResponse>> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;
    let user_id = params.user_id.unwrap_or(0);
    let rows = repository::list_user_payments(&db, user_id)
        .await
        .map_err(db_err)?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

#[utoipa::path(
    post,
    path = "/payments",
    request_body = CreatePaymentBody,
    responses(
        (status = 201, body = PaymentResponse))
    )
]
async fn api_create_payment(
    State(app): State<AppState>,
    Json(body): Json<CreatePaymentBody>,
) -> Result<(StatusCode, Json<PaymentResponse>), (StatusCode, String)> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;

    let new = NewPayment {
        user_id: body.user_id,
        amount_cents: body.amount_cents,
        currency: &body.currency,
        method: &body.method,
    };
    let p = repository::create_payment(&db, new).await.map_err(db_err)?;
    Ok((StatusCode::CREATED, Json(p.into())))
}

#[utoipa::path(
    get,
    path = "/payments/{id}",
    params(PaymentPath),
    responses(
        (status = 200, body = PaymentResponse),
        (status = 404, description = "Not found"))
    )
]
async fn api_get_payment(
    State(app): State<AppState>,
    Path(PaymentPath { id }): Path<PaymentPath>,
) -> ApiResult<PaymentResponse> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;
    repository::find_payment(&db, id)
        .await
        .map_err(db_err)?
        .map(|p| Json(p.into()))
        .ok_or_else(not_found)
}

#[utoipa::path(
    delete,
    path = "/payments/{id}",
    params(PaymentPath),
    responses(
        (status = 200, body = OkResponse),
        (status = 404, description = "Not found"))
    )
]
async fn api_delete_payment(
    State(app): State<AppState>,
    Path(PaymentPath { id }): Path<PaymentPath>,
) -> ApiResult<serde_json::Value> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;
    let deleted = repository::delete_payment(&db, id).await.map_err(db_err)?;
    if deleted {
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err(not_found())
    }
}
