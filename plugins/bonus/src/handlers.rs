use crate::error::AppError;
use crate::repository::BonusRepository;
use crate::types::{
    CalculateRequest, CalculateResponse, LedgerEntry, LedgerResponse, StatusResponse,
};

use axum::{Json, extract::Path};

#[utoipa::path(
    get,
    path = "/status",
    tag = "bonus",
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
    path = "/calculate",
    tag = "bonus",
    security(("bearerAuth" = [])),
    request_body(
        content = CalculateRequest,
        description = "Payment event details used to compute the tier-adjusted daily bonus. \
                       At most one bonus is written per user per calendar day; subsequent \
                       requests for the same user on the same day return 409."
    ),
    responses(
        (status = 200, description = "Bonus written to the ledger", body = CalculateResponse),
        (status = 400, description = "Missing or malformed request body"),
        (status = 409, description = "A bonus was already calculated for this user today"),
        (status = 500, description = "Unexpected database error"),
    )
)]
pub async fn post_calculate(
    Json(input): Json<CalculateRequest>,
) -> Result<Json<CalculateResponse>, AppError> {
    let multiplier = tier_multiplier(&input.user_tier);
    let bonus_cents = (input.amount_cents as f64 * multiplier) as i64;

    match BonusRepository::insert_daily_bonus(input.user_id as i64, bonus_cents).await? {
        0 => Err(AppError::Conflict(
            "a bonus was already calculated for this user today".to_string(),
        )),
        _ => Ok(Json(CalculateResponse {
            bonus_cents,
            tier_multiplier: multiplier,
            message: "Bonus calculated".to_string(),
        })),
    }
}

#[derive(serde::Deserialize, utoipa::IntoParams)]
pub struct LedgerParams {
    /// Numeric ID of the user whose bonus history to retrieve
    user_id: i64,
}

#[utoipa::path(
    get, path = "/ledger/{user_id}",
    tag = "bonus",
    params(LedgerParams),
    responses(
        (status = 200, description = "Bonus ledger entries for the user, ordered by date descending", body = LedgerResponse),
        (status = 400, description = "user_id is not a valid integer"),
        (status = 500, description = "Unexpected database error"),
    )
)]
pub async fn get_ledger(
    Path(LedgerParams { user_id }): Path<LedgerParams>,
) -> Result<Json<LedgerResponse>, AppError> {
    let entries = BonusRepository::find_by_user(user_id).await?;
    let entries = entries.into_iter().map(LedgerEntry::from).collect();
    Ok(Json(LedgerResponse {
        user_id: user_id as u64,
        entries,
    }))
}

fn tier_multiplier(tier: &str) -> f64 {
    match tier {
        "pro" => 0.05,
        "enterprise" => 0.10,
        _ => 0.01,
    }
}
