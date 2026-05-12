use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, ToSchema)]
pub struct CalculateRequest {
    /// Numeric ID of the user receiving the bonus
    #[schema(example = 42)]
    pub user_id: u64,
    /// Payment amount in cents that the bonus is computed from
    #[schema(example = 10000)]
    pub amount_cents: i64,
    /// User subscription tier: `free`, `pro`, or `enterprise`
    #[schema(example = "pro")]
    pub user_tier: String,
}

#[derive(Serialize, ToSchema)]
pub struct CalculateResponse {
    /// Bonus amount awarded in cents
    #[schema(example = 500)]
    pub bonus_cents: i64,
    /// Multiplier applied to the payment amount
    #[schema(example = 0.05)]
    pub tier_multiplier: f64,
    /// Human-readable confirmation
    #[schema(example = "Bonus calculated")]
    pub message: String,
}

#[derive(Serialize, ToSchema)]
pub struct LedgerEntry {
    /// Ledger row ID
    #[schema(example = 1)]
    pub id: i64,
    /// Bonus amount in cents
    #[schema(example = 500)]
    pub bonus_cents: i64,
    /// ISO-8601 date the bonus was calculated
    #[schema(example = "2026-05-04")]
    pub calculated_date: String,
}

#[derive(Serialize, ToSchema)]
pub struct LedgerResponse {
    /// User ID The entries belong to
    #[schema(example = 42)]
    pub user_id: u64,
    /// Bonus ledger entries ordered by date descending
    pub entries: Vec<LedgerEntry>,
}

#[derive(Serialize, ToSchema)]
pub struct StatusResponse {
    /// Plugin package name
    #[schema(example = "bonus")]
    pub plugin: String,
    /// Liveness status
    #[schema(example = "ok")]
    pub status: String,
    /// Semver version string
    #[schema(example = "0.1.0")]
    pub version: String,
}
