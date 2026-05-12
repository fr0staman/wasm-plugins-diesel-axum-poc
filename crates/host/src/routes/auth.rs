use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::{api::AppState, auth};

#[derive(Deserialize, utoipa::ToSchema)]
pub struct IssueTokenBody {
    user_id: u64,
    tenant_id: u64,
    role: String,
}

#[derive(Serialize, utoipa::ToSchema)]
struct IssueTokenResponse {
    token: String,
}

#[utoipa::path(
    post,
    path = "/auth/token",
    request_body = IssueTokenBody,
    responses(
        (status = 200, description = "Issue token success", body = IssueTokenResponse),
        (status = 501),
    )
)]
pub async fn issue_token(
    State(app): State<AppState>,
    Json(body): Json<IssueTokenBody>,
) -> Response {
    let exp = crate::util::unix_now() + 3600;
    let claims = auth::Claims {
        sub: body.user_id.to_string(),
        tenant_id: body.tenant_id,
        role: body.role,
        exp,
    };
    match app.auth.sign(&claims) {
        Ok(token) => Json(serde_json::json!({"token": token})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
