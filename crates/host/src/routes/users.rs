use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::api::AppState;
use crate::models::{NewUser, PatchUser};
use crate::repository;
use crate::types::{ApiResult, OkResponse};

pub fn router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(api_list_users, api_create_user))
        .routes(routes!(api_get_user, api_patch_user, api_delete_user))
}

#[derive(Serialize, ToSchema)]
pub struct UserResponse {
    id: i64,
    tenant_id: i64,
    email: String,
    locale: String,
    tier: String,
    created_at: u64,
    updated_at: u64,
}

impl From<crate::models::User> for UserResponse {
    fn from(u: crate::models::User) -> Self {
        Self {
            id: u.id,
            tenant_id: u.tenant_id,
            email: u.email,
            locale: u.locale,
            tier: u.tier,
            created_at: u.created_at.timestamp() as u64,
            updated_at: u.updated_at.timestamp() as u64,
        }
    }
}

#[derive(Deserialize, ToSchema, utoipa::IntoParams)]
pub struct ListUsersParams {
    tenant_id: Option<i64>,
}

#[derive(Deserialize, ToSchema)]
pub struct CreateUserBody {
    tenant_id: i64,
    email: String,
    #[serde(default = "default_locale")]
    locale: String,
    #[serde(default = "default_tier")]
    tier: String,
}

#[derive(Deserialize, ToSchema)]
pub struct PatchUserBody {
    locale: Option<String>,
    tier: Option<String>,
}

fn default_locale() -> String {
    "en".into()
}
fn default_tier() -> String {
    "free".into()
}

fn db_err(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

fn not_found() -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, "not found".into())
}

#[utoipa::path(get, path = "/users",
    params(ListUsersParams),
    responses((status = 200, body = Vec<UserResponse>)))]
async fn api_list_users(
    State(app): State<AppState>,
    Query(params): Query<ListUsersParams>,
) -> ApiResult<Vec<UserResponse>> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;
    let tenant_id = params.tenant_id.unwrap_or(1);
    let users = repository::list_users(&db, tenant_id)
        .await
        .map_err(db_err)?;
    Ok(Json(users.into_iter().map(Into::into).collect()))
}

#[utoipa::path(post, path = "/users",
    request_body = CreateUserBody,
    responses((status = 201, body = UserResponse)))]
async fn api_create_user(
    State(app): State<AppState>,
    Json(body): Json<CreateUserBody>,
) -> Result<(StatusCode, Json<UserResponse>), (StatusCode, String)> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;
    let new = NewUser {
        tenant_id: body.tenant_id,
        email: &body.email,
        locale: &body.locale,
        tier: &body.tier,
    };
    let user = repository::create_user(&db, new).await.map_err(db_err)?;
    Ok((StatusCode::CREATED, Json(user.into())))
}

#[utoipa::path(get, path = "/users/{id}",
    params(("id" = i64, Path, description = "User ID")),
    responses((status = 200, body = UserResponse), (status = 404, description = "Not found")))]
async fn api_get_user(State(app): State<AppState>, Path(id): Path<i64>) -> ApiResult<UserResponse> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;
    repository::find_user(&db, id)
        .await
        .map_err(db_err)?
        .map(|u| Json(u.into()))
        .ok_or_else(not_found)
}

#[utoipa::path(patch, path = "/users/{id}",
    params(("id" = i64, Path, description = "User ID")),
    request_body = PatchUserBody,
    responses((status = 200, body = UserResponse), (status = 404, description = "Not found")))]
async fn api_patch_user(
    State(app): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<PatchUserBody>,
) -> ApiResult<UserResponse> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;
    let patch = PatchUser {
        locale: body.locale,
        tier: body.tier,
        updated_at: None,
    };
    repository::update_user(&db, id, patch)
        .await
        .map_err(db_err)?
        .map(|u| Json(u.into()))
        .ok_or_else(not_found)
}

#[utoipa::path(delete, path = "/users/{id}",
    params(("id" = i64, Path, description = "User ID")),
    responses((status = 200, body = OkResponse), (status = 404, description = "Not found")))]
async fn api_delete_user(
    State(app): State<AppState>,
    Path(id): Path<i64>,
) -> ApiResult<serde_json::Value> {
    let db = app
        .runtime
        .db()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no db".into()))?;
    let deleted = repository::delete_user(&db, id).await.map_err(db_err)?;
    if deleted {
        Ok(Json(serde_json::json!({ "ok": true })))
    } else {
        Err(not_found())
    }
}
