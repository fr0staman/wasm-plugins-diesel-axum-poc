use anyhow::Result;
use chrono::Utc;
use diesel::OptionalExtension;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use crate::db::DbPool;
use crate::models::{NewPayment, NewUser, PatchUser, Payment, User};
use crate::schema::{payments, users};

// ── Users ─────────────────────────────────────────────────────────────────────

pub async fn find_user(pool: &DbPool, id: i64) -> Result<Option<User>> {
    let mut conn = pool.get_conn().await?;
    users::table
        .find(id)
        .select(User::as_select())
        .first(&mut *conn)
        .await
        .optional()
        .map_err(Into::into)
}

pub async fn list_users(pool: &DbPool, tenant_id: i64) -> Result<Vec<User>> {
    let mut conn = pool.get_conn().await?;
    users::table
        .filter(users::tenant_id.eq(tenant_id))
        .select(User::as_select())
        .order(users::id.asc())
        .load(&mut *conn)
        .await
        .map_err(Into::into)
}

pub async fn create_user(pool: &DbPool, new: NewUser<'_>) -> Result<User> {
    let mut conn = pool.get_conn().await?;
    diesel::insert_into(users::table)
        .values(&new)
        .returning(User::as_returning())
        .get_result(&mut *conn)
        .await
        .map_err(Into::into)
}

pub async fn update_user(pool: &DbPool, id: i64, patch: PatchUser) -> Result<Option<User>> {
    let mut conn = pool.get_conn().await?;
    let patch = PatchUser {
        updated_at: Some(Utc::now()),
        ..patch
    };
    diesel::update(users::table.find(id))
        .set(&patch)
        .returning(User::as_returning())
        .get_result(&mut *conn)
        .await
        .optional()
        .map_err(Into::into)
}

pub async fn delete_user(pool: &DbPool, id: i64) -> Result<bool> {
    let mut conn = pool.get_conn().await?;
    let n = diesel::delete(users::table.find(id))
        .execute(&mut *conn)
        .await?;
    Ok(n > 0)
}

// ── Payments ──────────────────────────────────────────────────────────────────

pub async fn find_payment(pool: &DbPool, id: i64) -> Result<Option<Payment>> {
    let mut conn = pool.get_conn().await?;
    payments::table
        .find(id)
        .select(Payment::as_select())
        .first(&mut *conn)
        .await
        .optional()
        .map_err(Into::into)
}

pub async fn list_user_payments(pool: &DbPool, user_id: i64) -> Result<Vec<Payment>> {
    let mut conn = pool.get_conn().await?;
    payments::table
        .filter(payments::user_id.eq(user_id))
        .select(Payment::as_select())
        .order(payments::id.asc())
        .load(&mut *conn)
        .await
        .map_err(Into::into)
}

pub async fn create_payment(pool: &DbPool, new: NewPayment<'_>) -> Result<Payment> {
    let mut conn = pool.get_conn().await?;
    diesel::insert_into(payments::table)
        .values(&new)
        .returning(Payment::as_returning())
        .get_result(&mut *conn)
        .await
        .map_err(Into::into)
}

pub async fn delete_payment(pool: &DbPool, id: i64) -> Result<bool> {
    let mut conn = pool.get_conn().await?;
    let n = diesel::delete(payments::table.find(id))
        .execute(&mut *conn)
        .await?;
    Ok(n > 0)
}
