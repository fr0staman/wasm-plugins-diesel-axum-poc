use chrono::{DateTime, Utc};
use diesel::prelude::*;

use crate::schema::{payments, users};

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = users)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct User {
    pub id: i64,
    pub tenant_id: i64,
    pub email: String,
    pub locale: String,
    pub tier: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = users)]
pub struct NewUser<'a> {
    pub tenant_id: i64,
    pub email: &'a str,
    pub locale: &'a str,
    pub tier: &'a str,
}

#[derive(Debug, Default, AsChangeset)]
#[diesel(table_name = users)]
pub struct PatchUser {
    pub locale: Option<String>,
    pub tier: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = payments)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Payment {
    pub id: i64,
    pub user_id: i64,
    pub amount_cents: i64,
    pub currency: String,
    pub method: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = payments)]
pub struct NewPayment<'a> {
    pub user_id: i64,
    pub amount_cents: i64,
    pub currency: &'a str,
    pub method: &'a str,
}
