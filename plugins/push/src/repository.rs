use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::Date;

use crate::db;
use crate::error::AppError;
use crate::models::PushNotifications;
use crate::schema::plugin_push_push_notifications as push_notifications;
use crate::types::NotificationEntry;

pub struct PushRepository;

impl PushRepository {
    /// Record a push notification for `uid`.
    /// Returns the number of affected rows; `0` means the conflict clause fired
    /// (a notification of this type was already sent to this user today).
    pub async fn insert_notification(uid: i64) -> Result<u64, AppError> {
        let query = diesel::insert_into(push_notifications::table)
            .values((
                push_notifications::user_id.eq(uid),
                push_notifications::channel.eq("fcm"),
                push_notifications::notification_type.eq("bonus"),
                push_notifications::sent_date.eq(sql::<Date>("CURRENT_DATE")),
            ))
            .on_conflict((
                push_notifications::user_id,
                push_notifications::sent_date,
                push_notifications::notification_type,
            ))
            .do_nothing();

        db::execute(query).await
    }

    pub async fn find_by_user(uid: i64) -> Result<Vec<NotificationEntry>, AppError> {
        let query = push_notifications::table
            .filter(push_notifications::user_id.eq(uid))
            .select(PushNotifications::as_select());

        let rows: Vec<PushNotifications> = db::query(query).await?;
        Ok(rows.into_iter().map(NotificationEntry::from).collect())
    }
}
