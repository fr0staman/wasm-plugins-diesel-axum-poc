use chrono::{Datelike, NaiveDate, NaiveDateTime};
use diesel::prelude::*;

use crate::schema::*;
use crate::types::NotificationEntry;

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = plugin_push_push_notifications)]
pub struct PushNotifications {
    pub id: i64,
    pub user_id: i64,
    pub channel: String,
    pub notification_type: String,
    pub sent_date: NaiveDate,
    pub created_at: NaiveDateTime,
}

impl From<PushNotifications> for NotificationEntry {
    fn from(m: PushNotifications) -> Self {
        let d = m.sent_date;
        NotificationEntry {
            id: m.id,
            channel: m.channel,
            notification_type: m.notification_type,
            sent_date: format!("{}-{:02}-{:02}", d.year(), d.month(), d.day()),
        }
    }
}
