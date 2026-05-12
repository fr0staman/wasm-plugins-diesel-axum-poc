CREATE TABLE IF NOT EXISTS plugin_push_push_notifications (
    id                BIGSERIAL    PRIMARY KEY,
    user_id           BIGINT       NOT NULL,
    channel           VARCHAR(50)  NOT NULL DEFAULT 'fcm',
    notification_type VARCHAR(100) NOT NULL DEFAULT 'bonus',
    sent_date         DATE         NOT NULL DEFAULT CURRENT_DATE,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT push_notifications_one_per_day
        UNIQUE (user_id, sent_date, notification_type)
);
