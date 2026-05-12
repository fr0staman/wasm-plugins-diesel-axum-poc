CREATE TABLE IF NOT EXISTS plugin_bonus_bonus_ledger (
    id              BIGSERIAL    PRIMARY KEY,
    user_id         BIGINT       NOT NULL,
    bonus_cents     BIGINT       NOT NULL,
    calculated_date DATE         NOT NULL DEFAULT CURRENT_DATE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT bonus_ledger_one_per_day UNIQUE (user_id, calculated_date)
);
