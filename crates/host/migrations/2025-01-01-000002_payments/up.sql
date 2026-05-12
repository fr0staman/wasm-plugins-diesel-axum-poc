CREATE TABLE IF NOT EXISTS payments (
    id           BIGSERIAL   PRIMARY KEY,
    user_id      BIGINT      NOT NULL REFERENCES users(id),
    amount_cents BIGINT      NOT NULL,
    currency     VARCHAR(3)  NOT NULL DEFAULT 'USD',
    method       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
