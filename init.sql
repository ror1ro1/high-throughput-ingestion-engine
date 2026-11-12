CREATE TABLE IF NOT EXISTS transaction_events (
    event_id VARCHAR(64) PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    user_id VARCHAR(32) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    payment_method VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    ip_address VARCHAR(45) NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_user_id ON transaction_events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_status ON transaction_events(status);