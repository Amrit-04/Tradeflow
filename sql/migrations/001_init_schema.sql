-- Tradeflow PostgreSQL 16 Schema Migration: 001_init_schema.sql
-- Enables UUID extension if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =====================================================================
-- 1. Order Events: Immutable, Append-Only Source of Truth (Event Sourcing)
-- =====================================================================
CREATE TABLE IF NOT EXISTS order_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id VARCHAR(64) NOT NULL,
    seq INT NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_order_seq UNIQUE(order_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id);
CREATE INDEX IF NOT EXISTS idx_order_events_created_at ON order_events(created_at);

-- =====================================================================
-- 2. Orders Projection: Read-Model for Order State
-- =====================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) PRIMARY KEY,
    cl_ord_id VARCHAR(64) NOT NULL,
    account VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    order_qty BIGINT NOT NULL CHECK (order_qty > 0),
    price BIGINT NOT NULL CHECK (price >= 0), -- Minor units (e.g. cents)
    state VARCHAR(24) NOT NULL,
    cum_qty BIGINT NOT NULL DEFAULT 0 CHECK (cum_qty >= 0),
    leaves_qty BIGINT NOT NULL CHECK (leaves_qty >= 0),
    avg_price BIGINT NOT NULL DEFAULT 0,      -- Minor units (e.g. cents)
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_cum_leaves CHECK (cum_qty + leaves_qty <= order_qty)
);

CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_state ON orders(state);
CREATE INDEX IF NOT EXISTS idx_orders_cl_ord_id ON orders(cl_ord_id);

-- =====================================================================
-- 3. Trades: Deduplicated on Venue + ExecID (Strict Idempotency)
-- =====================================================================
CREATE TABLE IF NOT EXISTS trades (
    trade_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    venue VARCHAR(16) NOT NULL,
    exec_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL REFERENCES orders(order_id),
    account VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    exec_qty BIGINT NOT NULL CHECK (exec_qty > 0),
    exec_price BIGINT NOT NULL CHECK (exec_price >= 0), -- Minor units
    transact_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_venue_exec UNIQUE(venue, exec_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_account_symbol ON trades(account, symbol);
CREATE INDEX IF NOT EXISTS idx_trades_order_id ON trades(order_id);
CREATE INDEX IF NOT EXISTS idx_trades_transact_time ON trades(transact_time);

-- =====================================================================
-- 4. Positions: Aggregated Holdings & Realized/Unrealized P&L
-- =====================================================================
CREATE TABLE IF NOT EXISTS positions (
    account VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    net_qty BIGINT NOT NULL DEFAULT 0,
    avg_cost BIGINT NOT NULL DEFAULT 0,   -- Minor units (cents)
    realized_pnl BIGINT NOT NULL DEFAULT 0, -- Minor units (cents)
    unrealized_pnl BIGINT NOT NULL DEFAULT 0, -- Minor units (cents)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(account, symbol)
);

-- =====================================================================
-- 5. Pending Fills: Staging for Early / Out-of-Order Fill Messages
-- =====================================================================
CREATE TABLE IF NOT EXISTS pending_fills (
    pending_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    venue VARCHAR(16) NOT NULL,
    exec_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_retry_at TIMESTAMPTZ,
    CONSTRAINT uq_pending_exec UNIQUE(venue, exec_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_fills_order_id ON pending_fills(order_id) WHERE status = 'PENDING';

-- =====================================================================
-- 6. Transactional Outbox: Guaranteed At-Least-Once Delivery to Kafka
-- =====================================================================
CREATE TABLE IF NOT EXISTS outbox (
    outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic VARCHAR(64) NOT NULL,
    partition_key VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(created_at) WHERE published_at IS NULL;

-- =====================================================================
-- 7. Reconciliation Breaks: End-of-Day Audit Mismatches
-- =====================================================================
CREATE TABLE IF NOT EXISTS breaks (
    break_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recon_date DATE NOT NULL,
    account VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    internal_qty BIGINT NOT NULL,
    clearing_qty BIGINT NOT NULL,
    break_type VARCHAR(32) NOT NULL CHECK (break_type IN ('QTY_MISMATCH', 'MISSING_TRADE', 'EXTRA_TRADE')),
    status VARCHAR(16) NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'INVESTIGATING', 'RESOLVED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_breaks_recon_date ON breaks(recon_date);
CREATE INDEX IF NOT EXISTS idx_breaks_status ON breaks(status);

-- =====================================================================
-- 8. Historical Risk Reports: 1-Day 99% VaR per Account
-- =====================================================================
CREATE TABLE IF NOT EXISTS risk_reports (
    report_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_date DATE NOT NULL,
    account VARCHAR(32) NOT NULL,
    var_99_1d BIGINT NOT NULL,      -- Value at Risk 99% 1-Day in cents
    total_notional BIGINT NOT NULL, -- Total portfolio notional in cents
    scenario_count INT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_risk_reports_date_account ON risk_reports(report_date, account);
