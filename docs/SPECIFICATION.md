# Tradeflow: Mini Trade Lifecycle & Pre-Trade Risk Platform
**System Specification & Architecture Reference**

---

## 1. Executive Summary

**Tradeflow** is an institutional-style equity order lifecycle and pre-trade risk management engine. It follows an equity order through the exact stages of a Tier-1 investment bank's algorithmic and cash equity technology stack:

$$\text{Client FIX Order} \longrightarrow \text{Pre-Trade Risk} \longrightarrow \text{OMS State Machine} \longrightarrow \text{Execution Simulator} \longrightarrow \text{Positions \& P\&L} \longrightarrow \text{EOD Reconciliation} \longrightarrow \text{Historical VaR}$$

Tradeflow is designed to solve real-world distributed systems and trading infrastructure challenges:
- **Zero double-counting** under duplicated or reordered messages.
- **Strict idempotency** where replaying execution fills produces deterministic final positions.
- **Event sourcing & transactional outbox** ensuring zero message loss between database commits and Kafka emission.
- **Atomic pre-trade checks** using Redis Lua scripts to eliminate race conditions across concurrent orders.
- **Fill-before-order resilience** parking early executions in a staging table until parent order registration.

---

## 2. Functional Requirements Matrix

| ID | Component | Operational Requirements |
|---|---|---|
| **F1** | **FIX Gateway** | • FIX 4.2 / 4.4 protocol handling via QuickFIX/Go.<br>• Inbound: `NewOrderSingle (MsgType=D)`, `OrderCancelRequest (F)`, `OrderCancelReplaceRequest (G)`.<br>• Outbound: `ExecutionReport (8)`, `OrderCancelReject (9)`, session `Reject (3)`.<br>• Full session lifecycle: Logon (`A`), Logout (`5`), Heartbeat (`0`), TestRequest (`1`), ResendRequest (`2`), Sequence Reset (`4`).<br>• Publishes raw validated messages to Kafka `orders.inbound` partitioned by `OrderID`. |
| **F2** | **Pre-Trade Risk Engine** | • Evaluates incoming orders against 7 strict risk checks before market routing.<br>• **RC01**: Single-order Max Quantity limit.<br>• **RC02**: Single-order Max Notional limit ($Qty \times Px$).<br>• **RC03**: Fat-finger price band ($\pm X\%$ deviation from last mark price).<br>• **RC04**: Per-account Net Position threshold.<br>• **RC05**: Per-account Daily Cumulative Notional limit.<br>• **RC06**: Restricted / Banned Symbol check.<br>• **RC07**: Global and per-account Kill Switch.<br>• Redis Lua script performs atomic check-and-increment to prevent concurrent limit bypass.<br>• Routes approved orders to `orders.accepted`, rejected to `orders.rejected`. |
| **F3** | **OMS & State Machine** | • Strict finite state machine governing order status transitions.<br>• Supported States: `PendingNew`, `New`, `PartiallyFilled`, `Filled`, `PendingCancel`, `Cancelled`, `PendingReplace`, `Replaced`, `Rejected`, `Expired`.<br>• Rejects and logs illegal state transitions (e.g., fill received after `Cancelled` or `Filled`).<br>• **Event Sourced**: Appends state changes to `order_events` table; updates `orders` projection table.<br>• **Transactional Outbox**: Writes outgoing events to `outbox` table in the same Postgres transaction; CDC outbox publisher relays events to `orders.routed`. |
| **F4** | **Execution Simulator** | • Simulated equity exchange venue (e.g., `SIM_VENUE_X`).<br>• Normal Mode: Consumes `orders.routed`, produces partial fills, full fills, rejections, and restings.<br>• **Chaos Mode**: Simulates real-world exchange anomalies:<br>  - Duplicate fills (duplicate `ExecID`).<br>  - Out-of-order fills (fill 2 before fill 1).<br>  - Delayed fills (injected latency).<br>  - Fill-before-order (execution arrives before OMS registers the order).<br>• Publishes fills to `market.fills` partitioned by `Account|Symbol`. |
| **F5** | **Position & P&L Engine** | • Consumes from `market.fills`.<br>• Computes Real-Time Book per `(Account, Symbol)`:<br>  - Net Position ($\sum \text{Buy Qty} - \sum \text{Sell Qty}$).<br>  - Volume-Weighted Average Cost (VWAP / average open price).<br>  - Realized P&L on closing or reducing trades.<br>  - Unrealized P&L calculated against simulated streaming market price feed.<br>• **Deduplication**: Enforces idempotency via `UNIQUE(venue, exec_id)` in `trades` table. |
| **F6** | **EOD Reconciliation** | • End-of-Day Python service comparing internal `positions` and `trades` against external clearing-house CSV (`clearing_eod.csv`).<br>• Identifies and categorizes breaks:<br>  - **BREAK_QTY_MISMATCH**: Internal net quantity $\neq$ clearing house net quantity.<br>  - **BREAK_MISSING_TRADE**: Trade exists in clearing house but absent internally.<br>  - **BREAK_EXTRA_TRADE**: Trade logged internally but unconfirmed by clearing.<br>• Writes break records into the `breaks` audit table. |
| **F7** | **Historical VaR Report** | • Nightly quantitative risk analytics in Python (pandas / numpy / scipy).<br>• Historical Simulation 1-Day 99% Value at Risk (VaR) per trading account.<br>• Calculates portfolio returns over historical window (e.g., 250 trading days) and identifies 1st percentile worst loss.<br>• Outputs risk metrics to `risk_reports` table and generates structured markdown/HTML summary. |

---

## 3. Non-Functional Architecture & Guarantees

```
                            Kafka Partitioning & Event Flow
                            
Client (FIX)
    │
    ▼
[FIX Gateway] ────────► Kafka: `orders.inbound` (Key: OrderID)
                              │
                              ▼
                        [Pre-Trade Risk] ◄─── Atomic Lua (Redis)
                              │
                 ┌────────────┴────────────┐
                 ▼ (Pass)                  ▼ (Fail)
     `orders.accepted`              `orders.rejected`
                 │                         │
                 ▼                         ▼
         [OMS State Machine] ◄─────── [Outbox Publisher]
                 │ (DB Transaction)
                 ├──► PostgreSQL: `order_events` (Append-Only)
                 ├──► PostgreSQL: `orders` (Current State Projection)
                 └──► PostgreSQL: `outbox` (Pending Events)
                 │
                 ▼
          `orders.routed` (Key: OrderID)
                 │
                 ▼
       [Execution Simulator] (Normal & Chaos Mode)
                 │
                 ▼
          `market.fills` (Key: Account|Symbol)
                 │
                 ▼
       [Position & P&L Engine] ◄── Market Data Price Stream
                 │
                 ├──► PostgreSQL: `trades` (Dedup on venue+exec_id)
                 └──► PostgreSQL: `positions` (NetQty, AvgCost, PnL)
```

### 3.1 Non-Functional Requirements (NFRs)

1. **Deterministic Idempotency:**
   - Every fill has `(Venue, ExecID)`. The `trades` table enforces a database unique constraint.
   - Replaying a stream of fills 5 times produces identical final position quantities and zero duplicate trade records.
2. **Auditability & Temporal Replay:**
   - All state transitions write immutable rows to `order_events(event_id, order_id, seq, type, payload_json, ts)`.
   - Any order's exact lifecycle can be reconstituted by replaying events from `seq = 1` to `seq = N`.
3. **High-Integrity Messaging:**
   - **Order flow** is partitioned by `OrderID` to preserve strict sequential causal consistency per order.
   - **Fill flow** is partitioned by `Account|Symbol` so position mutations for any individual instrument execute in strict order without concurrency deadlocks.
4. **Crash Recovery & Outbox Pattern:**
   - The OMS writes domain state transitions and outbound events in a single atomic database transaction.
   - If the OMS crashes after commit, the Transactional Outbox processor resumes on restart and guarantees at-least-once delivery to Kafka.
5. **Exact Money Handling:**
   - Float types (`float32`, `float64`, `DOUBLE PRECISION`) are prohibited for financial calculations.
   - Quantities are stored as 64-bit integers (`int64`).
   - Prices are represented as fixed-point minor units (e.g., cents or basis points, $100.50 \rightarrow 10050$) or PostgreSQL `NUMERIC(18, 4)`.

---

## 4. State Machine Specification

```
                  ┌──────────────┐
                  │  PendingNew  │
                  └──────┬───────┘
                         │
             ┌───────────┴───────────┐
             │ (Ack)                 │ (Reject)
             ▼                       ▼
      ┌─────────────┐         ┌──────────────┐
      │     New     │         │   Rejected   │ (Terminal)
      └──────┬──────┘         └──────────────┘
             │
             ├─── [Partial Fill] ───┐
             │                      │
             ▼                      ▼
      ┌─────────────┐        ┌──────────────┐
      │PartiallyFill│◄───────┤PartiallyFill │
      └──────┬──────┘        └──────────────┘
             │                      │
             │                      │
     [Full Fill]                [Full Fill]
             │                      │
             ▼                      ▼
      ┌─────────────┐        ┌──────────────┐
      │   Filled    │        │    Filled    │ (Terminal)
      └─────────────┘        └──────────────┘

  Pending Cancellations & Replaces:
  New / PartiallyFilled ──[Cancel Request]──► PendingCancel ──[Cancel Ack]──► Cancelled (Terminal)
  New / PartiallyFilled ──[Replace Request]─► PendingReplace ─[Replace Ack]─► Replaced
```

### Legal Transitions Table

| Current State | Trigger Event | Next State | Notes |
|---|---|---|---|
| `None` | `SubmitNew` | `PendingNew` | Gateway receives NewOrderSingle |
| `PendingNew` | `RiskPassed` | `New` | Routed to venue simulator |
| `PendingNew` | `RiskRejected` | `Rejected` | Terminal state |
| `New` | `PartialFill` | `PartiallyFilled` | $0 < \text{cum\_qty} < \text{order\_qty}$ |
| `New` | `FullFill` | `Filled` | $\text{cum\_qty} == \text{order\_qty}$ (Terminal) |
| `New` | `CancelReq` | `PendingCancel` | In-flight cancel |
| `New` | `ReplaceReq` | `PendingReplace` | In-flight cancel/replace |
| `PartiallyFilled` | `PartialFill` | `PartiallyFilled` | Increments cum_qty |
| `PartiallyFilled` | `FullFill` | `Filled` | $\text{cum\_qty} == \text{order\_qty}$ (Terminal) |
| `PartiallyFilled` | `CancelReq` | `PendingCancel` | Cancel remaining leaves |
| `PendingCancel` | `CancelAck` | `Cancelled` | Remaining qty cancelled (Terminal) |
| `PendingCancel` | `CancelReject`| `New` or `PartiallyFilled` | Reverts to prior active state |
| `PendingReplace`| `ReplaceAck` | `Replaced` | New qty or price applied |
| `PendingReplace`| `ReplaceReject`| `New` or `PartiallyFilled`| Reverts to prior active state |
| `Filled` | Any Fill / Cancel | **Illegal** | Logged as anomaly, rejected |
| `Cancelled` | Any Fill / Cancel | **Late Fill / Illegal** | Handled by Late Fill logic |

---

## 5. Pre-Trade Risk Checks (Detailed Rules)

| Code | Check Name | Evaluation Mechanism | Reject Reason Description |
|---|---|---|---|
| `RC01` | Max Order Quantity | $Qty \le \text{MaxQty}$ (e.g., 10,000 shares) | `ORDER_QTY_EXCEEDS_MAX` |
| `RC02` | Max Order Notional | $Qty \times Price \le \text{MaxNotional}$ (e.g., $1,000,000) | `ORDER_NOTIONAL_EXCEEDS_MAX` |
| `RC03` | Fat-Finger Price Band | $|Price - LastPrice| / LastPrice \le \text{Threshold}$ (e.g. 5%) | `PRICE_OUTSIDE_BAND` |
| `RC04` | Per-Account Position Limit | $|\text{CurrentPos} \pm OrderQty| \le \text{MaxPosition}$ | `ACCOUNT_POSITION_LIMIT_BREACH` |
| `RC05` | Per-Account Daily Notional | $\text{DailyNotional} + (Qty \times Price) \le \text{DailyLimit}$ | `ACCOUNT_DAILY_NOTIONAL_BREACH` |
| `RC06` | Restricted Symbol List | Symbol not in Redis Set `restricted_symbols` | `SYMBOL_RESTRICTED` |
| `RC07` | Kill Switch | Key `kill_switch:global` or `kill_switch:{account}` is 0 | `KILL_SWITCH_ACTIVE` |

### Atomic Redis Lua Execution
To eliminate the classic race condition where two concurrent orders simultaneously check the limit and both pass before either updates the counter:
```lua
-- KEYS[1] = account_daily_notional_key, ARGV[1] = order_notional, ARGV[2] = max_limit
local current = redis.call('GET', KEYS[1])
if not current then
    current = 0
else
    current = tonumber(current)
end
if current + tonumber(ARGV[1]) > tonumber(ARGV[2]) then
    return 0 -- Rejected
else
    redis.call('INCRBY', KEYS[1], ARGV[1])
    return 1 -- Approved
end
```

---

## 6. PostgreSQL Relational Schema

```sql
-- 1. Order Events (Immutable, Append-Only Source of Truth)
CREATE TABLE order_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id VARCHAR(64) NOT NULL,
    seq INT NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_order_seq UNIQUE(order_id, seq)
);

-- 2. Orders Projection (Fast Query State)
CREATE TABLE orders (
    order_id VARCHAR(64) PRIMARY KEY,
    cl_ord_id VARCHAR(64) NOT NULL,
    account VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    side VARCHAR(4) NOT NULL,       -- BUY / SELL
    order_qty BIGINT NOT NULL,      -- Integer shares
    price BIGINT NOT NULL,          -- Integer cents / fixed-point
    state VARCHAR(24) NOT NULL,
    cum_qty BIGINT NOT NULL DEFAULT 0,
    leaves_qty BIGINT NOT NULL,
    avg_price BIGINT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- 3. Executed Trades (Deduplicated on venue + exec_id)
CREATE TABLE trades (
    trade_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    venue VARCHAR(16) NOT NULL,
    exec_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL REFERENCES orders(order_id),
    account VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    side VARCHAR(4) NOT NULL,
    exec_qty BIGINT NOT NULL,
    exec_price BIGINT NOT NULL,
    transact_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_venue_exec UNIQUE(venue, exec_id)
);

-- 4. Positions (Current Holdings and PnL)
CREATE TABLE positions (
    account VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    net_qty BIGINT NOT NULL DEFAULT 0,
    avg_cost BIGINT NOT NULL DEFAULT 0, -- Minor units (cents)
    realized_pnl BIGINT NOT NULL DEFAULT 0,
    unrealized_pnl BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(account, symbol)
);

-- 5. Staging for Early / Out-of-Order Fills
CREATE TABLE pending_fills (
    pending_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    venue VARCHAR(16) NOT NULL,
    exec_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_pending_exec UNIQUE(venue, exec_id)
);

-- 6. Transactional Outbox
CREATE TABLE outbox (
    outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic VARCHAR(64) NOT NULL,
    partition_key VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ
);

-- 7. Reconciliation Breaks Table
CREATE TABLE breaks (
    break_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recon_date DATE NOT NULL,
    account VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    internal_qty BIGINT NOT NULL,
    clearing_qty BIGINT NOT NULL,
    break_type VARCHAR(32) NOT NULL, -- QTY_MISMATCH, MISSING_TRADE, EXTRA_TRADE
    status VARCHAR(16) NOT NULL DEFAULT 'OPEN',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- 8. Historical Risk & VaR Reports
CREATE TABLE risk_reports (
    report_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    report_date DATE NOT NULL,
    account VARCHAR(32) NOT NULL,
    var_99_1d BIGINT NOT NULL,      -- Value at Risk 99% 1-Day in cents
    total_notional BIGINT NOT NULL,
    scenario_count INT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
```

---

## 7. Interview Defense Guide

### Q1: What happens if an execution fill arrives before its parent order?
> **Defense:** This is the *fill-before-order* race condition common in distributed trading networks (e.g., if execution occurs and the fill message reaches Kafka faster than the OMS finishes recording the parent order). Our Position & P&L engine queries the orders table. If the order is not found, the fill is inserted into `pending_fills` with status `PENDING` and a retry schedule (exponential backoff). Once the OMS processes the parent order, a post-insert hook triggers replay of parked fills for that `order_id`. A TTL alert fires if a fill remains orphaned past 30 seconds.

### Q2: How do you prevent two concurrent orders from breaching a single position limit?
> **Defense:** Checking limits in standard application memory or via simple `SELECT then UPDATE` statements invites a classic check-then-act race condition. Tradeflow executes limit evaluation inside a **Redis Lua script**. Because Redis evaluates Lua scripts atomically on a single thread, the check and the counter increment occur in one unbreakable operation. No two concurrent orders can read the stale counter simultaneously.

### Q3: Why Kafka partition keys, and what ordering guarantees do you actually get?
> **Defense:** Kafka guarantees total order *within a partition*, not across partitions. We key `orders.inbound` and `orders.routed` by `OrderID` so every transition of an individual order is serialized on the same partition. In contrast, fills in `market.fills` are keyed by `Account|Symbol` because multiple orders can alter the net position of the same equity. Partitioning fills by `Account|Symbol` ensures that position calculations for any given instrument are processed in strict chronological sequence without database row lock contention.

### Q4: What if the OMS crashes after committing to PostgreSQL but before publishing to Kafka?
> **Defense:** We implement the **Transactional Outbox pattern**. The state transition in `order_events`, the update to `orders`, and the creation of the outbound event in `outbox` happen inside a single ACID database transaction. If the service crashes right after the DB commit, the event is safely recorded in PostgreSQL. On restart, the Outbox Publisher worker reads un-emitted rows from `outbox`, publishes them to Kafka, and marks them `published_at`. Because Kafka delivery is at-least-once, downstream consumers are idempotent.

### Q5: How is an OrderCancelReplaceRequest different from Cancel-and-New?
> **Defense:** A Replace request retains the original order's time priority on the exchange if only quantity is decreased, whereas Cancel-and-New creates an entirely new order that loses queue priority. Architecturally, Replace introduces tricky race conditions: while the Replace request is in-flight (`PendingReplace`), a fill for the original quantity might already be in transit from the venue. The OMS must reconcile `cum_qty` upon receiving the replace confirmation and reject replaces if the new quantity is less than the already filled quantity.
