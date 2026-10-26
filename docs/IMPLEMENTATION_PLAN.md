# Tradeflow: 6-Week Implementation Roadmap & Test Checklists

This document outlines the step-by-step engineering roadmap for building Tradeflow. Following the build guardrail:
> *"Build in this order: state machine (pure code, unit-tested) → OMS and Postgres → risk checks → FIX gateway → fill simulator → positions and P&L → reconciliation → VaR. After each stage the system should demo end-to-end, even if it's rough."*

---

## Roadmap Overview

```mermaid
gantt
    title Tradeflow 6-Week Implementation Timeline
    dateFormat  YYYY-MM-DD
    section Core Infrastructure
    Week 1: State Machine & Domain Models        :w1, 2026-10-27, 7d
    Week 2: OMS & Event Sourcing (Postgres)      :w2, after w1, 7d
    section Trading Logic
    Week 3: Pre-Trade Risk Engine (Redis Lua)    :w3, after w2, 7d
    Week 4: FIX Gateway & Session Management     :w4, after w3, 7d
    section Execution & Accounting
    Week 5: Execution Simulator (Chaos Mode)     :w5, after w4, 7d
    Week 6: Position/P&L, Recon & VaR Engine     :w6, after w5, 7d
```

---

## Week 1: Pure State Machine & Domain Foundation

### Objectives
- Define canonical financial types: zero floating-point math, 64-bit integer shares, minor-unit prices.
- Implement the pure finite state machine (FSM) in Go with zero external dependencies.
- Write thorough table-driven unit tests and property-based tests verifying invariants.

### Milestones & Deliverables
- [ ] `internal/domain/types.go`: `Side` (Buy/Sell), `OrderType`, `TimeInForce`, `Price` (cents), `Qty`.
- [ ] `internal/domain/order.go`: `Order` struct with immutable creation and transition methods.
- [ ] `internal/statemachine/fsm.go`: FSM transition table and transition validation logic.
- [ ] `internal/statemachine/fsm_test.go`: 100% code coverage on all legal and illegal transitions.
- [ ] `internal/statemachine/property_test.go`: Property test using `pgregory.net/rapid` generating random action sequences.

### Test Checklist
- [x] **Unit Tests:**
  - Transition `PendingNew` $\to$ `New` (valid).
  - Transition `New` $\to$ `PartiallyFilled` with quantity accumulation.
  - Transition `PartiallyFilled` $\to$ `Filled` when cumulative quantity equals order quantity.
  - Transition `Filled` $\to$ `Cancelled` (must fail with `ErrIllegalTransition`).
  - Transition `Cancelled` $\to$ `PartiallyFilled` (must fail with `ErrLateFillOrIllegal`).
- [ ] **Property Test (Rapid):**
  - Invariant: `cum_qty` never exceeds `order_qty` under any valid sequence of partial fills.
  - Invariant: Order state never transitions once entering terminal states (`Filled`, `Cancelled`, `Rejected`).
  - Invariant: State machine cannot panic regardless of arbitrary fuzz inputs.

---

## Week 2: OMS, PostgreSQL Event Sourcing & Transactional Outbox

### Objectives
- Establish the Docker Compose environment (PostgreSQL 16, Redis 7, Kafka KRaft).
- Create PostgreSQL DDL migrations (`order_events`, `orders`, `outbox`).
- Implement the Order Management Service (OMS) event-sourced repository and state projection.
- Build the Transactional Outbox publisher polling/listening to `outbox` table and relaying to Kafka.

### Milestones & Deliverables
- [ ] `deploy/docker-compose.yml`: Core service containers configured with healthchecks.
- [ ] `sql/migrations/001_init_schema.sql`: Full DDL with constraints and indexes.
- [ ] `internal/storage/postgres.go`: Connection pooling, database migration runner.
- [ ] `internal/storage/order_repository.go`: Atomic transaction writing `order_events` and updating `orders`.
- [ ] `internal/outbox/publisher.go`: Background worker draining pending outbox rows to Kafka.
- [ ] `cmd/oms/main.go`: OMS daemon consuming `orders.accepted` and writing outbox events to `orders.routed`.

### Test Checklist
- [ ] **Integration Tests (Testcontainers):**
  - Verify append-only constraint: duplicate `(order_id, seq)` causes DB constraint violation.
  - Verify atomic rollback: simulated crash during event insertion rolls back the `orders` projection.
  - Outbox recovery: kill publisher mid-drain, restart, and assert zero lost events in Kafka.
  - Projection rebuild: clear `orders` table, replay all `order_events`, assert identical projected state.

---

## Week 3: Pre-Trade Risk Engine & Atomic Redis Checks

### Objectives
- Implement the 7 pre-trade risk checks (Max Qty, Max Notional, Price Band, Position Limit, Daily Notional, Restricted Symbols, Kill Switch).
- Implement Redis Lua scripts for atomic check-and-increment operations.
- Build the Risk Service consuming `orders.inbound` and publishing to `orders.accepted` or `orders.rejected`.

### Milestones & Deliverables
- [ ] `internal/risk/engine.go`: Risk rule evaluator returning specific reject codes (`RC01`–`RC07`).
- [ ] `internal/risk/lua/check_and_increment.lua`: Atomic Lua script for cumulative daily notional.
- [ ] `internal/risk/lua/check_position_limit.lua`: Atomic Lua script for net open position limits.
- [ ] `cmd/risk/main.go`: Risk microservice with Prometheus counters for approval/reject metrics.
- [ ] CLI command to toggle global and account-level kill switches.

### Test Checklist
- [ ] **Concurrency Tests:**
  - 50 concurrent goroutines submitting orders that together would breach the daily notional limit. Assert exact cutoff where approved total $\le$ limit and remaining orders receive `RC05`.
  - Kill Switch latency: trigger kill switch, verify immediate rejection of all subsequent orders within $< 1\text{ ms}$.
- [ ] **Unit Tests:**
  - Price band check: reject when order price exceeds $\pm 5\%$ of cached market price.
  - Restricted symbols: reject orders in banned equities (e.g., sanction list).

---

## Week 4: FIX Gateway (QuickFIX/Go) & Test Client

### Objectives
- Implement FIX protocol gateway using `quickfixgo`.
- Map FIX `NewOrderSingle` (`MsgType=D`) to domain model and publish to `orders.inbound`.
- Listen for order status updates from OMS and send outbound `ExecutionReport` (`MsgType=8`) to clients.
- Handle session management: Logon, Heartbeat, and Resend Requests.
- Build a Python/Go FIX test client to simulate real client trading sessions.

### Milestones & Deliverables
- [ ] `cmd/gateway/main.go`: QuickFIX acceptor configuration (`quickfix.cfg`).
- [ ] `internal/fix/mapper.go`: Bidirectional translation between FIX tags and domain types.
- [ ] `cmd/client/main.go`: Automated FIX initiator capable of firing scripted orders and cancel requests.
- [ ] Prometheus metrics: FIX message ingress rate, session status, gateway latency.

### Test Checklist
- [ ] **Session & Protocol Tests:**
  - Complete logon and heartbeat exchange.
  - Sequence number gap detection and resend request handling.
  - Submit `NewOrderSingle` $\to$ receive pending `ExecutionReport(ExecType=0, OrdStatus=A)`.
  - Reject response: risk failure sends `ExecutionReport(ExecType=8, OrdStatus=8, Text="RC02: ...")`.

---

## Week 5: Execution Simulator (Chaos Mode)

### Objectives
- Create the simulated execution venue (`SIM_VENUE_X`).
- Normal Mode: Generate realistic fill sequences (1–3 partial fills followed by full fill or rest).
- **Chaos Mode**: Inject realistic exchange anomalies (duplicates, reordering, delay, late cancel).
- Publish fills to `market.fills` partitioned by `Account|Symbol`.

### Milestones & Deliverables
- [ ] `cmd/execsim/main.go`: Venue simulator listening to `orders.routed`.
- [ ] `internal/execsim/generator.go`: Fill generator with configurable execution profiles (Aggressive, Passive, Partial).
- [ ] `internal/execsim/chaos.go`: Anomaly injector with toggles for:
  - Duplicate fill rate (e.g., 5% of fills re-sent with identical `ExecID`).
  - Out-of-order delivery (fill 2 emitted before fill 1).
  - Simulated latency spikes (50ms–500ms).
- [ ] Chaos control endpoints / CLI flags.

### Test Checklist
- [ ] **Chaos Verification Tests:**
  - Assert simulator produces duplicate `ExecID` when chaos mode is active.
  - Verify downstream services maintain data integrity under intentional chaos injection.

---

## Week 6: Position & P&L Engine, EOD Recon & VaR Analytics

### Objectives
- Implement the Position and P&L service in Go consuming `market.fills`.
- Handle deduplication via `UNIQUE(venue, exec_id)` in the `trades` table.
- Implement fill-before-order staging table (`pending_fills`) with retry worker.
- Build EOD Reconciliation in Python comparing internal book against clearing CSV.
- Build Historical VaR calculator in Python (1-day 99% VaR over 250 historical return scenarios).
- Set up Prometheus metrics and Grafana dashboard.

### Milestones & Deliverables
- [ ] `cmd/position/main.go`: Position keeper updating `positions` and `trades` tables.
- [ ] `internal/position/book.go`: Real-time VWAP and realized/unrealized P&L calculations.
- [ ] `internal/position/pending_worker.go`: Resolves early fills parked in `pending_fills`.
- [ ] `scripts/reconciliation/recon.py`: Detects `QTY_MISMATCH`, `MISSING_TRADE`, `EXTRA_TRADE`.
- [ ] `scripts/var/calculate_var.py`: Nightly 1-Day 99% historical VaR calculation.
- [ ] `deploy/grafana/dashboards/tradeflow_overview.json`: Complete operational dashboard.
- [ ] `scripts/chaos/run_chaos_test.ps1`: End-to-end chaos test runner.

### Test Checklist
- [ ] **Idempotency Property Test:**
  - Feed 1,000 randomized fills with 50% duplicate injection. Assert final net quantity and average cost match the exact mathematical result of unique trades.
- [ ] **Fill-Before-Order Test:**
  - Fire fill to Kafka *before* parent order is committed. Assert fill is parked in `pending_fills`, parent order is committed, retry worker wakes up, and position updates correctly.
- [ ] **Reconciliation Test:**
  - Inject 1 deliberate mismatch and 1 missing trade into test clearing CSV. Verify reconciliation script correctly flags both in `breaks` table.
- [ ] **VaR Calculation Test:**
  - Run synthetic price series with known distribution; verify calculated 99% VaR matches theoretical quantile.
