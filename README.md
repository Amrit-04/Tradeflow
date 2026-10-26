# Tradeflow 🌊
> **Mini Trade Lifecycle and Pre-Trade Risk Platform**
> A deterministic, event-sourced equity order processing engine built in Go, PostgreSQL, Redis, and Kafka.

---

## 1. What is Tradeflow?

Tradeflow models an equity order through the exact pipeline it traverses in an institutional investment bank's cash trading infrastructure:

$$\text{Client FIX Order} \longrightarrow \text{Pre-Trade Risk} \longrightarrow \text{OMS State Machine} \longrightarrow \text{Execution Simulator} \longrightarrow \text{Positions \& P\&L} \longrightarrow \text{EOD Reconciliation} \longrightarrow \text{Historical VaR}$$

It focuses on the core plumbing and distributed systems challenges that banking infrastructure solves daily:
- **Zero double-counting** under duplicate or out-of-order execution reports.
- **Strict idempotency** where replaying execution fills changes nothing.
- **Event sourcing & Transactional Outbox** guaranteeing zero lost events between PostgreSQL commits and Kafka message delivery.
- **Atomic risk enforcement** via Redis Lua scripts preventing concurrent limit breaches.
- **Fill-before-order resilience** parking early executions until parent order registration.

---

## 2. Architecture Diagram

```
                        ┌──────────────────────┐
  Test Client  ──FIX──► │   FIX Gateway (Go)   │ ──► ExecutionReports back to client
 (QuickFIX/Go)          │ session + validation │
                        └──────────┬───────────┘
                                   │ orders.inbound  (partition key = order_id)
                                   ▼
                        ┌──────────────────────┐        ┌───────────┐
                        │  Pre-Trade Risk (Go) │◄──────►│   Redis   │
                        │  7 limit checks      │        │ counters, │
                        └───────┬──────┬───────┘        │ idem keys │
                 orders.accepted│      │orders.rejected └───────────┘
                                ▼      ▼
                        ┌──────────────────────┐
                        │   OMS (Go)           │──► Postgres: order_events (append-only)
                        │   state machine      │             orders (projection)
                        │   outbox publisher   │             outbox (reliable publish)
                        └───────┬──────────────┘
                                │ orders.routed (partition key = order_id)
                                ▼
                        ┌──────────────────────┐
                        │ Execution Simulator  │  chaos mode: dupes, reorder, delay
                        │  (Simulated Venue)   │
                        └───────┬──────────────┘
                                │ fills (partition key = account|symbol)
                                ▼
                        ┌──────────────────────┐   market prices ◄── price simulator
                        │ Position & P&L (Go)  │──► Postgres: trades (dedup venue+exec_id)
                        └───────┬──────────────┘              positions (net_qty, pnl)
                                │
                  ┌─────────────┴────────────┐
                  ▼                          ▼
       EOD Reconciliation (Python)   Historical VaR (Python)
       vs clearing CSV → breaks      nightly → risk_reports

   All services ──► Prometheus ──► Grafana
```

---

## 3. Project Structure

```
Tradeflow/
├── cmd/
│   ├── gateway/            # QuickFIX Inbound Gateway (Go)
│   ├── risk/               # Pre-Trade Risk Engine (Go)
│   ├── oms/                # Order Management System & Outbox Publisher (Go)
│   ├── execsim/            # Simulated Execution Venue & Chaos Injector (Go)
│   ├── position/           # Position & Real-Time P&L Keeper (Go)
│   └── client/             # Interactive / Scripted FIX Test Client (Go)
├── internal/
│   ├── domain/             # Canonical financial types (Minor units, zero float)
│   ├── statemachine/       # Pure OMS finite state machine
│   ├── risk/               # Risk rules, evaluation engine, Redis Lua scripts
│   ├── events/             # Domain events, Kafka message payloads & envelopes
│   ├── storage/            # Postgres repository, outbox, append-only order_events
│   ├── idempotency/        # Dedup logic & Redis cache keys
│   └── config/             # Environment & service configurations
├── sql/
│   └── migrations/         # PostgreSQL 16 DDL migrations
├── scripts/
│   ├── reconciliation/     # Python EOD reconciliation vs clearing CSV
│   ├── var/                # Python Historical Simulation 1-Day 99% VaR
│   ├── chaos/              # End-to-end chaos verification scripts
│   └── benchmarks/         # Latency & throughput benchmark scripts
├── deploy/
│   ├── docker-compose.yml  # Local stack (Postgres, Redis, Kafka, Prometheus, Grafana)
│   ├── prometheus/         # Prometheus scraper configuration
│   └── grafana/            # Pre-provisioned dashboards & data sources
└── docs/
    ├── SPECIFICATION.md    # Full system specification and data models
    └── IMPLEMENTATION_PLAN.md # 6-week milestones and test checklists
```

---

## 4. Key Design Decisions

1. **Event Sourcing for Orders:** Every transition is an immutable row in `order_events`. The `orders` table is a fast read-model projection.
2. **Partitioning:** Inbound and routed orders are keyed by `order_id` to enforce linear order-level sequencing. Fills are keyed by `account|symbol` to preserve execution order per equity without row-lock contention.
3. **At-Least-Once Delivery + Idempotent Consumers:** Every execution fill is deduplicated on `UNIQUE(venue, exec_id)`.
4. **Transactional Outbox:** The OMS writes the state transition and outbound event within a single ACID transaction, with a dedicated publisher worker streaming to Kafka.
5. **Atomic Risk Limits:** Redis Lua scripts evaluate and increment risk usage in a single atomic step, eliminating check-then-act concurrency bugs.
6. **Fill-Before-Order Parking:** Early execution fills that reach the engine before parent order registration are staged in `pending_fills` and replayed upon order confirmation.
7. **Minor-Unit Arithmetic:** All prices and monetary amounts are stored as integers (cents / basis points) or `NUMERIC`, strictly eliminating IEEE 754 floating-point inaccuracies.

---

## 5. Scope Guardrails (What Tradeflow is NOT)

To maintain high credibility and avoid overreach:
- **NOT** a matching engine or public exchange.
- **NOT** a quantitative alpha or trading strategy generator.
- **NOT** an options / derivatives pricing platform.
- **NOT** a Kubernetes / multi-cloud microservices sprawl (runs cleanly via Docker Compose).

---

## 6. Quick Start

### Prerequisites
- **Go:** 1.21+ (tested on Go 1.27)
- **Docker & Docker Compose**
- **Python:** 3.11+

### Running the Stack
```powershell
# 1. Start infrastructure services (Postgres, Redis, Kafka, Prometheus, Grafana)
docker compose -f deploy/docker-compose.yml up -d

# 2. Run state machine tests & property tests
go test -v ./internal/statemachine/...

# 3. Check documentation
Get-Content docs/SPECIFICATION.md
Get-Content docs/IMPLEMENTATION_PLAN.md
```
