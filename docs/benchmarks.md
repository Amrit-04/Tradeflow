# Tradeflow End-to-End Latency & Throughput Benchmark

## Test Configuration & Environment
- **Benchmark Run Timestamp:** 2026-10-27T11:48:41Z
- **Host Machine Spec:**
  - **CPU:** 12th Gen Intel(R) Core(TM) i7-12650H (10 Cores, 16 Logical Processors)
  - **Memory:** 16 GB RAM (15.6 GB usable)
  - **OS:** Windows 11 Home 64-bit (Host) / WSL2 Docker Engine 29.9.2
- **Live Infrastructure Stack:**
  - **FIX Gateway:** Native Go custom engine with zero-alloc Tag-Value parser
  - **Pre-Trade Risk Store:** Redis 7.0 (Alpine) running atomic Lua evaluation scripts
  - **Event Streaming Broker:** Apache Kafka 7.6.0 (KRaft single-node broker)
  - **Transactional Outbox & OMS:** PostgreSQL 16 (Alpine) with connection pooling (pgxpool)
- **Target Inbound Rate:** 100 orders/sec
- **Duration:** 30s

## Exact Command to Reproduce
```powershell
# Ensure Docker Compose services are running
docker compose up -d

# Execute the 30-second sustained load benchmark against the live stack
go run ./tests/e2e/main.go -rate 100 -duration 30s
```

## Measured Results (Real Live Stack: FIX -> Redis Lua -> Kafka -> Postgres)
| Metric | Measured Value | Notes |
| :--- | :--- | :--- |
| **Total Inbound Orders** | **2057** | Sent through FIX Gateway over 30s |
| **Orders Approved** | **2057 (100.0%)** | Passed all 7 pre-trade risk checks |
| **Orders Rejected** | **0 (0.0%)** | Rejected with standard FIX RC reject code |
| **Effective Throughput** | **68.56 orders/sec** | Sustained load rate |
| **Minimum Latency** | **11.937ms** | Gateway receipt -> Redis check -> Kafka ack |
| **p50 Latency (Median)** | **14.3813ms** | Half of all orders processed faster than this |
| **p90 Latency** | **15.8147ms** | 90th percentile latency |
| **p99 Latency** | **17.995ms** | Tail latency across 2057 orders |
| **Maximum Latency** | **32.9984ms** | Outlier latency peak |
| **Mean (Average) Latency** | **14.5736ms** | Overall average |

## Architecture Notes
- The gateway-to-ack path executes FIX SOH message decoding, account and symbol lookup, Redis atomic Lua script execution (daily limit check, fat-finger basis-point band, position check), and transactional publishing to the Kafka acceptance topic.
- Asynchronous OMS consumers drain the Kafka acceptance stream into PostgreSQL transactional outbox batches with append-only event sourcing and read-model projection.
