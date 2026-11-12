markdown
# High-Throughput Event Ingestion Engine (Kotlin vs Rust)

A comparative backend engineering project evaluating two modern systems/backend stacks—**Kotlin (JVM)** and **Rust**—handling concurrent HTTP transaction event streams, in-memory buffering with backpressure, and micro-batched persistence into **PostgreSQL**.

Built to simulate high-volume FinTech / E-commerce telemetry pipelines (compatible with [enterprise-traffic-generator](https://github.com)).

---

## 🏗️ Architecture & Key Patterns

```text
[ Python Async Load Generator ]
         │  (50 concurrent workers, JSON over HTTP POST /events)
         ▼
┌──────────────────────────────────────────────────────────┐
│               Ingestion Engine (Port 8080)               │
│                                                          │
│  1. Non-blocking HTTP Handler (Ktor Netty / Axum)        │
│  2. Bounded MPSC Channel (Capacity: 50,000 events)       │
│     └── Backpressure: Returns HTTP 429 when full         │
│  3. Background Micro-Batcher Worker                      │
│     └── Flushes every 500 events OR 100ms timeout        │
└────────────────────────┬─────────────────────────────────┘
                         │  (Bulk / Batch INSERT + ON CONFLICT DO NOTHING)
                         ▼
               [ PostgreSQL 16 Database ]
```

### Engineering Highlights
* **Bounded Queue & Backpressure:** Uses a fixed-capacity in-memory channel (`50,000` events) with non-blocking `trySend` / `try_send`. Prevents `OutOfMemoryError` crashes under traffic spikes by shedding excess load immediately with `HTTP 429 Too Many Requests`.
* **Micro-Batching Persistence:** Decouples HTTP ingestion from disk I/O. A dedicated background worker aggregates incoming events into batches of `500` records (or flushes every `100ms` during low traffic) to minimize database transaction overhead.
* **Idempotent Writes:** Uses `ON CONFLICT (event_id) DO NOTHING` to guarantee safe retries without failing entire batches on duplicate events.
* **Lock-Free Telemetry:** Exposes real-time ingestion metrics (`GET /stats`) tracked via atomic counters (`AtomicLong` / `AtomicU64`).

---

## 📊 Benchmark Results (Side-by-Side)

Tested locally on Windows 11 against PostgreSQL 16 (`50` concurrent async workers, `10s` sustained load, `500`-event batch size):

| Metric | Kotlin (`Ktor` + `Coroutines` + `HikariCP`) | Rust (`Axum` + `Tokio` + `SQLx`) | Delta (Rust vs Kotlin) |
| :--- | :---: | :---: | :---: |
| **Persisted Events** | 4,138 | **4,751** | **+14.8%** |
| **Throughput (RPS)** | 409.90 req/s | **472.68 req/s** | **+15.3%** |
| **Average Latency** | 121.23 ms | **105.29 ms** | **-13.1%** |
| **p95 Latency** | 374.82 ms | **335.73 ms** | **-10.4%** |
| **p99 Latency** | 668.12 ms | **630.80 ms** | **-5.6%** |
| **Dropped / Failed** | 0 / 0 | **0 / 0** | 100% reliability |

*(Note: Total throughput in this local run was bounded by the Python `Faker` payload generator running on the same host; both services operated well below CPU saturation).*

---

## 🛠️ Tech Stack

* **Kotlin Service (`/kotlin-service`):** Kotlin 2.0, Ktor (Netty engine), Kotlinx Coroutines & Channels, Kotlinx Serialization, HikariCP, PostgreSQL JDBC.
* **Rust Service (`/rust-service`):** Rust (Edition 2021), Axum, Tokio (`mpsc` channels), Serde, SQLx (`PgPool` & `QueryBuilder` bulk inserts).
* **Database & Infra:** PostgreSQL 16 (`init.sql` schema + `docker-compose.yml`).
* **Load Testing:** Python 3.12 (`asyncio`, `httpx`, `Faker`).

---

## 🚀 Quick Start

### 1. Start PostgreSQL
```bash
docker compose up -d
```
*(Or apply `init.sql` to a local PostgreSQL 16 instance with database `events_db`).*

### 2. Run Kotlin Engine
```bash
cd kotlin-service
gradle run
```

### 3. Run Rust Engine
```bash
cd rust-service
cargo run --release
```

### 4. Execute Load Benchmark
```bash
pip install httpx faker
python load_tester.py
```