FULL INSTRUCTION HOW TO USE ON THE BOTTOM PAGE
markdown
## Prerequisites
* [Docker](https://docker.com)
* [Docker Compose](https://docker.com)

## How to Run
1. Clone the repository:
   ```bash
   git clone https://github.com
   cd twoja-nazwa-repozytorium
   ```

2. Build and start the application along with the PostgreSQL database:
   ```bash
   docker-compose up --build
   ```

3. Verify that the service is running by checking the stats endpoint:
   ```bash
   curl http://localhost:8080/stats
   ```

   
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

======================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================


===========================================================================
FULL INSTRUCTION OF USE — HIGH-THROUGHPUT INGESTION ENGINE (KOTLIN vs RUST)
===========================================================================

1) INSTALL TOOLS VIA POWERSHELL (ADMINISTRATOR MODE)
---------------------------------------------------------------------------
winget install --id Git.Git -e --source winget
winget install -e --id EclipseAdoptium.Temurin.21.JDK
winget install -e --id Rustlang.Rustup
winget install -e --id Python.Python.3.12
winget install -e --id PostgreSQL.PostgreSQL.16

IMPORTANT MANUAL STEPS AFTER WINGET:
* Rust Linker: Download & install Visual Studio Build Tools with "Desktop development with C++" selected (provides link.exe).
* Gradle: Download Gradle 8.14.7 (binary-only) from gradle.org/releases, extract to C:\Gradle, and add C:\Gradle\bin to System Environment Variables -> Path.
* Windows Security: If Windows blocks Rust compilation (os error 4551), turn off "Smart App Control" in Windows Security.
* Restart Terminal: Close PowerShell and open a fresh CMD window so Windows loads the updated Path.

===========================================================================
2) CLONE REPOSITORY (IN CMD)
===========================================================================
mkdir C:\Users\%USERNAME%\Projects
cd C:\Users\%USERNAME%\Projects
git clone https://github.com
cd high-throughput-ingestion-engine

===========================================================================
3) PREPARE POSTGRESQL 16 DATABASE (SKIP IF ALREADY CREATED)
===========================================================================
1. Open PowerShell (as Administrator) and connect to PostgreSQL (enter the password set during installation):
& "C:\Program Files\PostgreSQL\16\bin\psql.exe" -U postgres

2. In postgres=# console, paste and press Enter:
ALTER USER postgres WITH PASSWORD 'secret_password';
CREATE USER synerise_user WITH PASSWORD 'secret_password';
CREATE DATABASE events_db OWNER synerise_user;

3. Switch to the new database:
\c events_db

4. In events_db=# console, paste and press Enter:
GRANT ALL PRIVILEGES ON DATABASE events_db TO synerise_user;

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

GRANT ALL PRIVILEGES ON TABLE transaction_events TO synerise_user;
CREATE INDEX IF NOT EXISTS idx_events_user_id ON transaction_events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_status ON transaction_events(status);

5. Exit SQL console:
\q

===========================================================================
4) RUN & BENCHMARK VERSION 1: KOTLIN ENGINE
===========================================================================
1. Open fresh CMD #1 (keep it open while testing):
cd C:\Users\%USERNAME%\Projects\high-throughput-ingestion-engine\kotlin-service
gradle run

2. Wait for "Uruchamianie Kotlin Ingestion Engine na porcie 8080...", then open fresh CMD #2:
cd C:\Users\%USERNAME%\Projects\high-throughput-ingestion-engine
py -m pip install httpx faker
py load_tester.py

3. Stop Kotlin before starting Rust:
Click on CMD #1 -> press Ctrl + C -> type Y -> press Enter (frees port 8080).

===========================================================================
5) RUN & BENCHMARK VERSION 2: RUST ENGINE
===========================================================================
1. In CMD #1, build and start Rust:
cd C:\Users\%USERNAME%\Projects\high-throughput-ingestion-engine\rust-service
cargo run --release

2. Wait for "Uruchamianie Rust Ingestion Engine (Axum + Tokio) na porcie 8080...", then in CMD #2 run:
cd C:\Users\%USERNAME%\Projects\high-throughput-ingestion-engine
py load_tester.py

3. Stop Rust when finished:
Click on CMD #1 -> press Ctrl + C.

(Optional) Check live metrics in browser while any server is running:
http://localhost:8080/stats


