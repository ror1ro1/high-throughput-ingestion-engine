use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool, QueryBuilder, Postgres};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

// 1. Model danych identyczny jak w Kotlinie i generatorze w Pythonie
#[derive(Debug, Deserialize, Clone)]
struct TransactionEvent {
    event_id: String,
    timestamp: String,
    user_id: String,
    amount: f64,
    payment_method: String,
    status: String,
    ip_address: String,
}

#[derive(Serialize)]
struct EngineStats {
    engine: &'static str,
    #[serde(rename = "receivedEvents")]
    received_events: u64,
    #[serde(rename = "persistedEvents")]
    persisted_events: u64,
    #[serde(rename = "droppedEvents")]
    dropped_events: u64,
    #[serde(rename = "bufferLimit")]
    buffer_limit: usize,
}

// Współdzielony stan aplikacji (bezpieczny między wątkami dzięki Arc i AtomicU64)
struct AppState {
    sender: mpsc::Sender<TransactionEvent>,
    received_count: AtomicU64,
    persisted_count: AtomicU64,
    dropped_count: AtomicU64,
    buffer_limit: usize,
}

#[tokio::main]
async fn main() {
    // 2. Pula połączeń do PostgreSQL (SQLx PgPool - odpowiednik HikariCP)
    let database_url = "postgres://synerise_user:secret_password@localhost:5432/events_db";
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .expect("Nie udalo sie polaczyc z baza PostgreSQL");

    // 3. Ograniczony kanał w pamięci RAM (Bounded MPSC Channel - 50 000 eventów)
    let buffer_limit = 50_000;
    let (tx, mut rx) = mpsc::channel::<TransactionEvent>(buffer_limit);

    let state = Arc::new(AppState {
        sender: tx,
        received_count: AtomicU64::new(0),
        persisted_count: AtomicU64::new(0),
        dropped_count: AtomicU64::new(0),
        buffer_limit,
    });

    // 4. Worker w tle (Agregator paczek / Micro-Batcher)
    let worker_state = Arc::clone(&state);
    let worker_pool = pool.clone();
    tokio::spawn(async move {
        let mut batch: Vec<TransactionEvent> = Vec::with_capacity(500);
        let batch_size = 500;
        let flush_timeout = Duration::from_millis(100);
        let mut last_flush = Instant::now();

        loop {
            let recv_result = tokio::time::timeout(Duration::from_millis(20), rx.recv()).await;

            match recv_result {
                Ok(Some(event)) => batch.push(event),
                Ok(None) => break,
                Err(_) => {}
            }

            let should_flush_by_size = batch.len() >= batch_size;
            let should_flush_by_time = !batch.is_empty() && last_flush.elapsed() >= flush_timeout;

            if should_flush_by_size || should_flush_by_time {
                flush_batch_to_postgres(&worker_pool, &batch, &worker_state.persisted_count).await;
                batch.clear();
                last_flush = Instant::now();
            }
        }
    });

    // 5. Konfiguracja routingu HTTP w Axum i start serwera na porcie 8080
    let app = Router::new()
        .route("/events", post(ingest_event))
        .route("/stats", get(get_stats))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("Nie udalo sie zajac portu 8080");

    println!("Uruchamianie Rust Ingestion Engine (Axum + Tokio) na porcie 8080...");
    axum::serve(listener, app).await.unwrap();
}

async fn ingest_event(
    State(state): State<Arc<AppState>>,
    Json(event): Json<TransactionEvent>,
) -> impl IntoResponse {
    state.received_count.fetch_add(1, Ordering::Relaxed);

    match state.sender.try_send(event) {
        Ok(_) => (StatusCode::ACCEPTED, Json(serde_json::json!({"status": "queued"}))),
        Err(_) => {
            state.dropped_count.fetch_add(1, Ordering::Relaxed);
            (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({"status": "buffer_full"})),
            )
        }
    }
}

async fn get_stats(State(state): State<Arc<AppState>>) -> Json<EngineStats> {
    Json(EngineStats {
        engine: "Rust + Axum + Tokio",
        received_events: state.received_count.load(Ordering::Relaxed),
        persisted_events: state.persisted_count.load(Ordering::Relaxed),
        dropped_events: state.dropped_count.load(Ordering::Relaxed),
        buffer_limit: state.buffer_limit,
    })
}

// 6. Zbiorczy zapis paczki do PostgreSQL (Multi-row Bulk INSERT za pomocą QueryBuilder)
async fn flush_batch_to_postgres(
    pool: &PgPool,
    batch: &[TransactionEvent],
    persisted_count: &AtomicU64,
) {
    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO transaction_events (event_id, event_timestamp, user_id, amount, payment_method, status, ip_address) "
    );

    query_builder.push_values(batch, |mut b, event| {
        let parsed_ts = DateTime::parse_from_rfc3339(&event.timestamp)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        b.push_bind(&event.event_id)
            .push_bind(parsed_ts)
            .push_bind(&event.user_id)
            .push_bind(event.amount)
            .push_bind(&event.payment_method)
            .push_bind(&event.status)
            .push_bind(&event.ip_address);
    });

    query_builder.push(" ON CONFLICT (event_id) DO NOTHING");

    match query_builder.build().execute(pool).await {
        Ok(_) => {
            persisted_count.fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        Err(e) => {
            eprintln!("Blad zapisu paczki do PostgreSQL: {}", e);
        }
    }
}


/*
// Import external tools: Axum web framework, Chrono for dates, Serde for JSON, SQLx for PostgreSQL, standard thread-safe types, and Tokio async runtime

// 1. Define the incoming transaction data model (can be parsed from JSON and cloned)
STRUCT TransactionEvent {
    // event_id as text
    // timestamp as text
    // user_id as text
    // amount as decimal number
    // payment_method as text
    // status as text
    // ip_address as text
}

// Define the statistics response model (can be converted to JSON with custom camelCase field names)
STRUCT EngineStats {
    // engine name as static text
    // received_events as counter (serialized as "receivedEvents")
    // persisted_events as counter (serialized as "persistedEvents")
    // dropped_events as counter (serialized as "droppedEvents")
    // buffer_limit as number (serialized as "bufferLimit")
}

// Shared application state accessible safely across multiple concurrent threads
STRUCT AppState {
    // sender channel to push events into the memory queue
    // received_count as thread-safe atomic counter
    // persisted_count as thread-safe atomic counter
    // dropped_count as thread-safe atomic counter
    // buffer_limit as maximum queue capacity
}

// Main entry point of the asynchronous application
ASYNC FUNCTION main() {
    // 2. Set PostgreSQL connection string and create a connection pool with max 10 connections (stop program if connection fails)
    
    // 3. Set memory queue limit to 50,000 events and create a bounded sender (tx) and receiver (rx) channel
    
    // Initialize the shared AppState with the sender channel and zeroed counters

    // 4. Clone references to state and database pool, then spawn a background worker task (Micro-Batcher)
    SPAWN ASYNC BACKGROUND TASK {
        // Create an empty batch list with capacity for 500 events
        // Set max batch size to 500
        // Set max wait time (flush timeout) to 100 milliseconds
        // Record the current time as last_flush

        LOOP FOREVER {
            // Wait up to 20 milliseconds to receive a new event from the channel
            
            MATCH received_result {
                // If an event arrived -> add it to the current batch list
                // If the channel was closed -> break out of the loop and stop worker
                // If 20ms timeout passed with no event -> do nothing and check flush conditions
            }

            // Check if batch reached 500 items OR if batch is not empty and 100ms passed since last flush
            IF (should_flush_by_size OR should_flush_by_time) {
                // Call flush_batch_to_postgres() to save all batched events to the database
                // Empty the batch list
                // Reset last_flush timer to current time
            }
        }
    }

    // 5. Configure HTTP web routes: POST "/events" calls ingest_event, GET "/stats" calls get_stats, and attach shared state
    
    // Bind TCP network listener to port 8080 on all network interfaces (stop program if port is busy)
    
    // Print startup message to console and start serving HTTP requests forever
}

// HTTP handler for receiving a single transaction event via POST /events
ASYNC FUNCTION ingest_event(shared_state, json_event) {
    // Atomically increment received_count by 1

    // Try to push the event into the memory queue without waiting
    MATCH state.sender.try_send(event) {
        // If successful -> return HTTP 202 Accepted with JSON {"status": "queued"}
        // If queue is full -> {
            // Atomically increment dropped_count by 1
            // Return HTTP 429 Too Many Requests with JSON {"status": "buffer_full"}
        }
    }
}

// HTTP handler for returning current engine metrics via GET /stats
ASYNC FUNCTION get_stats(shared_state) {
    // Read all atomic counters and return them as a JSON EngineStats object
    RETURN Json(EngineStats {
        // engine: "Rust + Axum + Tokio"
        // received_events: current received_count
        // persisted_events: current persisted_count
        // dropped_events: current dropped_count
        // buffer_limit: configured buffer_limit
    })
}

// 6. Helper function to insert an entire batch of events into PostgreSQL in a single SQL query
ASYNC FUNCTION flush_batch_to_postgres(db_pool, event_batch, persisted_counter) {
    // Start building a bulk SQL INSERT query for table "transaction_events"

    // Loop through every event in the batch to append its values to the SQL query
    FOR EACH event IN batch {
        // Parse RFC3339 timestamp string into UTC DateTime (fallback to current UTC time if invalid)
        // Safely bind all 7 event fields as SQL parameters
    }

    // Append "ON CONFLICT (event_id) DO NOTHING" to ignore duplicate event IDs

    // Execute the bulk SQL query asynchronously on the database pool
    MATCH query_builder.build().execute(pool) {
        // If database insert succeeded -> atomically add the batch length to persisted_count
        // If database insert failed -> print error message to standard error console
    }
}
*/
