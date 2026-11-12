package com.synerise.ingestion

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

// 1. Model danych dopasowany 1:1 do Twojego generatora ruchu w Pythonie
@Serializable
data class TransactionEvent(
    @SerialName("event_id") val eventId: String,
    val timestamp: String,
    @SerialName("user_id") val userId: String,
    val amount: Double,
    @SerialName("payment_method") val paymentMethod: String,
    val status: String,
    @SerialName("ip_address") val ipAddress: String
)

// Statystyki do podglądu wydajności na żywo
@Serializable
data class EngineStats(
    val engine: String = "Kotlin + Ktor + Coroutines",
    val receivedEvents: Long,
    val persistedEvents: Long,
    val droppedEvents: Long,
    val bufferLimit: Int
)

fun main() {
    // 2. Konfiguracja szybkiej puli połączeń HikariCP do naszej bazy PostgreSQL
    val hikariConfig = HikariConfig().apply {
        jdbcUrl = "jdbc:postgresql://localhost:5432/events_db"
        username = "synerise_user"
        password = "secret_password"
        maximumPoolSize = 10
        isAutoCommit = false // Wyłączamy auto-commit, bo będziemy robić zapisy paczkami (Batch)!
    }
    val dataSource = HikariDataSource(hikariConfig)

    // 3. Bezpieczny, ograniczony bufor w pamięci RAM (chroni przed OutOfMemoryError!)
    val bufferCapacity = 50_000
    val eventChannel = Channel<TransactionEvent>(capacity = bufferCapacity)

    // Liczniki atomowe (bezpieczne przy tysiącach równoczesnych wątków/korutyn)
    val receivedCount = AtomicLong(0)
    val persistedCount = AtomicLong(0)
    val droppedCount = AtomicLong(0)

    // 4. Worker w tle (Agregator paczek / Micro-Batcher)
    val workerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    workerScope.launch {
        val batch = ArrayList<TransactionEvent>(500)
        val batchSize = 500
        val flushTimeoutMs = 100L
        var lastFlushTime = System.currentTimeMillis()

        while (isActive) {
            // Czekamy maksymalnie 20ms na kolejny event z kanału, żeby móc sprawdzać timeout
            val event = withTimeoutOrNull(20L) {
                eventChannel.receive()
            }

            if (event != null) {
                batch.add(event)
            }

            val now = System.currentTimeMillis()
            val shouldFlushBySize = batch.size >= batchSize
            val shouldFlushByTime = batch.isNotEmpty() && (now - lastFlushTime >= flushTimeoutMs)

            if (shouldFlushBySize || shouldFlushByTime) {
                flushBatchToPostgres(dataSource, batch, persistedCount)
                batch.clear()
                lastFlushTime = now
            }
        }
    }

    // 5. Uruchomienie ultraszybkiego serwera HTTP Netty na porcie 8080
    println("🚀 Uruchamianie Kotlin Ingestion Engine na porcie 8080...")
    embeddedServer(Netty, port = 8080) {
        install(ContentNegotiation) {
            json()
        }

        routing {
            // Endpoint główny przyjmujący strumień zdarzeń z Pythona
            post("/events") {
                val event = call.receive<TransactionEvent>()
                receivedCount.incrementAndGet()

                // Nieblokująca próba wrzucenia do kolejki w RAM (trySend zamiast send!)
                val result = eventChannel.trySend(event)
                if (result.isSuccess) {
                    call.respond(HttpStatusCode.Accepted, mapOf("status" to "queued"))
                } else {
                    // Backpressure: bufor pełny -> zwracamy kod 429 zamiast zawiesić serwer
                    droppedCount.incrementAndGet()
                    call.respond(HttpStatusCode.TooManyRequests, mapOf("status" to "buffer_full"))
                }
            }

            // Endpoint do sprawdzania metryk wydajności na żywo w przeglądarce
            get("/stats") {
                call.respond(
                    EngineStats(
                        receivedEvents = receivedCount.get(),
                        persistedEvents = persistedCount.get(),
                        droppedEvents = droppedCount.get(),
                        bufferLimit = bufferCapacity
                    )
                )
            }
        }
    }.start(wait = true)
}

// 6. Funkcja wykonująca zbiorczy zapis paczki (JDBC Batch Insert)
private fun flushBatchToPostgres(
    dataSource: HikariDataSource,
    batch: List<TransactionEvent>,
    persistedCount: AtomicLong
) {
    val sql = """
        INSERT INTO transaction_events 
        (event_id, event_timestamp, user_id, amount, payment_method, status, ip_address)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (event_id) DO NOTHING
    """.trimIndent()

    try {
        dataSource.connection.use { conn ->
            conn.prepareStatement(sql).use { stmt ->
                for (event in batch) {
                    stmt.setString(1, event.eventId)
                    stmt.setTimestamp(2, Timestamp.from(Instant.parse(event.timestamp)))
                    stmt.setString(3, event.userId)
                    stmt.setDouble(4, event.amount)
                    stmt.setString(5, event.paymentMethod)
                    stmt.setString(6, event.status)
                    stmt.setString(7, event.ipAddress)
                    stmt.addBatch() // Dodajemy rekord do paczki zamiast wysyłać od razu!
                }
                stmt.executeBatch() // Wysyłamy wszystkie 500 rekordów jednym strzałem sieciowym!
                conn.commit()
                persistedCount.addAndGet(batch.size.toLong())
            }
        }
    } catch (e: Exception) {
        println("❌ Błąd zapisu paczki do PostgreSQL: ${e.message}")
    }
}