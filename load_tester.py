import asyncio
import random
import time
import uuid
from datetime import datetime, timezone
import httpx
from faker import Faker

fake = Faker()
PAYMENT_METHODS = ["ALLEGRO_PAY_LATER", "ALLEGRO_PAY_INSTALLMENTS", "BLIK", "CARD"]

TARGET_URL = "http://localhost:8080/events"
STATS_URL = "http://localhost:8080/stats"

CONCURRENT_WORKERS = 50
TEST_DURATION_SEC = 10

def generate_event() -> dict:
    is_fraud = random.random() < 0.05
    amount = round(random.uniform(5000.0, 15000.0), 2) if is_fraud else round(random.uniform(10.0, 1200.0), 2)
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"USR-{random.randint(10000, 99999)}",
        "amount": amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": "SUSPICIOUS" if is_fraud else "SUCCESS",
        "ip_address": fake.ipv4()
    }

async def worker(client: httpx.AsyncClient, end_time: float, latencies: list, results: dict):
    while time.perf_counter() < end_time:
        payload = generate_event()
        t0 = time.perf_counter()
        try:
            resp = await client.post(TARGET_URL, json=payload)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if resp.status_code == 202:
                results["accepted"] += 1
                latencies.append(elapsed_ms)
            elif resp.status_code == 429:
                results["rate_limited"] += 1
            else:
                results["errors"] += 1
        except Exception:
            results["errors"] += 1

async def run_benchmark():
    print(f"Start testu obciazeniowego ({CONCURRENT_WORKERS} workerow przez {TEST_DURATION_SEC}s) -> {TARGET_URL}")
    latencies = []
    results = {"accepted": 0, "rate_limited": 0, "errors": 0}

    limits = httpx.Limits(max_keepalive_connections=CONCURRENT_WORKERS, max_connections=CONCURRENT_WORKERS + 10)
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client:
        start_time = time.perf_counter()
        end_time = start_time + TEST_DURATION_SEC

        tasks = [
            asyncio.create_task(worker(client, end_time, latencies, results))
            for _ in range(CONCURRENT_WORKERS)
        ]
        await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_time

        await asyncio.sleep(0.3)
        stats_resp = await client.get(STATS_URL)
        server_stats = stats_resp.json()

    latencies.sort()
    total_ok = results["accepted"]
    rps = total_ok / total_time if total_time > 0 else 0
    avg_lat = sum(latencies) / len(latencies) if latencies else 0
    p95_lat = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99_lat = latencies[int(len(latencies) * 0.99)] if latencies else 0

    print("\n" + "=" * 55)
    print(f"WYNIKI BENCHMARKU: {server_stats.get('engine')}")
    print("=" * 55)
    print(f"Przyjete zdarzenia (202):  {total_ok}")
    print(f"Odrzucone (429 Backpress): {results['rate_limited']}")
    print(f"Bledy sieciowe:            {results['errors']}")
    print(f"Przepustowosc (RPS):       {rps:,.2f} req/s")
    print(f"Srednie opoznienie (Avg):  {avg_lat:.2f} ms")
    print(f"Opoznienie p95:            {p95_lat:.2f} ms")
    print(f"Opoznienie p99:            {p99_lat:.2f} ms")
    print("-" * 55)
    print(f"Zapisane w PostgreSQL:     {server_stats.get('persistedEvents')} rekordow")
    print("=" * 55 + "\n")

if __name__ == "__main__":
    asyncio.run(run_benchmark())