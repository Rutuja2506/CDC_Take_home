# CDC Implementation Project with Flink, Kafka, and Postgres

This project demonstrates a simple **Change Data Capture (CDC) pipeline** using:
- **Kafka** for event streaming  
- **Flink** for processing (Table API + watermarks + checkpointing)  
- **Postgres** as the sink (via JDBC)  
- **Python** producer for sending CDC events from JSONL files  

---
## Architectural Diagram

Below is the architectural diagram illustrating the data flow and components:

![Architectural Diagram](/images/CDC_event_streaming.drawio.png)

# Tech Stack

## Components

| Component   | Technology                                                                                 |
|-------------|--------------------------------------------------------------------------------------------|
| Source      | Simulated table emitting CDC events (`producer_from_json.py` → `data/sample_events.jsonl`) |
| Stream      | Kafka topic                                                                               |
| Processor   | Apache Flink                                                                              |
| Sink        | Postgres table `migrated_users` (upsert/soft-delete)                                      |
| DLQ         | Kafka DLQ `users_cdc_dlq` for late events                                                 |
| Language    | Python                                                                                    |
| Environment | Docker (setup enabled)                                                                    |



## 1. Installation Steps

1. Install **Python 3.9+** (needed for the producer script).  
2. Install **Docker Desktop** (includes Docker Compose).  
3. Install **DBeaver** (or any SQL client for Postgres).  
4. *(Optional)* Install Python dependencies locally:
   ```
   pip install -r requirements.txt
   ```
## 2. How to Run This Project

### 2.1 Start Docker services
``` 
docker compose up -d
```
Verify all the containers are up and and running 
```
docker ps
```
### 2.2 Submit the Flink Job 
```
docker exec -it jobmanager flink run -py /opt/src/flink_job/basic_cdc.py
```
Then confirm on Flink UI at http://localhost:8081/
You should see 2 jobs job running:
1. sinf_pg,(upsert/deletes into sink), sink_dlq (late arriving events)
2. sink_counts (windowed aggregartion)

### 2.3 Run the producer to send CDC events from data/sample_events.jsonl
```
python src/producers/produce_from_json.py
```
or 
```
python src/producers/produce_from_json.py \
  --bootstrap localhost:29092 \
  --topic users_cdc \
  --file data/sample_events.jsonl \
  --sleep 0.2
```
This will stream CDC JSONL events to Kafka.

### 2.4 Verify in Flink UI

Visit http://localhost:8081/
 and confirm the jobs are RUNNING.

### 2.5 Query results in Postgres (via DBeaver or psql)

Connection details:
- **Host:** localhost  
- **Port:** 15432  
- **Database:** postgres  
- **User:** postgres  
- **Password:** postgres

Ensure the target table exists:
```
CREATE TABLE IF NOT EXISTS migrated_users (
  id           INT PRIMARY KEY,
  name         TEXT,
  email        TEXT,
  op           TEXT,
  event_time   TIMESTAMP,
  processed_at TIMESTAMP DEFAULT NOW()
);
```
Query results:
```
SELECT id, name, email, op, event_time, processed_at
FROM migrated_users
ORDER BY id;
```

## 3. Project Structure
```
cdc_implementation_project/
├── docker-compose.yml                  # Spins up Flink, Kafka, ZK, Postgres
├── data/
│   └── sample_events.jsonl             # CDC events (create/update/delete)
├── src/
│   ├── flink_job/
│   │   └── basic_cdc.py                # Flink Table API job
│   ├── producers/
│   │   └── produce_from_json.py        # Python Kafka producer (JSONL → Kafka)
│   └── schemas/
│       └── target.sql                  # Postgres DDL for target table
├── requirements.txt                    # Python deps (e.g., kafka-python)
└── README.md
```
## 4. Sample CDC Events 
data/sample_events.jsonl
```
{"op":"c","before":null,"after":{"id":1,"name":"Alice","email":"alice@example.com"},"timestamp":"2025-09-19T10:00:00Z"}
{"op":"u","before":{"id":1,"name":"Alice","email":"alice@example.com"},"after":{"id":1,"name":"Alice B","email":"aliceb@example.com"},"timestamp":"2025-09-19T10:00:15Z"}
{"op":"c","before":null,"after":{"id":2,"name":"Bob","email":"bob@example.com"},"timestamp":"2025-09-19T10:02:00Z"}
{"op":"d","before":{"id":2,"name":"Bob","email":"bob@example.com"},"after":null,"timestamp":"2025-09-19T10:03:00Z"}
{"op":"u","before":{"id":1,"name":"Alice B","email":"aliceb@example.com"},"after":{"id":1,"name":"Alice C","email":"alicec@example.com"},"timestamp":"2025-09-19T10:04:00Z"}
```

## 5. Unit Testing
Run the following command and verify results in Postgres
```
python src/tests/test_flink_cdc.py 
```
### 6. Architectural Decisions & Trade-offs (Summary)

#### 1. Why Flink SQL?
- **Decision:** Used Flink SQL/Table API instead of low-level DataStream API.  
- **Reason:** Declarative SQL is concise, portable, and aligns well with CDC-style relational transformations (before/after rows, deduplication, watermarking).  
- **Trade-off:** Easier to read and maintain but less flexible than custom operators in the DataStream API. If complex stateful enrichment is needed, production systems may need to combine both approaches.

#### 2. Sink Design & Exactly-once Guarantees
- **Why:** JDBC upsert sink to Postgres with a primary key ensures idempotency (last-write-wins). Flink checkpointing runs every 10 seconds to provide exactly-once guarantees.  
- **Trade-off:** JDBC sinks work for demos and moderate loads but do not scale well under high throughput. In production, an upsert-Kafka sink or CDC-aware database connector would be preferable.

#### 3. Handling Out-of-order Events
- **Why:** Added a watermark (`event_time - 5s`) to tolerate small event delays while maintaining event-time correctness. Late events are routed to a Kafka DLQ.  
- **Trade-off:** A 5-second watermark balances latency and correctness. Events delayed beyond 5 seconds are dropped into DLQ. In production, the interval should be tuned based on upstream latency profiles.

#### 4. Enrichment & State Management

- **Lowercasing emails**  
  - **Why:** Basic normalization to reduce duplicates and issues during joins.  
  - **Trade-off:** Case sensitivity rules vary across systems; ensure business requirements accept this (e.g., technically the local part of an email is case-sensitive).

- **Casting ISO-8601 string to TIMESTAMP**  
  - **Why:** Produces a consistent `event_time` column regardless of source driver specifics.  
  - **Trade-off:** Assumes UTC "Z" timezone semantics. If sources send mixed timezones or omit the "Z", parsing might drift. Consider using a robust timestamp UDF or schema-aware formats.

- **Postgres consistency via SQL views**  
  - Utilizes two views to manage state and enrichment:  
    - `v_on_time` normalizes CDC operations into INSERT, UPDATE, and DELETE.  
    - `v_latest` deduplicates records applying a *last-write-wins* policy using `ROW_NUMBER()` partitioned by ID.  
  - **Trade-off:** This design ensures the target table always reflects the latest state but does not preserve historical changes. To retain auditability, an append-only history sink would be required.


#### 5. Error Handling & Reliability
- **Why:**  
  - Fixed-delay restart strategy (5 retries, 5s delay).  
  - JSON parser skips invalid messages (`ignore-parse-errors = true`).  
  - Dead Letter Queue sink for malformed or late events.  
- **Trade-off:** Skipping bad records improves availability but may lose data. In production, DLQ monitoring and alerting should be added.

#### 6. Schema Evolution
Kafka JSON connector with `ignore-parse-errors`, `fail-on-missing-field=false` enabled permits forward compatibility, tolerating added/unknown fields.
- **Why:** 
Kept schema static (`id, name, email, op, event_time`) for simplicity in this demo.  
- **Trade-off:** Real systems require schema evolution handling. Without a schema registry, manual intervention is required. In production, Debezium’s Avro/Protobuf + Schema Registry or Flink’s schema evolution features would be integrated.

#### 7. Monitoring:
Print sink for debug visibility in JobManager logs.
Flink Web UI (http://localhost:8081) shows task status; for  production setups can connect to Prometheus/Grafana.  
