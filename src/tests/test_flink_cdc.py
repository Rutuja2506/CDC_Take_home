# src/tests/test_flink_cdc.py
import os
import time
import json
import psycopg2
# import pytest
from kafka import KafkaProducer, KafkaConsumer

# --- CONFIG ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = "users_cdc"
DLQ_TOPIC = "users_cdc_dlq"

PG_CONN = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", 5432)),
    dbname=os.getenv("PG_DB", "postgres"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASS", "postgres"),
)


def publish_events(events):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for e in events:
        producer.send(TOPIC, e)
        print("Published to Kafka:", e)
    producer.flush()

def wait_for_flink():
    print("Waiting for Flink to process...")
    time.sleep(10)  # adjust as needed for job checkpoint interval

def get_pg_table():
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email, op FROM migrated_users ORDER BY id")
            return cur.fetchall()

def get_dlq_messages(max_wait=5):
    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f"dlq-test-{int(time.time())}"  # fresh group each run
    )
    msgs = []
    start = time.time()
    while time.time() - start < max_wait:
        polled = consumer.poll(timeout_ms=500)
        for records in polled.values():
            for rec in records:
                msgs.append(rec.value)
    consumer.close()
    return msgs

# --- TEST CASE 1: Simple Insert ---
publish_events([
    {"op": "c", "before": None, "after": {"id": 1, "name": "Alice", "email": "Alice@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:00:05Z"}
])
wait_for_flink()
print(get_pg_table())

# --- TEST CASE 2: Update with Higher Event Time ---
publish_events([
    {"op": "c", "before": None, "after": {"id": 2, "name": "Bob", "email": "bob@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:00:10Z"},
    {"op": "u", "before": {"id": 2, "name": "Bob", "email": "bob@EXAMPLE.COM"}, "after": {"id": 2, "name": "Robert", "email": "robert@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:00:20Z"}
])
wait_for_flink()
print(get_pg_table())

# --- TEST CASE 3: Delete ---
publish_events([
    {"op": "c", "before": None, "after": {"id": 3, "name": "Carl", "email": "carl@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:00:30Z"},
    {"op": "d", "before": {"id": 3, "name": "Carl", "email": "carl@EXAMPLE.COM"}, "after": None, "timestamp": "2025-09-21T10:00:40Z"}
])
wait_for_flink()
print(get_pg_table())

# --- TEST CASE 4: Out-of-Order/Old Event ---
publish_events([
    {"op": "u", "before": {"id": 4, "name": "Dana", "email": "dana@EXAMPLE.COM"}, "after": {"id": 4, "name": "Daisy", "email": "daisy@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:00:50Z"},
    {"op": "u", "before": {"id": 4, "name": "Dana", "email": "dana@EXAMPLE.COM"}, "after": {"id": 4, "name": "Dani", "email": "dani@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:00:45Z"}
])
wait_for_flink()
print(get_pg_table())

# --- TEST CASE 5: Late Event (DLQ) ---
# Newer event for id=5
publish_events([
    {"op": "c", "before": None, "after": {"id": 5, "name": "EvaNew", "email": "eva_new@EXAMPLE.COM"},
     "timestamp": "2025-09-21T10:10:00Z"}
])
wait_for_flink()

# Older event for id=5 (late)
publish_events([
    {"op": "c", "before": None, "after": {"id": 5, "name": "Eva", "email": "eva@EXAMPLE.COM"},
     "timestamp": "2025-09-21T09:50:00Z"}
])
wait_for_flink()

print("DLQ messages:", get_dlq_messages())

# --- TEST CASE 6: Multiple Inserts-Updates-Deletes ---
publish_events([
    {"op": "c", "before": None, "after": {"id": 6, "name": "Greg", "email": "greg@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:01:00Z"}, 
    {"op": "u", "before": {"id": 6, "name": "Greg", "email": "greg@EXAMPLE.COM"}, "after": {"id": 6, "name": "Gregory", "email": "gregory@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:01:05Z"},
    {"op": "u", "before": {"id": 6, "name": "Gregory", "email": "gregory@EXAMPLE.COM"}, "after": {"id": 6, "name": "Greg G.", "email": "gregg@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:01:10Z"},
    {"op": "d", "before": {"id": 6, "name": "Greg G.", "email": "gregg@EXAMPLE.COM"}, "after": None, "timestamp": "2025-09-21T10:01:15Z"},
    {"op": "u", "before": {"id": 6, "name": "Gregory", "email": "gregory@EXAMPLE.COM"}, "after": {"id": 6, "name": "Greg XX", "email": "gregxx@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:01:09Z"}
])
wait_for_flink()
print(get_pg_table())

# --- TEST CASE 7: Email Lowercase Handling ---
publish_events([
    {"op": "c", "before": None, "after": {"id": 7, "name": "Frank", "email": "FRANK@EXAMPLE.COM"}, "timestamp": "2025-09-21T10:02:00Z"}
])
wait_for_flink()
print(get_pg_table())