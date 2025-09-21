# src/flink_job/basic_cdc.py
import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def require_env(name: str) -> str:
    """Require an environment variable, fail if missing."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
# Strict env vars (injected by Docker)
BOOTSTRAP = require_env("KAFKA_BOOTSTRAP")
TOPIC     = require_env("KAFKA_TOPIC")
PG_URL    = require_env("PG_URL")
PG_USER   = require_env("PG_USER")
PG_PASS   = require_env("PG_PASS")


def main():
    # --- Runtime & reliability ---
    settings = EnvironmentSettings.in_streaming_mode()
    t = TableEnvironment.create(settings)
    conf = t.get_config().get_configuration()
    conf.set_string("execution.checkpointing.interval", "10s")
    conf.set_string("parallelism.default", "1")
    conf.set_string("restart-strategy", "fixed-delay")
    conf.set_string("restart-strategy.fixed-delay.attempts", "5")
    conf.set_string("restart-strategy.fixed-delay.delay", "5 s")

    # ---- SOURCE: Kafka (CDC JSON) with watermark ----
    t.execute_sql(f"""
        CREATE TABLE src_cdc (
          op STRING,
          before ROW<id INT, name STRING, email STRING>,
          after  ROW<id INT, name STRING, email STRING>,
          `timestamp` STRING,
          event_time AS CAST(REPLACE(REPLACE(`timestamp`,'T',' '),'Z','') AS TIMESTAMP(3)),
          WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{TOPIC}',
          'properties.bootstrap.servers' = '{BOOTSTRAP}',
          'properties.group.id' = 'flink-cdc-sql-v2',
          'scan.startup.mode' = 'earliest-offset',
          'format' = 'json',
          'json.fail-on-missing-field' = 'false',
          'json.ignore-parse-errors' = 'true'
        )
    """)

    # ---- DEBUG SINK: 10s event-time window counts (to verify watermarking) ----
    t.execute_sql("""
        CREATE TABLE sink_counts (
          window_start TIMESTAMP(3),
          window_end   TIMESTAMP(3),
          cnt          BIGINT
        ) WITH ('connector' = 'print')
    """)
    t.execute_sql("""
        INSERT INTO sink_counts
        SELECT
          window_start,
          window_end,
          COUNT(*) AS cnt
        FROM TABLE(
          TUMBLE(TABLE src_cdc, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)
        )
        GROUP BY window_start, window_end
    """)

    # ---- TARGET: Postgres upsert table ----
    t.execute_sql(f"""
        CREATE TABLE sink_pg (
          id INT,
          name STRING,
          email STRING,
          op STRING,
          event_time TIMESTAMP(3),
          processed_at TIMESTAMP(3),
          PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
          'connector' = 'jdbc',
          'url' = '{PG_URL}',
          'table-name' = 'migrated_users',
          'username' = '{PG_USER}',
          'password' = '{PG_PASS}',
          'sink.buffer-flush.max-rows' = '1',
          'sink.buffer-flush.interval' = '1s'
        )
    """)

    # # --- OPTIONAL: Kafka DLQ for late events ---
    t.execute_sql(f"""
    CREATE TABLE sink_dlq (
      id         INT,
      op         STRING,
      `before`   ROW<id INT, name STRING, email STRING>,
      `after`    ROW<id INT, name STRING, email STRING>,
      `timestamp` STRING,
      event_time TIMESTAMP(3)
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'users_cdc_dlq',
      'properties.bootstrap.servers' = '{BOOTSTRAP}',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true',
      'json.timestamp-format.standard' = 'ISO-8601'
    )
""")

    # Helper: ISO-8601 -> TIMESTAMP(3)
    cast_event_time = "CAST(REPLACE(REPLACE(`timestamp`,'T',' '),'Z','') AS TIMESTAMP(3))"

    # ---- Unified on-time stream (INSERT/UPDATE + DELETE) ----
    t.execute_sql(f"""
        CREATE VIEW v_on_time AS
        SELECT
          after.id                                          AS id,
          after.name                                        AS name,
          LOWER(after.email)                                AS email,
          CASE op WHEN 'c' THEN 'INSERT' WHEN 'u' THEN 'UPDATE' END AS op,
          {cast_event_time}                                 AS event_time,
          CURRENT_TIMESTAMP                                 AS processed_at
        FROM src_cdc
        WHERE op IN ('c','u') AND event_time >= CURRENT_WATERMARK(event_time)
        UNION ALL
        SELECT
          before.id                                         AS id,
          CAST(NULL AS STRING)                              AS name,
          CAST(NULL AS STRING)                              AS email,
          'DELETE'                                          AS op,
          {cast_event_time}                                 AS event_time,
          CURRENT_TIMESTAMP                                 AS processed_at
        FROM src_cdc
        WHERE op = 'd' AND event_time >= CURRENT_WATERMARK(event_time)
    """)

    # ---- Per-id Top-N (dedup): keep only the newest event_time (last-write-wins) ----
    t.execute_sql("""
        CREATE VIEW v_latest AS
        SELECT id, name, email, op, event_time, processed_at
        FROM (
          SELECT
            id, name, email, op, event_time, processed_at,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_time DESC) AS rn
          FROM v_on_time
        )
        WHERE rn = 1
    """)

    # ---- Single upsert into Postgres from the deduplicated view ----
    ss = t.create_statement_set()
    ss.add_insert_sql("""
        INSERT INTO sink_pg
        SELECT id, name, email, op, event_time, processed_at
        FROM v_latest
    """)
    
    # Late events -> DLQ 
    ss.add_insert_sql("""
        INSERT INTO sink_dlq
        SELECT
        COALESCE(after.id, before.id) AS id,
        op,
        `before`,
        `after`,
        `timestamp`,
        event_time
        FROM src_cdc
        WHERE event_time < CURRENT_WATERMARK(event_time)
    """)


    # Submit as one job
    ss.execute()

if __name__ == "__main__":
    main()
