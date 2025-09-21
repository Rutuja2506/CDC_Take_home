"""
Kafka CDC Producer
------------------
Reads CDC events from a JSONL file and sends them to a Kafka topic.

Usage:
    python src\producers/producer.py --bootstrap localhost:29092 \
                       --topic users_cdc \
                       --file data/sample_events.jsonl \
                       --sleep 0.5
"""
import argparse, json, time, sys
from kafka import KafkaProducer

def valid_json_lines(path):
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
                yield evt
            except Exception as e:
                print(f"Skipping line {i}: {e}", file=sys.stderr)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap (host view)")
    ap.add_argument("--topic", default="users_cdc", help="Kafka topic")
    ap.add_argument("--file", default="data/sample_events.jsonl", help="JSONL file with CDC events")
    ap.add_argument("--sleep", type=float, default=0.5, help="sleep seconds between messages")
    args = ap.parse_args()

    prod = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks="all",
    )

    count = 0
    for evt in valid_json_lines(args.file):
        prod.send(args.topic, value=evt)
        prod.flush()
        count += 1
        print("Sent:", evt)
        time.sleep(args.sleep)

    print(f"Done. Sent {count} messages.")

if __name__ == "__main__":
    main()


