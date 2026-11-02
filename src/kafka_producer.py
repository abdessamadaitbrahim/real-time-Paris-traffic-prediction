"""Replays data/data.json into a Kafka topic, one message per second."""

import json
import time
from pathlib import Path

from kafka import KafkaProducer

KAFKA_TOPIC = "trafic_paris"
KAFKA_BROKER = "localhost:9092"
DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "data.json"
DELAY_SECONDS = 1  # simulates a real-time feed

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

with open(DATA_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

# Accept both a JSON array and a single JSON object
if isinstance(data, dict):
    data = [data]

for record in data:
    producer.send(KAFKA_TOPIC, value=record)
    print(f"[OK] Message sent: {record}")
    time.sleep(DELAY_SECONDS)

producer.flush(
print("[DONE] All messages have been sent.")
