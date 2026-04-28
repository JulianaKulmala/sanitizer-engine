import csv
import json
import getpass
from datetime import datetime, timezone
from kafka import KafkaProducer

CSV_FILE = "input.csv"
KAFKA_BROKER = "localhost:9092"
TOPIC = "sanitizer_in"

def read_csv(csv_file):
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

def build_json(csv_file):
    return {
        "metadata": {
            "user": getpass.getuser(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "job_request",
            "filename": csv_file
        },
        "payload": read_csv(csv_file)
    }

def send_to_kafka(data):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(TOPIC, data)
    producer.flush()
    print("Sent to Kafka topic:", TOPIC)

if __name__ == "__main__":
    message = build_json(CSV_FILE)
    print(json.dumps(message, indent=2))
    send_to_kafka(message)
