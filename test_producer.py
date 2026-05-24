import json, os
from confluent_kafka import Producer

conf = {"bootstrap.servers": "localhost:9092"}
print("Config passed:", conf)
print("KAFKA-related env vars:", {k:v for k,v in os.environ.items() if "KAFKA" in k})
producer = Producer(conf)
print("Producer created, sending test message...")
try:
    producer.produce("transactions", value=json.dumps({"test":1}).encode("utf-8"))
    producer.flush(timeout=5)
    print("Message sent successfully!")
except Exception as e:
    print("Error:", e)
