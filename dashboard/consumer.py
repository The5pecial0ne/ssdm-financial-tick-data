from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "ema-results",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000
)

print("Connected. Waiting for messages...")

count = 0

for msg in consumer:
    count += 1
    print(msg.value.decode("utf-8"))

print(f"Finished. Messages read: {count}")