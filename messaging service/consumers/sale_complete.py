from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "sale_completed",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="consumer-group-a",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

def create_order() -> None:
    for msg in consumer:
        sale = msg.value
        sale_id = sale['id']
        print(f"Creating order...{sale_id}")

def send_email() -> None:
    for msg in consumer:
        sale = msg.value
        username = sale["username"]
        sale_id = sale['id']
        print(f"Hi {username}. Your order {sale_id} has been received...")