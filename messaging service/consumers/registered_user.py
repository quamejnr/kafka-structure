from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "registered_user",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="consumer-group-a",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


def send_email() -> None:
    for msg in consumer:
        user = msg.value
        username = user['name']
        print(f"Hi {username}. You are welcome to mPharma")

def send_slack_message() -> None:
    for msg in consumer:
        user = msg.value
        username = user["name"]
        print(f"Hi guys, Welcome {username} to the team.")
