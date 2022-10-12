from kafka import KafkaProducer
import json
from typing import Dict, Any

producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )


def publish_to_registered_user(data: Dict[Any, Any]) -> None:
    TOPIC = 'registered_user'
    producer.send(TOPIC, data)

def publish_to_sale_completed(data: Dict[Any, Any]) -> None:
    TOPIC = 'sale_completed'
    producer.send(TOPIC, data)
