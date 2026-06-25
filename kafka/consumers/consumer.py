from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "price_ticks",
    bootstrap_servers="localhost:9092",
    group_id="flink-app",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8")
)

for message in consumer:
    print(f"Тикер: {message.key}")
    print(f"Цена: {message.value['p']}")
    print(f"Offset: {message.offset}")