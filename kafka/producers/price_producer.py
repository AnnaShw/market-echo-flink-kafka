import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import websocket
import json
from kafka import KafkaProducer
from config.settings import FINNHUB_API_KEY, SYMBOLS

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
)

def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "trade":
        for trade in data["data"]:
            producer.send(
                topic="price_ticks",
                key=trade["s"],
                value=trade
            )

def on_open(ws):
    print(f"Connected to Finnhub. Subscribing to {SYMBOLS}...")
    for symbol in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
    print("Subscribed. Waiting for trades...")

def on_error(_ws, error):
    print(f"WebSocket error: {error}")

def on_close(_ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} {close_msg}")
    producer.flush()

ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}",
    on_message=on_message,
    on_open=on_open,
    on_error=on_error,
    on_close=on_close,
)
ws.run_forever(reconnect=5)