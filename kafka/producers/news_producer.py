import time
import requests
from kafka import KafkaProducer
import json
from config.settings import FINNHUB_API_KEY, SYMBOLS

news_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
)

POLL_INTERVAL = 60
seen_ids = set()

def fetch_news(symbol):
    response = requests.get(
        "https://finnhub.io/api/v1/company-news",
        params={"symbol": symbol, "from": "2026-01-01", "to": "2099-01-01", "token": FINNHUB_API_KEY},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()

def poll():
    while True:
        for symbol in SYMBOLS:
            try:
                articles = fetch_news(symbol)
                for article in articles:
                    article_id = str(article.get("id"))
                    if article_id in seen_ids:
                        continue
                    seen_ids.add(article_id)
                    news_producer.send(
                        topic="news_raw",
                        key=symbol,
                        value={
                            "id":       article_id,
                            "symbol":   symbol,
                            "headline": article.get("headline", ""),
                            "summary":  article.get("summary", ""),
                            "datetime": article.get("datetime"),
                        },
                    )
            except Exception as e:
                print(f"Error fetching news for {symbol}: {e}")
        news_producer.flush()
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    poll()
