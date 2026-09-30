import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import time
import requests
from kafka import KafkaProducer
import json
from datetime import datetime
from config.settings import FINNHUB_API_KEY, SYMBOLS
from config.market_calendar import MARKET_TZ, reference_date

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
    ref = reference_date()
    from_date = ref.strftime("%Y-%m-%d")
    to_date   = ref.strftime("%Y-%m-%d")
    response = requests.get(
        "https://finnhub.io/api/v1/company-news",
        params={"symbol": symbol, "from": from_date, "to": to_date, "token": FINNHUB_API_KEY},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()

def poll():
    print(f"Polling Finnhub news for {SYMBOLS} every {POLL_INTERVAL}s...")
    while True:
        ref = reference_date()
        batch = []
        for symbol in SYMBOLS:
            try:
                articles = fetch_news(symbol)
                for article in articles:
                    article_id = str(article.get("id"))
                    if article_id in seen_ids:
                        continue
                    seen_ids.add(article_id)
                    batch.append({
                        "id":       article_id,
                        "symbol":   symbol,
                        "headline": article.get("headline", ""),
                        "summary":  article.get("summary", ""),
                        "datetime": article.get("datetime"),
                    })
            except Exception as e:
                print(f"Error fetching news for {symbol}: {e}")
        # Finnhub returns newest first; send oldest first so the Flink join's
        # event-time watermark doesn't mark most of the batch as late
        batch.sort(key=lambda a: a["datetime"] or 0)
        for article in batch:
            news_producer.send(topic="news_raw", key=article["symbol"], value=article)
        news_producer.flush()
        print(f"[{datetime.now(MARKET_TZ):%H:%M:%S}] ref date {ref}: {len(batch)} new articles sent.")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    poll()
