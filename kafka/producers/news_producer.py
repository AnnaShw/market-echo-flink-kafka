import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import time
import requests
from kafka import KafkaProducer
import json
from datetime import date, datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
import holidays
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

MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = dtime(9, 30)
NYSE_HOLIDAYS = holidays.financial_holidays("NYSE")

def _is_trading_day(d):
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS

def _previous_trading_day(d):
    prev = d - timedelta(days=1)
    while not _is_trading_day(prev):
        prev -= timedelta(days=1)
    return prev

def reference_date():
    now = datetime.now(MARKET_TZ)
    today = now.date()
    # Weekend, or trading hasn't started yet today — use the last full trading day
    if not _is_trading_day(today) or now.time() < MARKET_OPEN:
        return _previous_trading_day(today)
    return today

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
        new_count = 0
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
                    new_count += 1
            except Exception as e:
                print(f"Error fetching news for {symbol}: {e}")
        news_producer.flush()
        print(f"[{datetime.now(MARKET_TZ):%H:%M:%S}] ref date {ref}: {new_count} new articles sent.")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    poll()
