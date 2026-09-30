import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import websocket
import json
import threading
import time
import requests
from datetime import datetime, time as dtime
from kafka import KafkaProducer
from config.settings import FINNHUB_API_KEY, SYMBOLS
from config.market_calendar import (
    MARKET_TZ, is_market_open, reference_date, seconds_until_close, day_bounds,
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
)

CLOSED_POLL_INTERVAL = 60
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}  # Yahoo rejects requests without one


# ── Live mode: market open ─────────────────────────────────────────────────

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

def run_live():
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}",
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close,
    )
    # Drop the socket at the closing bell so the main loop switches to backfill mode
    timer = threading.Timer(seconds_until_close(), ws.close)
    timer.daemon = True
    timer.start()
    ws.run_forever(reconnect=5)
    timer.cancel()


# ── Closed mode: backfill the reference trading day with 1-minute bars ─────
# Finnhub's free tier has no historical candles (403), so bars come from
# free keyless sources. Each bar is emitted in the same {s, p, v, t} shape
# as a websocket trade (p = bar close, t = bar open in epoch ms), so Flink
# and ClickHouse treat it like any other tick.

def _get_json(url, params, attempts=3):
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, headers=HTTP_HEADERS, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i == attempts - 1:
                raise
            print(f"  retry {i + 1} for {url}: {e}")
            time.sleep(2 ** i)

def fetch_stock_bars(symbol, ref):
    # Pre + regular + post session (04:00–20:00 ET) so overnight-ish news
    # published around the session still finds prices to join against.
    lo, hi = day_bounds(ref, dtime(4, 0), dtime(20, 0))
    data = _get_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        {"interval": "1m", "period1": lo, "period2": hi, "includePrePost": "true"},
    )
    result = data["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]
    bars = [
        {"s": symbol, "p": float(c), "v": float(v or 0), "t": ts * 1000}
        for ts, c, v in zip(result.get("timestamp", []), quote["close"], quote["volume"])
        if c is not None
    ]
    return drop_bad_prints(bars)

def drop_bad_prints(bars, window=5, max_dev=0.015):
    # Yahoo's pre/post-market bars carry no volume and contain isolated bad
    # prints (e.g. AMZN -6% for one minute). Those fake a big "reaction" to
    # any news next to them, so drop bars far from their neighbours' median.
    kept = []
    for i, b in enumerate(bars):
        neighbours = sorted(x["p"] for x in bars[max(0, i - window): i + window + 1])
        median = neighbours[len(neighbours) // 2]
        if abs(b["p"] / median - 1) <= max_dev:
            kept.append(b)
    if len(kept) < len(bars):
        print(f"  {bars[0]['s']}: dropped {len(bars) - len(kept)} bad prints")
    return kept

def fetch_crypto_bars(symbol, ref):
    # Crypto trades around the clock, so take the whole ET calendar day
    lo, hi = day_bounds(ref)
    pair = symbol.split(":", 1)[1]  # BINANCE:BTCUSDT -> BTCUSDT
    bars, start = [], lo * 1000
    while start < hi * 1000:
        page = _get_json(
            "https://api.binance.com/api/v3/klines",
            {"symbol": pair, "interval": "1m", "startTime": start,
             "endTime": hi * 1000 - 1, "limit": 1000},
        )
        if not page:
            break
        bars += [{"s": symbol, "p": float(k[4]), "v": float(k[5]), "t": int(k[0])} for k in page]
        start = int(page[-1][0]) + 60_000
    return bars

def backfill(ref, pending):
    """Send bars for every symbol in `pending`; return the symbols that succeeded."""
    rows, done = [], set()
    for symbol in pending:
        try:
            fetch = fetch_crypto_bars if symbol.startswith("BINANCE:") else fetch_stock_bars
            bars = fetch(symbol, ref)
            rows += bars
            done.add(symbol)
            print(f"  {symbol}: {len(bars)} bars")
        except Exception as e:
            print(f"  {symbol}: backfill failed, will retry — {e}")
    # Send in global time order so each Kafka partition (shared by several
    # symbols) stays monotonic and Flink's watermarks see as little disorder as possible
    rows.sort(key=lambda r: r["t"])
    for r in rows:
        producer.send(topic="price_ticks", key=r["s"], value=r)
    producer.flush()
    return done

def main():
    backfilled = {}  # ref date -> symbols already sent
    while True:
        if is_market_open():
            print(f"[{datetime.now(MARKET_TZ):%H:%M:%S}] Market open — streaming live trades.")
            run_live()
            continue
        ref = reference_date()
        done = backfilled.setdefault(ref, set())
        pending = [s for s in SYMBOLS if s not in done]
        if pending:
            print(f"[{datetime.now(MARKET_TZ):%H:%M:%S}] Market closed — backfilling {ref} for {pending}")
            done |= backfill(ref, pending)
        time.sleep(CLOSED_POLL_INTERVAL)

if __name__ == "__main__":
    main()
