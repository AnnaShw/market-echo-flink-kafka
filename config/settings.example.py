# Copy to config/settings.py (gitignored) and fill in your key.

# ── Finnhub ────────────────────────────────────────────────
FINNHUB_API_KEY = "your-finnhub-api-key"  # free key: finnhub.io

# ── Symbols ────────────────────────────────────────────────
# Stocks use their ticker; crypto uses Finnhub's EXCHANGE:PAIR form
# (backfill maps BINANCE:BTCUSDT -> Binance's BTCUSDT)
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "META", "NVDA",
           "BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]

# ── Kafka ──────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_PRICES = "price_ticks"
KAFKA_TOPIC_NEWS   = "news_raw"

# ── ClickHouse ─────────────────────────────────────────────
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB   = "market_echo"
CLICKHOUSE_USER = "default"
