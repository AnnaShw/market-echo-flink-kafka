CREATE DATABASE IF NOT EXISTS market_echo;

-- ── price_ticks ────────────────────────────────────────────────────────────
-- Raw price ticks from Finnhub WebSocket via Kafka price_ticks topic.
-- Keyed by (symbol, ts) — ReplacingMergeTree deduplicates at-least-once retries.
CREATE TABLE IF NOT EXISTS market_echo.price_ticks
(
    symbol  LowCardinality(String),
    price   Float64,
    volume  Float64,
    ts      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ── news_events ────────────────────────────────────────────────────────────
-- News articles enriched with VADER sentiment score, written by Flink job.
CREATE TABLE IF NOT EXISTS market_echo.news_events
(
    id              String,
    symbol          LowCardinality(String),
    headline        String,
    sentiment_score Float32,
    ts              DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, id)
TTL toDateTime(ts) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ── sentiment_impact ───────────────────────────────────────────────────────
-- Result of Flink interval join: price movement in [-5min, +10min] around a news event.
-- price_delta = price_after - price_before.
CREATE TABLE IF NOT EXISTS market_echo.sentiment_impact
(
    symbol          LowCardinality(String),
    news_ts         DateTime64(3, 'UTC'),
    sentiment_score Float32,
    price_before    Float64,
    price_after     Float64,
    price_delta     Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(news_ts)
ORDER BY (symbol, news_ts)
TTL toDateTime(news_ts) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
