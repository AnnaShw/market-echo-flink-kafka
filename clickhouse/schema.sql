-- VADER sentiment score: a number from -1.0 to +1.0.
-- -1.0 = very negative ("company collapses"), 0 = neutral, +1.0 = very positive ("record profits").
-- Stored in sentiment_score column of news_events and sentiment_impact tables.

-- ReplacingMergeTree: if two rows have the same ORDER BY key, ClickHouse keeps only the latest one.
-- We use this to handle duplicates from Kafka's at-least-once delivery —
-- if Flink sends the same row twice, the duplicate is silently dropped on the next merge.

CREATE DATABASE IF NOT EXISTS market_echo;

-- ── price_ticks ────────────────────────────────────────────────────────────
-- Raw price ticks from Finnhub WebSocket. Kafka topic: price_ticks.
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
-- One row per (news event × matched price tick) from Flink interval join.
-- Window: price ticks in [news_ts - 5min, news_ts + 10min].
-- is_before=1 → price tick arrived before or at the news event.
-- Grafana computes price_before/after as AVG(price WHERE is_before=1/0).
CREATE TABLE IF NOT EXISTS market_echo.sentiment_impact
(
    symbol          LowCardinality(String),
    news_ts         DateTime64(3, 'UTC'),
    sentiment_score Float32,
    price_ts        DateTime64(3, 'UTC'),
    price           Float64,
    is_before       UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(news_ts)
ORDER BY (symbol, news_ts, price_ts)
TTL toDateTime(news_ts) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
