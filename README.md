# MarketEcho: Real-Time Sentiment & Price Impact Pipeline

MarketEcho is an end-to-end, event-driven data engineering pipeline that captures live financial news, scores sentiment via NLP on the fly, and correlates results with real-time price movements using low-latency stream processing.

The project demonstrates production-grade streaming patterns: out-of-order data handling (watermarks), interval stream joins, and idempotent sink design.

---

## Architecture

```
Finnhub WebSocket               Finnhub News API
(price ticks — stocks + crypto) (REST polling every 60s)
        |                               |
        v                               v
  [price_ticks]                   [news_raw]
     Kafka topic (3 partitions)    Kafka topic (1 partition)
        |                               |
        +---------------+---------------+
                        |
                        v
               Apache Flink (PyFlink 1.19)
               - VADER sentiment scoring (UDF)
               - Watermarks — 30s out-of-order tolerance
               - Interval Join: price ticks in [news_ts - 5min, news_ts + 10min]
               - At-least-once delivery via HTTP sink
                        |
                        v
                   ClickHouse
                   ReplacingMergeTree (deduplication on merge)
                   TTL = 90 days | Partitioned by month
                        |
                        v
                     Grafana
               (price feed + sentiment timeline + impact table)
```

---

## Design Decisions

| Concern             | Decision                                                        | Rationale                                             |
| ------------------- | --------------------------------------------------------------- | ----------------------------------------------------- |
| Price data source   | Finnhub WebSocket (free tier)                                   | Real NYSE/NASDAQ + crypto data, no cost               |
| News data source    | Finnhub REST News API (poll 60s)                                | Same free key, company + market news                  |
| Tracked symbols     | AAPL, GOOGL, MSFT, TSLA, AMZN, META, NVDA + BTC/ETH (24/7)    | Stocks + crypto covers market hours and weekends      |
| Weekend handling    | News producer shifts to Saturday when run on Sunday            | Markets closed Sunday; last active day used as anchor |
| Kafka mode          | KRaft (no ZooKeeper)                                            | Simpler ops, fewer containers, Kafka 2.8+ native      |
| Flink state backend | In-memory (hashmap)                                             | Sufficient for low-volume local dev                   |
| Delivery guarantee  | At-least-once + idempotent sink                                 | Simpler than 2PC exactly-once, same analytical result |
| ClickHouse engine   | ReplacingMergeTree                                              | Handles duplicates from at-least-once delivery        |
| ClickHouse sink     | HTTP API (requests.post)                                        | No JDBC JAR needed; works natively with PyFlink       |
| Data retention      | 90-day TTL                                                      | Enough for pattern analysis and portfolio demo        |
| Deployment          | Docker Compose (local)                                          | Single-command startup, no cloud cost                 |

---

## Tech Stack

| Layer              | Technology                        |
| ------------------ | --------------------------------- |
| Stream Processing  | Apache Flink 1.19 (PyFlink)       |
| Message Broker     | Apache Kafka 7.6.1 (KRaft mode)   |
| Analytical Storage | ClickHouse 24.3                   |
| Price Producer     | Python — websocket-client         |
| News Producer      | Python — requests (REST polling)  |
| NLP                | VADER Sentiment (vaderSentiment)  |
| Visualization      | Grafana 10.4                      |
| Deployment         | Docker Compose                    |

---

## Project Structure

```
market-echo-flink-kafka/
├── docker-compose.yml                  # All services: Kafka, Flink, ClickHouse, Grafana
├── start.bat                           # One-command startup (cmd / PowerShell)
├── stop.bat                            # Teardown
├── stop.sh                             # Teardown (Git Bash)
├── requirements.txt                    # Python deps for producers
├── config/
│   └── settings.py                     # API key, symbols, Kafka/ClickHouse config
├── kafka/
│   ├── producers/
│   │   ├── price_producer.py           # Finnhub WebSocket → Kafka price_ticks
│   │   └── news_producer.py            # Finnhub REST polling → Kafka news_raw
│   └── consumers/
│       └── consumer.py                 # Debug consumer for verifying Kafka messages
├── flink_jobs/
│   └── sentiment_join.py               # PyFlink: VADER scoring + interval join → ClickHouse
├── clickhouse/
│   └── schema.sql                      # price_ticks, news_events, sentiment_impact tables
├── docker/
│   └── flink/
│       ├── Dockerfile                  # PyFlink image + Kafka connector JAR
│       └── lib/                        # flink-sql-connector-kafka-3.2.0-1.19.jar (download once)
├── grafana/
│   └── provisioning/
│       └── datasources/
│           └── clickhouse.yaml         # Auto-provisioned ClickHouse datasource
└── docs/
    ├── phase1-infrastructure.md
    ├── phase2-producers-schema.md
    └── phase3-deploy-grafana-observability.md
```

---

## Quickstart

### Prerequisites
- Docker Desktop running
- Python 3.10+ (`py` on Windows)
- Finnhub free API key — [finnhub.io](https://finnhub.io)

### First-time setup

```bash
# 1. Add your Finnhub key and symbols
cp config/settings.example.py config/settings.py
# Edit config/settings.py: set FINNHUB_API_KEY

# 2. Download the Flink Kafka connector JAR (one time only)
curl -L -o docker/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

# 3. Install Python dependencies
py -m pip install -r requirements.txt
```

### Start

```powershell
# CMD or PowerShell
.\start.bat
```

`start.bat` will:
1. Verify Docker is running
2. Start Kafka (KRaft), Flink, ClickHouse, Grafana via Docker Compose
3. Wait for each service to become healthy
4. Launch price and news producers in separate terminal windows
5. Submit the PyFlink sentiment job to the Flink cluster
6. Open Grafana at [http://localhost:3000](http://localhost:3000) (admin / admin)

### Stop

```powershell
.\stop.bat       # cmd / PowerShell
./stop.sh        # Git Bash
```

---

## Service URLs

| Service         | URL                                            |
| --------------- | ---------------------------------------------- |
| Grafana         | [http://localhost:3000](http://localhost:3000) |
| Flink UI        | [http://localhost:8081](http://localhost:8081) |
| Kafka UI        | [http://localhost:8080](http://localhost:8080) |
| ClickHouse HTTP | [http://localhost:8123/play](http://localhost:8123/play) |

---

## ClickHouse Tables

**`price_ticks`** — raw price stream from Kafka (written by price_producer directly; reserved for future use)

**`news_events`** — news articles enriched with VADER sentiment (reserved for future use)

**`sentiment_impact`** — one row per (news event × matched price tick) from the Flink interval join

```sql
-- Check data is flowing
SELECT count(), max(news_ts) FROM market_echo.sentiment_impact;

-- Sentiment impact: average price before vs after news
SELECT
    symbol,
    round(sentiment_score, 1) AS bucket,
    avgIf(price, is_before = 1) AS avg_price_before,
    avgIf(price, is_before = 0) AS avg_price_after,
    round(avgIf(price, is_before = 0) - avgIf(price, is_before = 1), 4) AS delta
FROM market_echo.sentiment_impact
GROUP BY symbol, bucket
ORDER BY symbol, bucket;
```
