# MarketEcho: Real-Time Sentiment & Price Impact Pipeline

MarketEcho is an end-to-end, event-driven data engineering pipeline that captures live financial news, scores sentiment via NLP on the fly, and correlates results with real-time stock price movements using low-latency stream processing.

The project demonstrates production-grade streaming patterns: out-of-order data handling (Watermarks), interval stream joins, and idempotent sink design.

---

## Architecture

```
Finnhub WebSocket          Finnhub News API
(price ticks, ~5-50/s)     (polling every 60s)
        |                        |
        v                        v
  [price_ticks]            [news_raw]
     Kafka topic             Kafka topic
        |                        |
        +----------+-------------+
                   |
                   v
            Apache Flink (PyFlink)
            - VADER sentiment scoring
            - Watermarks (out-of-order)
            - Interval Join:
              price [-5min, +10min] around news event
            - At-least-once delivery
                   |
                   v
             ClickHouse
             ReplacingMergeTree (idempotent)
             TTL = 90 days
             Partitioned by month
                   |
                   v
              Grafana
         (price line + news event flags)
```

---

## Design Decisions

| Concern | Decision | Rationale |
|---|---|---|
| Price data source | Finnhub WebSocket (free tier) | Real NYSE/NASDAQ data, no cost, real WebSocket |
| News data source | Finnhub REST News API (poll 60s) | Same free key, company + market news |
| Tracked symbols | 5–10 (AAPL, GOOGL, MSFT, TSLA, AMZN, META, NVDA) | Manageable state, meaningful correlations |
| Flink state backend | In-memory (heap) | Sufficient for low-volume local dev |
| Delivery guarantee | At-least-once + idempotent sink | Simpler than 2PC exactly-once, same analytical result |
| ClickHouse engine | ReplacingMergeTree | Handles duplicate events from at-least-once delivery |
| Data retention | 90-day TTL | Enough for pattern analysis and portfolio demo |
| Deployment | Docker Compose (local) | Single-command startup, no cloud cost |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Stream Processing | Apache Flink (PyFlink) |
| Message Broker | Apache Kafka |
| Analytical Storage | ClickHouse |
| Data Producers | Python (Asyncio, WebSockets) |
| NLP | VADER Sentiment |
| Visualization | Grafana |
| Deployment | Docker & Docker Compose |

---

## Project Structure (planned)

```
market-echo-flink-kafka/
├── docker-compose.yml
├── producers/
│   ├── price_producer.py       # Finnhub WebSocket → Kafka price_ticks
│   └── news_producer.py        # Finnhub REST polling → Kafka news_raw
├── flink_jobs/
│   └── sentiment_join.py       # PyFlink: sentiment + interval join → ClickHouse
├── clickhouse/
│   └── schema.sql              # Table definitions with TTL and partitioning
├── grafana/
│   └── dashboard.json          # Pre-built dashboard provisioning
├── config/
│   └── settings.py             # Symbols list, Kafka brokers, Finnhub key
└── README.md
```

---

## Quickstart

```bash
# 1. Add your free Finnhub API key (finnhub.io → free registration)
cp config/settings.example.py config/settings.py

# 2. One command to start everything
start.bat

# To shut everything down
stop.bat
```

`start.bat` will:
1. Verify Docker is running
2. Start Kafka, Flink, ClickHouse, Grafana via Docker Compose
3. Wait for each service to be healthy (no fixed sleeps)
4. Launch price and news producers in separate terminal windows
5. Submit the PyFlink job to the cluster
6. Open Grafana at http://localhost:3000 (login: admin / admin)

Flink UI is available at http://localhost:8081
