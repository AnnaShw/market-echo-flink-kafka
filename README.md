# 📈 MarketEcho: Real-Time Sentiment & Price Impact Pipeline

MarketEcho is an end-to-end, event-driven data engineering pipeline designed to capture financial news streams, evaluate their emotional sentiment via NLP on the fly, and correlate them with real-time stock price fluctuations using low-latency stream processing.

The project demonstrates production-grade streaming patterns such as out-of-order data management (Watermarks), interval stream joins, and transactional consistency (Exactly-Once semantics).

---

## 🏗️ Architecture at a Glance

1. **Ingestion (Producers):** Asynchronous Python scripts streaming live financial news and high-frequency WebSocket price ticks into Kafka.
2. **Message Broker (Kafka):** Distributed backbone decoupling data ingestion from heavy computation.
3. **Processing Engine (Apache Flink):** PyFlink orchestrates sentiment scoring, handles late-arriving data via Watermarks, and performs an interval join to measure price change 5m before vs. 10m after a news flash.
4. **Analytical Storage (Sink):** Cleaned, joined, and calculated data is written into ClickHouse.
5. **Visualization:** Real-time Grafana dashboard mapping stock price lines with instant news event flags.

---

## 🛠️ Tech Stack
* **Stream Processing:** Apache Flink (PyFlink)
* **Ingestion Backbone:** Apache Kafka
* **Storage:** ClickHouse
* **Analytics Engine:** Python (Asyncio, WebSockets, VADER Sentiment NLP)
* **Visualization:** Grafana
* **Deployment:** Docker & Docker Compose
