# MarketEcho

MarketEcho pulls in stock and crypto prices along with news about those companies. It scores each headline as positive or negative, then checks what the price did in the minutes after the news came out.

I built it to practise streaming with Kafka, Flink and ClickHouse on a real, messy data source. Everything runs locally in Docker and uses only free APIs.

![MarketEcho Grafana dashboard](images/dashboard.png)

*The dashboard for 29 Sep 2026 (times in New York time).*

---

## How it works

```
  Market open                      Market closed
  live trades from Finnhub         1-minute prices for the last trading day
  (websocket)                      (Yahoo for stocks, Binance for crypto)
            \                      /
             v                    v
            price_producer.py                 news_producer.py
                    |                         (asks Finnhub for news every minute)
                    v                                  |
           Kafka: price_ticks                 Kafka: news_raw
                    \                                 /
                     v                               v
                            Flink job
                  - scores each headline with VADER
                  - matches every news item with the prices
                    from 5 min before to 10 min after it
                                  |
                                  v
                             ClickHouse
                                  |
                                  v
                               Grafana
```

1. **Prices.** While the US market is open, the price producer streams live trades from Finnhub. When it's closed, it downloads 1-minute prices for the last trading day instead.
2. **News.** The news producer asks Finnhub for company news once a minute.
3. **Flink** reads both Kafka topics, gives each headline a sentiment score and pairs every news item with the prices around it.
4. **ClickHouse** stores the results, and **Grafana** shows them.

### What happens when the market is closed

If you open the dashboard at night or on a weekend, live data would mean almost no stock prices and news that doesn't line up with any prices. So outside market hours, **everything switches to the last trading day**: stocks, crypto and news. The dashboard always shows one complete day, and news can still be matched with prices.

| When (New York time)            | Which day you see     | Where prices come from          |
| ------------------------------- | --------------------- | ------------------------------- |
| Trading day, 9:30–16:00         | today                 | live trades                     |
| Trading day, after 16:00        | today                 | 1-minute prices for today       |
| Before 9:30, weekends, holidays | last trading day      | 1-minute prices for that day    |

This logic lives in [`config/market_calendar.py`](config/market_calendar.py) and uses the NYSE holiday calendar.

---

## The dashboard

Grafana loads it automatically from [`grafana/provisioning/dashboards/market-echo.json`](grafana/provisioning/dashboards/market-echo.json). All times are New York time.

**Prices**
- **Price change since start of range**: how much each symbol went up or down, in %. Putting everything in % lets an $84k Bitcoin and a $300 stock share one chart. Hover to see the % change and the actual price.
- **Last price & change in range**: where each symbol ended up.

**News sentiment**
- **How to read the sentiment score**: a short explanation with real example headlines.
- **Sentiment by company**: which companies got better or worse press.
- **Sentiment heatmap**: the tone of each company's news, hour by hour.
- **News volume per hour**: how much news came out and how much of it was positive or negative.

**Does sentiment move price?**
- **Price reaction by sentiment class**: after positive, neutral and negative news, how much did the price move on average, and how often did it go up?
- **Top 10 price moves after news**: the headlines followed by the biggest moves, and whether the move matched the tone of the headline.

**Raw feed**
- **Recent Headlines**: the latest headlines with their scores.

### What the sentiment score means

Every headline (plus its short summary) gets a score from **−1** to **+1**. The score measures how negative or positive the *wording* is. It comes from [VADER](https://github.com/cjhutto/vaderSentiment), a list of about 7,500 words that people rated as good or bad, with a few extra rules for things like "not good", CAPS and "!!!".

| Score          | Level         | Example from 29 Sep                                            |
| -------------- | ------------- | -------------------------------------------------------------- |
| −1 … −0.5      | Very negative | "Apple Faces a New Threat From Meta's Muse" (−0.72)            |
| −0.5 … −0.05   | Negative      | "Tesla Delays Roadster Reveal Over Severe Weather in Texas" (−0.38) |
| −0.05 … +0.05  | Neutral       | "Company News for Sep 29, 2026" (0.00)                         |
| +0.05 … +0.5   | Positive      | "3 Reasons To Buy Airbnb Now" (+0.36)                          |
| +0.5 … +1      | Very positive | "OpenAI tried to invest $100 million in Hugging Face" (+0.54)  |

VADER looks at words, not meaning. "MongoDB stock crashes 26% as its CEO jumps ship" scored **+0.40** because its summary used upbeat language. Treat the score as the tone of the text, not as a tip to buy or sell.

### So, does it move the price?

Not really, at least not on the first day of data. On 29 Sep the price moved about 0% on average after both positive and negative news, and it went up roughly half the time either way. That's a real result, not a bug: headline wording is a weak signal. The panels will say more once there are more days of data.

---

## Problems I ran into (and what I did about them)

- **Finnhub's free plan has no price history.** Asking for past prices returns an error. For past days I use Yahoo (stocks) and Binance (crypto) instead. Neither needs an API key.
- **News came in newest-first, and Flink threw most of it away.** Flink expects events roughly in time order and drops anything that shows up too late. About 99% of the news was being dropped, which is why the news-to-price table stayed empty for a long time. The fix was to sort the news oldest-first and give Flink up to a day of slack.
- **That slack uses memory.** Holding a day of prices inside Flink doesn't fit in memory, so Flink stores that state on disk with RocksDB.
- **Yahoo sometimes returns bad prices.** Before and after regular trading hours, Yahoo's data has odd one-minute spikes, for example Amazon "dropping" 6% for a single minute. One of those spikes had become the biggest "reaction to news" on the dashboard. Now any minute that's more than 1.5% away from its neighbours gets thrown out.
- **The last few prices got stuck.** Prices are written to ClickHouse in batches of 200. When the market was closed, no new prices came in to fill the last batch, so it never got written. Now the batch is also written every 5 seconds.
- **Duplicates.** If something retries, the same row can be written twice. ClickHouse tables use `ReplacingMergeTree`, which removes those duplicates on its own, so rerunning anything is safe.

---

## Tech stack

| What              | Tool                                   |
| ----------------- | -------------------------------------- |
| Stream processing | Apache Flink 1.19 (PyFlink)            |
| Message broker    | Apache Kafka (KRaft, no ZooKeeper)     |
| Storage           | ClickHouse 24.3                        |
| Producers         | Python (websocket-client, requests)    |
| Sentiment         | VADER                                  |
| Dashboard         | Grafana 10.4                           |
| Running it        | Docker Compose                         |

---

## Project layout

```
market-echo-flink-kafka/
├── docker-compose.yml         # Kafka, Flink, ClickHouse, Grafana, Kafka UI
├── start.bat / stop.bat       # start and stop everything
├── requirements.txt
├── config/
│   ├── settings.example.py    # copy to settings.py and add your API key
│   └── market_calendar.py     # is the market open, and which day to show
├── kafka/
│   ├── producers/
│   │   ├── price_producer.py  # live trades, or 1-minute prices for a past day
│   │   └── news_producer.py   # company news from Finnhub
│   └── consumers/consumer.py  # small helper for peeking at Kafka messages
├── flink_jobs/
│   └── sentiment_join.py      # scores news and matches it with prices
├── clickhouse/schema.sql      # the three tables
├── docker/flink/              # Flink image with the Kafka connector
├── grafana/provisioning/      # datasource and dashboard, loaded automatically
└── images/dashboard.png       # the screenshot above
```

---

## Running it

### You'll need
- Docker Desktop
- Python 3.10+ (`py` on Windows)
- A free API key from [finnhub.io](https://finnhub.io)

### First time only

```bash
# 1. Add your Finnhub key
cp config/settings.example.py config/settings.py
# then open config/settings.py and set FINNHUB_API_KEY

# 2. Download the Kafka connector for Flink
curl -L -o docker/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

# 3. Install the Python packages
py -m pip install -r requirements.txt
```

### Start

```powershell
.\start.bat
```

This starts all the containers, waits until they're ready, opens the two producers in their own windows, starts the Flink job and opens Grafana at [localhost:3000](http://localhost:3000) (login `admin` / `admin`).

If the market is closed, the price producer prints how many 1-minute prices it loaded per symbol (for example `AAPL: 960 bars`) and then waits for the next market open.

### Stop

```powershell
.\stop.bat
```

Closing the producer windows also stops new data from coming in.

### Useful links

| What            | Where                                          |
| --------------- | ---------------------------------------------- |
| Grafana         | [localhost:3000](http://localhost:3000)        |
| Flink           | [localhost:8081](http://localhost:8081)        |
| Kafka UI        | [localhost:8080](http://localhost:8080)        |
| ClickHouse      | [localhost:8123/play](http://localhost:8123/play) |

---

## The data

There are three tables in ClickHouse, all in the `market_echo` database:

- **`price_ticks`**: every price, either a live trade or a 1-minute price.
- **`news_events`**: every headline with its sentiment score.
- **`sentiment_impact`**: each news item paired with every price from 5 minutes before to 10 minutes after it. `is_before` tells you which side of the news the price was on.

Only news that came out while there were prices can be matched. Stock prices exist from 4:00 to 20:00 New York time, so late-night articles show up in `news_events` but not in `sentiment_impact`.

A couple of queries to try:

```sql
-- Is data coming in?
SELECT count(), max(ts) FROM market_echo.price_ticks;
SELECT count(), max(news_ts) FROM market_echo.sentiment_impact;

-- Does sentiment move the price?
-- (% change: average price 10 min after vs 5 min before each news item)
WITH per_news AS (
    SELECT symbol, news_ts, any(sentiment_score) AS s,
           (avgIf(price, is_before = 0) / avgIf(price, is_before = 1) - 1) * 100 AS move
    FROM market_echo.sentiment_impact
    GROUP BY symbol, news_ts
    HAVING countIf(is_before = 1) > 0 AND countIf(is_before = 0) > 0
)
SELECT multiIf(s >= 0.05, 'positive', s <= -0.05, 'negative', 'neutral') AS sentiment,
       count()                         AS news,
       round(avg(move), 3)             AS avg_move_pct,
       round(countIf(move > 0) / count() * 100) AS went_up_pct
FROM per_news
GROUP BY sentiment;
```
