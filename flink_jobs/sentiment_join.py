from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests
import json
import threading

# VADER looks up each word in a hand-rated dictionary (~7500 words, scored -4 to +4).
# It adjusts scores for context: "not great" flips the sign, "GREAT" gets a caps bonus,
# "great!!!" gets an exclamation bonus. All word scores are summed and normalized to
# a single "compound" number between -1.0 (very negative) and +1.0 (very positive).
analyzer = SentimentIntensityAnalyzer()

@udf(result_type=DataTypes.FLOAT())
def vader_score(headline, summary):
    # Combine headline and summary, then return the compound sentiment score
    text = f"{headline or ''} {summary or ''}".strip()
    return float(analyzer.polarity_scores(text)["compound"])


class SentimentImpactSink(MapFunction):
    def map(self, row):
        symbol, news_ts, sentiment_score, price_ts, price, is_before = row

        # Build a JSON row and send it to ClickHouse over HTTP
        data = json.dumps({
            "symbol":          str(symbol),
            "news_ts":         str(news_ts),
            "sentiment_score": float(sentiment_score),
            "price_ts":        str(price_ts),
            "price":           float(price),
            "is_before":       int(bool(is_before)),  # 1 = price tick happened before the news
        })
        try:
            requests.post(
                "http://clickhouse:8123/",
                params={"query": "INSERT INTO market_echo.sentiment_impact FORMAT JSONEachRow"},
                data=data,
                timeout=5,
            )
        except Exception as e:
            print(f"ClickHouse write error (sentiment_impact): {e}")

        return row  # pass the row through so .print() can log it


class PriceTicksSink(MapFunction):
    # Price ticks arrive far more often than news (crypto never stops trading),
    # so unlike the other sinks this one batches rows into one HTTP insert
    # instead of firing a request per row.
    BATCH_SIZE = 200
    # A size-only trigger would strand the tail of a backfill in the buffer
    # forever when the market is closed and no further ticks arrive
    FLUSH_INTERVAL_S = 5

    def open(self, runtime_context):
        self.buffer = []
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.flusher = threading.Thread(target=self._flush_periodically, daemon=True)
        self.flusher.start()

    def map(self, row):
        symbol, price, volume, ts = row
        with self.lock:
            self.buffer.append({
                "symbol": str(symbol),
                "price":  float(price),
                "volume": float(volume),
                "ts":     str(ts),
            })
            full = len(self.buffer) >= self.BATCH_SIZE
        if full:
            self._flush()
        return row

    def _flush_periodically(self):
        while not self.stopped.wait(self.FLUSH_INTERVAL_S):
            self._flush()

    def _flush(self):
        with self.lock:
            rows, self.buffer = self.buffer, []
        if not rows:
            return
        data = "\n".join(json.dumps(r) for r in rows)
        try:
            requests.post(
                "http://clickhouse:8123/",
                params={"query": "INSERT INTO market_echo.price_ticks FORMAT JSONEachRow"},
                data=data,
                timeout=5,
            )
        except Exception as e:
            print(f"ClickHouse write error (price_ticks): {e}")

    def close(self):
        self.stopped.set()
        self._flush()


class NewsEventsSink(MapFunction):
    def map(self, row):
        id_, symbol, headline, sentiment_score, ts = row

        # Every news event gets scored and stored here, whether or not it
        # ever matches a price tick in the interval join below.
        data = json.dumps({
            "id":              str(id_),
            "symbol":          str(symbol),
            "headline":        str(headline),
            "sentiment_score": float(sentiment_score),
            "ts":              str(ts),
        })
        try:
            requests.post(
                "http://clickhouse:8123/",
                params={"query": "INSERT INTO market_echo.news_events FORMAT JSONEachRow"},
                data=data,
                timeout=5,
            )
        except Exception as e:
            print(f"ClickHouse write error (news_events): {e}")

        return row


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # PyFlink doesn't pick up JARs from /opt/flink/lib/ automatically — add explicitly
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar")
    t_env = StreamTableEnvironment.create(env)
    t_env.create_temporary_function("vader_score", vader_score)

    # Read price ticks from Kafka.
    # `t` is epoch milliseconds from Finnhub; we convert it to a timestamp for watermarks.
    #
    # The two tables feeding the interval join (this one and news_raw) tolerate
    # 1 day of disorder: when the market is closed the producers backfill the
    # previous trading day, which lands in the topics *after* newer live data.
    # A tight watermark would mark that whole backfill as late and the join
    # would silently drop it. The interval join still emits matches as soon as
    # both sides arrive — the watermark only controls when old state is freed,
    # so the cost is ~1 day of join state (kept in RocksDB, not on the heap).
    t_env.execute_sql("""
        CREATE TABLE price_ticks (
            `s`  STRING,
            `p`  DOUBLE,
            `v`  DOUBLE,
            `t`  BIGINT,
            ts   AS TO_TIMESTAMP_LTZ(`t`, 3),
            WATERMARK FOR ts AS ts - INTERVAL '1' DAY
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'price_ticks',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id'          = 'flink-sentiment-job',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    # Second independent reader on the price_ticks topic, own consumer group —
    # same reasoning as news_raw_for_events below: this feeds the raw
    # price_ticks ClickHouse sink separately from the interval join above,
    # so the two queries don't compete over one group.id.
    t_env.execute_sql("""
        CREATE TABLE price_ticks_for_raw (
            `s`  STRING,
            `p`  DOUBLE,
            `v`  DOUBLE,
            `t`  BIGINT,
            ts   AS TO_TIMESTAMP_LTZ(`t`, 3),
            WATERMARK FOR ts AS ts - INTERVAL '30' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'price_ticks',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id'          = 'flink-sentiment-job-price-raw',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    # Read news events from Kafka.
    # `datetime` is epoch seconds from Finnhub, so we multiply by 1000 to get milliseconds.
    #
    # This table is read by two independent queries below (the news_events sink and
    # the price interval join). Each query gets its own CREATE TABLE with a distinct
    # `properties.group.id`: the topic has a single partition, so two readers sharing
    # one group.id would compete for it and one would starve. Separate group IDs let
    # both read the full topic independently from earliest-offset.
    t_env.execute_sql("""
        CREATE TABLE news_raw (
            `id`       STRING,
            `symbol`   STRING,
            `headline` STRING,
            `summary`  STRING,
            `datetime` BIGINT,
            ts         AS TO_TIMESTAMP_LTZ(`datetime` * 1000, 3),
            WATERMARK FOR ts AS ts - INTERVAL '1' DAY
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'news_raw',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id'          = 'flink-sentiment-job',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE news_raw_for_events (
            `id`       STRING,
            `symbol`   STRING,
            `headline` STRING,
            `summary`  STRING,
            `datetime` BIGINT,
            ts         AS TO_TIMESTAMP_LTZ(`datetime` * 1000, 3),
            WATERMARK FOR ts AS ts - INTERVAL '30' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'news_raw',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id'          = 'flink-sentiment-job-events',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    # For each news event, find all price ticks within a 15-minute window:
    # 5 minutes before the news and 10 minutes after.
    # Each matched pair becomes one output row.
    # is_before tells us whether the price tick happened before or after the news.
    result = t_env.sql_query("""
        SELECT
            n.symbol                               AS symbol,
            CAST(n.ts AS TIMESTAMP(3))             AS news_ts,
            vader_score(n.headline, n.summary)     AS sentiment_score,
            CAST(p.ts AS TIMESTAMP(3))             AS price_ts,
            p.p                                    AS price,
            p.ts <= n.ts                           AS is_before
        FROM news_raw n
        JOIN price_ticks p
          ON n.symbol = p.s
         AND p.ts BETWEEN n.ts - INTERVAL '5' MINUTE
                      AND n.ts + INTERVAL '10' MINUTE
    """)

    # Write each result row to ClickHouse; also print to Flink logs for debugging
    t_env.to_data_stream(result) \
        .map(SentimentImpactSink()) \
        .print()

    # Score and store every news event on its own, independent of whether it
    # ever finds a matching price tick above.
    news_events = t_env.sql_query("""
        SELECT
            n.id                                    AS id,
            n.symbol                                AS symbol,
            n.headline                              AS headline,
            vader_score(n.headline, n.summary)      AS sentiment_score,
            CAST(n.ts AS TIMESTAMP(3))              AS ts
        FROM news_raw_for_events n
    """)

    t_env.to_data_stream(news_events) \
        .map(NewsEventsSink()) \
        .print()

    # Store every raw price tick, independent of the news join above —
    # this is what powers the live price panel in Grafana.
    price_ticks_raw = t_env.sql_query("""
        SELECT
            s                          AS symbol,
            p                          AS price,
            v                          AS volume,
            CAST(ts AS TIMESTAMP(3))   AS ts
        FROM price_ticks_for_raw
    """)

    t_env.to_data_stream(price_ticks_raw) \
        .map(PriceTicksSink()) \
        .print()

    env.execute("MarketEcho Sentiment Join")


if __name__ == "__main__":
    main()
