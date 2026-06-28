from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests
import json

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


class ClickHouseSink(MapFunction):
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
            print(f"ClickHouse write error: {e}")

        return row  # pass the row through so .print() can log it


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)
    t_env.create_temporary_function("vader_score", vader_score)

    # Read price ticks from Kafka.
    # `t` is epoch milliseconds from Finnhub; we convert it to a timestamp for watermarks.
    # Watermark of 30s means Flink will wait up to 30 seconds for late-arriving events.
    t_env.execute_sql("""
        CREATE TABLE price_ticks (
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
            'properties.group.id'          = 'flink-sentiment-job',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    # Read news events from Kafka.
    # `datetime` is epoch seconds from Finnhub, so we multiply by 1000 to get milliseconds.
    t_env.execute_sql("""
        CREATE TABLE news_raw (
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
            'properties.group.id'          = 'flink-sentiment-job',
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
            n.ts                                   AS news_ts,
            vader_score(n.headline, n.summary)     AS sentiment_score,
            p.ts                                   AS price_ts,
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
        .map(ClickHouseSink()) \
        .print()

    env.execute("MarketEcho Sentiment Join")


if __name__ == "__main__":
    main()
