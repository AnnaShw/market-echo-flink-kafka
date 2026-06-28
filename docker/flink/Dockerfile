# Base Flink image — includes Java 11 and the Flink runtime, but no Python
FROM flink:1.19-scala_2.12-java11

# Install Python to can run PyFlink jobs
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev curl && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

# Install Python dependencies for the Flink job:
#   apache-flink   — PyFlink API (must match the Flink runtime version above)
#   vaderSentiment — NLP library that scores text sentiment without any model training
#   confluent-kafka — Kafka client used by producers (not the Flink job itself)
#   requests       — HTTP client used to write results to ClickHouse
RUN pip3 install --no-cache-dir \
    apache-flink==1.19.0 \
    vaderSentiment==3.3.2 \
    confluent-kafka==2.4.0 \
    requests==2.31.0

# Add the Kafka connector JAR so Flink can read from Kafka topics.
# This is a fat JAR — it includes kafka-clients, so no separate download needed.
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.19/flink-sql-connector-kafka-3.1.0-1.19.jar
