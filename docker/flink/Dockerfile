FROM flink:1.19-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev curl && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir \
    apache-flink==1.19.0 \
    vaderSentiment==3.3.2 \
    confluent-kafka==2.4.0 \
    requests==2.31.0
