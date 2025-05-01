# ./spark-py.Dockerfile
FROM bitnami/spark:3.5.5
RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
        pyyaml kafka-python pandas yfinance praw faker
