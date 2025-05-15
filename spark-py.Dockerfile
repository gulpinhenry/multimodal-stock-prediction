# ./spark-py.Dockerfile
# FROM bitnami/spark:3.5.5
# RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
#         pyyaml kafka-python pandas yfinance praw faker
FROM bitnami/spark:3.5.5

# Install curl
USER root
RUN install_packages curl

# Install Python packages
RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
    pyyaml kafka-python pandas yfinance praw faker


# Add Kafka connectors to Spark classpath
# RUN mkdir -p /opt/bitnami/spark/jars && \
#     curl -Lo /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar \
#          https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar && \
#     curl -Lo /opt/bitnami/spark/jars/kafka-clients-3.5.1.jar \
#          https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
#     curl -Lo /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
#          https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar

# Add Kafka connectors and dependencies to Spark classpath
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -Lo /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar \
         https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar && \
    curl -Lo /opt/bitnami/spark/jars/kafka-clients-3.5.1.jar \
         https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -Lo /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
         https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar && \
    curl -Lo /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
         https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

