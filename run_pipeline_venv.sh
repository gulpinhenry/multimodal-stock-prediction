#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────────
# Cross-platform launcher:
#   • Brings up Docker services (ZK, Kafka, Spark master & worker)
#   • Waits for Kafka on localhost:9092
#   • Submits Spark job inside the spark-master container
#   • Starts all four dummy producers on the host
# ────────────────────────────────────────────────────────────────────────────────

echo "⟳ Clearing any existing stack…"
docker compose down -v >/dev/null 2>&1 || true
set -euo pipefail

mkdir -p logs

# 0) define variables
SPARK_MASTER_SERVICE=spark-master
SPARK_MASTER_URL=spark://spark-master:7077
APP_PATH=/opt/app               # where we mount your repo inside the container
PY_SPARK_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5

# 1) Up your core services
echo "⟳ Bringing up ZK, Kafka, Elasticsearch & Spark cluster in Docker…"
docker compose up -d zookeeper kafka elasticsearch kibana spark-master spark-worker test-kafka-consumer
docker compose exec kafka \
  kafka-topics --create --topic twitter_raw  --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092 && \
  kafka-topics --create --topic reddit_raw   --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092 && \
  kafka-topics --create --topic news_raw     --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092 && \
  kafka-topics --create --topic sentiment_scored --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092

# 2) Wait for Kafka
echo "↻ Waiting for Kafka on localhost:9092 …"
for i in {1..60}; do
  if (echo > /dev/tcp/127.0.0.1/9092) &>/dev/null; then
    echo "✓ Kafka is up."
    break
  fi
  sleep 1
  echo "Waiting… ($i)"
done

# 3) Wait for Elasticsearch
echo "↻ Waiting for Elasticsearch on localhost:9200 …"
for i in {1..60}; do
  if curl -s "http://localhost:9200/_cluster/health" > /dev/null; then
    echo "✓ Elasticsearch is up."
    break
  fi
  sleep 1
  echo "Waiting… ($i)"
done

# 3) Submit Spark streaming job inside the container
#    assumes your compose mounts the repo at /opt/app and working_dir is /opt/app
# echo "⟳ Submitting Spark job inside container ${SPARK_MASTER_SERVICE}…"
# MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" \
# docker compose exec ${SPARK_MASTER_SERVICE} \
#   /opt/bitnami/spark/bin/spark-submit \
#     --master ${SPARK_MASTER_URL} \
#     --packages ${PY_SPARK_PACKAGE} \
#     --conf spark.jars.ivy=/tmp/ivy-cache \
#     --conf spark.hadoop.hadoop.security.authentication=NOSASL \
#     --conf spark.hadoop.hadoop.security.authorization=false \
#     ${APP_PATH}/processing/sentiment_stream.py \
#   2>&1 | tee logs/spark-job.log | sed 's/^/   [spark] /g'



# 4) Launch dummy producers on the host
echo "⟳ Starting dummy producers on host…"

for p in price; do
  python "ingestion/${p}_producer.py" &
done

# 5) Wait for everything
wait
