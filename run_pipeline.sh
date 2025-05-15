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
echo "⟳ Bringing up ZK, Kafka, Elasticsearch, Kibana, MongoDB & Spark cluster in Docker…"
docker compose up -d zookeeper kafka elasticsearch kibana mongodb spark-master spark-worker

echo "⟳ Creating Kafka topics..."
docker compose exec kafka bash -c "
  kafka-topics --create --topic twitter_raw       --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092
  kafka-topics --create --topic prices_raw       --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092
  kafka-topics --create --topic reddit_raw        --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092
  kafka-topics --create --topic news_raw          --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092
  kafka-topics --create --topic sentiment_scored  --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server kafka:29092
"

echo "✓ Kafka topics ensured. Waiting a few seconds for topic propagation..."
sleep 5 # Add a small delay

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

# 4) Submit Spark streaming job
echo "⟳ Submitting Spark job inside container ${SPARK_MASTER_SERVICE}…"
docker compose exec -T ${SPARK_MASTER_SERVICE} bash -c "\
  nohup /opt/bitnami/spark/bin/spark-submit \
      --master ${SPARK_MASTER_URL} \
      /opt/app/processing/sentiment_stream.py > /opt/app/logs/spark_submit.log 2>&1 &"

# 5) Start core services in correct order
echo "⟳ Starting core services..."

# Start MongoDB consumer
echo "⟳ Starting MongoDB consumer..."
conda run -n sentiment-stocks python "ingestion/mongo_consumer.py" > logs/mongo_consumer_stdout.log 2> logs/mongo_consumer_stderr.log &
MONGO_CONSUMER_PID=$!
echo "✓ MongoDB consumer started (PID: $MONGO_CONSUMER_PID)"

# Wait for MongoDB consumer to be ready
echo "↻ Waiting for MongoDB consumer to be ready..."
sleep 5

# Start Sentiment Analyzer
echo "⟳ Starting Sentiment Analyzer..."
conda run -n sentiment-stocks python "models/sentiment_analyzer.py" > logs/sentiment_analyzer.log 2>&1 &
SENTIMENT_ANALYZER_PID=$!
echo "✓ Sentiment Analyzer started (PID: $SENTIMENT_ANALYZER_PID)"

# Wait for Sentiment Analyzer to initialize
echo "↻ Waiting for Sentiment Analyzer to initialize..."
sleep 10

# Start producers
echo "⟳ Starting data producers..."
for p in twitter price; do
  conda run -n sentiment-stocks --no-capture-output \
        python "ingestion/${p}_producer.py" &
  echo "✓ ${p} producer started"
done

# Training and Inference
while getopts "ti" opt; do
  case $opt in
    t)
      echo "⟳ Starting model training with fresh data aggregation..."
      cd "$(dirname "$0")"
      conda run -n sentiment-stocks python models/model_training.py > logs/model_training.log 2>&1
      ;;
    i)
      echo "⟳ Starting inference service..."
      (conda run -n sentiment-stocks python models/inference.py > logs/inference_service.log 2>&1) &
      echo $! > inference_service.pid
      ;;
    *)
      echo "Usage: $0 [-t] [-i]" >&2
      echo "  -t  Run model training" >&2
      echo "  -i  Start inference service" >&2
      exit 1
      ;;
  esac
done

# 6) Verify services are running
echo "✓ Verifying services..."
if ! ps -p $MONGO_CONSUMER_PID > /dev/null; then
    echo "❌ MongoDB consumer is not running"
    exit 1
fi

if ! ps -p $SENTIMENT_ANALYZER_PID > /dev/null; then
    echo "❌ Sentiment Analyzer is not running"
    exit 1
fi

# 7) Check MongoDB data
echo "✓ Verifying MongoDB data..."
docker exec -it multimodal-stock-prediction-mongodb-1 mongosh --eval "
db = db.getSiblingDB('stock_data');
print('Collections:');
db.getCollectionNames();
print('\nSample sentiment data:');
db.stream_data.find({'data.type':'sentiment'}).limit(1).pretty();
print('\nSample price data:');
db.stream_data.find({'data.type':'price'}).limit(1).pretty();
" > logs/mongo_data_check.log

echo "✓ Pipeline ready"
echo "   - Use './run_pipeline.sh -t' to train model"
echo "   - Use './run_pipeline.sh -i' to start inference"

# 8) Wait for everything
wait