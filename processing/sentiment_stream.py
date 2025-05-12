#!/usr/bin/env python
import logging
import json, random, yaml, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_json, struct
from pyspark.sql.types import StructType, StringType, DoubleType

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler('logs/sentiment_stream.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "..", "config", "config.yaml")
cfg = yaml.safe_load(open(config_path))
logger.info("Configuration loaded successfully.")

# Create checkpoint directory if it doesn't exist
checkpoint_dir = os.path.join(script_dir, "..", "checkpoints", "sentiment")
os.makedirs(checkpoint_dir, exist_ok=True)
logger.info(f"Checkpoint directory: {checkpoint_dir}")

spark = (SparkSession.builder.appName(cfg["spark"]["app_name"])
         .master(cfg["spark"]["master"])
         .config("spark.jars.packages", cfg["spark"]["kafka_packages"])
         .getOrCreate())
logger.info("Spark session started successfully.")

schema_text = StructType() \
    .add("id", StringType()) \
    .add("text", StringType()) \
    .add("ts", DoubleType())

schema_price = StructType() \
    .add("symbol", StringType()) \
    .add("opening_price", DoubleType()) \
    .add("timestamp", StringType()) \
    .add("closing_price", DoubleType()) \
    .add("volume", DoubleType())

SENTIMENTS = ["POSITIVE", "NEGATIVE", "NEUTRAL"]

@udf("string")
def dummy_score(txt):
    """Very naive rule: contains 'tumble' → NEG, 'record' → POS, else random."""
    txt_low = txt.lower()
    if "tumble" in txt_low or "trouble" in txt_low:
        return "NEGATIVE"
    if "record" in txt_low or "moon" in txt_low:
        return "POSITIVE"
    return random.choice(SENTIMENTS)

def process_batch(df, epoch_id):
    # Convert the batch to rows and log each message
    rows = df.collect()
    logger.info(f"Processing batch {epoch_id} with {len(rows)} messages")
    for row in rows:
        message = row.asDict()
        logger.info(f"Processed message: {json.dumps(message)}")

def stream(topic: str, schema: StructType):
    logger.info(f"Starting stream for topic: {topic}")
    return (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"]["internal"])
             .option("subscribe", topic)
             .option("startingOffsets", "earliest")
             .option("failOnDataLoss", "false")
             .load()
             .selectExpr("CAST(value AS STRING) AS json")
             .select(from_json(col("json"), schema).alias("data"))
             .select("data.*")
    )

logger.info("Initializing streams for Twitter, Reddit, and News...")
twitter = stream(cfg["kafka"]["topics"]["twitter"], schema_text)
reddit  = stream(cfg["kafka"]["topics"]["reddit"],  schema_text)
news    = stream(cfg["kafka"]["topics"]["news"],    schema_text)

texts  = twitter.unionByName(reddit).unionByName(news)
logger.info("Successfully unioned all text streams.")

scored = texts.withColumn("sentiment", dummy_score(col("text")))
logger.info("Applied sentiment scoring to text stream.")

# Add console output for debugging
console_query = (scored
    .writeStream
    .outputMode("append")
    .format("console")
    .start())
logger.info("Started console output stream for debugging.")

# Write to Kafka with foreachBatch to log messages
write_query = (scored
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"]["internal"])
    .option("topic", cfg["kafka"]["topics"]["sentiment_stream"])
    .option("checkpointLocation", checkpoint_dir)
    .start())

logger.info(f"Started writeStream to topic: {cfg['kafka']['topics']['sentiment_stream']}")
logger.info("All streams are now running. Waiting for termination...")

spark.streams.awaitAnyTermination()
