#!/usr/bin/env python
import logging
import json, random, yaml, os, uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import to_json, struct
from collections import defaultdict


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

# Generate a unique checkpoint directory per run
unique_id = str(uuid.uuid4())[:6]
checkpoint_dir = os.path.join(script_dir, "..", "checkpoints", f"sentiment_{unique_id}")
os.makedirs(checkpoint_dir, exist_ok=True)
logger.info(f"Using checkpoint directory: {checkpoint_dir}")

# Initialize Spark session
spark = (SparkSession.builder.appName(cfg["spark"]["app_name"])
         .master(cfg["spark"]["master"])
         .config("spark.jars.packages", cfg["spark"]["kafka_packages"])
         .getOrCreate())
logger.info("Spark session started successfully.")

# Define schemas
schema_text = StructType() \
    .add("id", StringType()) \
    .add("company", StringType()) \
    .add("timestamp", StringType()) \
    .add("text", StringType()) \
    .add("replyCount", IntegerType()) \
    .add("likeCount", IntegerType()) \
    .add("quoteCount", IntegerType()) \
    .add("repostCount", IntegerType())

schema_price = StructType() \
    .add("symbol", StringType()) \
    .add("closing_price", DoubleType()) \
    .add("opening_price", DoubleType()) \
    .add("high", DoubleType()) \
    .add("low", DoubleType()) \
    .add("volume", DoubleType()) \
    .add("timestamp", StringType())

SENTIMENTS = ["POSITIVE", "NEGATIVE", "NEUTRAL"]

@udf("string")
def dummy_score(txt):
    txt_low = txt.lower()
    if "tumble" in txt_low or "trouble" in txt_low:
        return "NEGATIVE"
    if "record" in txt_low or "moon" in txt_low:
        return "POSITIVE"
    return random.choice(SENTIMENTS)

# def process_batch(df, epoch_id):
#     rows = df.collect()
#     logger.info(f"Processing batch {epoch_id} with {len(rows)} messages")
#     for row in rows:
#         message = row.asDict()
#         logger.info(f"Processed message: {json.dumps(message)}")

message_counts = defaultdict(int)

def log_raw_input(df, epoch_id, topic_name):
    count = df.count()
    message_counts[topic_name] += count
    logger.info(f"[{topic_name.upper()}] Batch {epoch_id}: {count} new messages")
    logger.info(f"[{topic_name.upper()}] Total messages consumed so far: {message_counts[topic_name]}")

def process_batch_named(df, epoch_id, topic_name):
    count = df.count()
    message_counts[topic_name] += count
    logger.info(f"[{topic_name.upper()}] Batch {epoch_id}: {count} new messages")
    logger.info(f"[{topic_name.upper()}] Total messages so far: {message_counts[topic_name]}")


# def stream(topic: str, schema: StructType):
#     logger.info(f"Starting stream for topic: {topic}")
#     return (
#         spark.readStream
#              .format("kafka")
#              .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"]["internal"])
#              .option("subscribe", topic)
#              .option("startingOffsets", "earliest")
#              .option("failOnDataLoss", "false")
#              .load()
#              .selectExpr("CAST(value AS STRING) AS json")
#              .select(from_json(col("json"), schema).alias("data"))
#              .select("data.*")
#     )

def stream(topic: str, schema: StructType):
    logger.info(f"Starting stream for topic: {topic}")
    raw_df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"]["internal"])
             .option("subscribe", topic)
             .option("startingOffsets", "earliest")
             .option("failOnDataLoss", "false")
             .load()
             .selectExpr("CAST(value AS STRING) AS json")
    )

    # Attach logging
    raw_df.writeStream \
        .foreachBatch(lambda df, eid: log_raw_input(df, eid, topic)) \
        .outputMode("append") \
        .option("checkpointLocation", os.path.join(checkpoint_dir, f"{topic}_raw_log")) \
        .start()

    return (
        raw_df.select(from_json(col("json"), schema).alias("data"))
              .select("data.*")
    )


# Initialize Kafka streams
logger.info("Initializing streams for Twitter, Reddit, and News...")
twitter = stream(cfg["kafka"]["topics"]["twitter"], schema_text)
# reddit = stream(cfg["kafka"]["topics"]["reddit"], schema_text)
# news = stream(cfg["kafka"]["topics"]["news"], schema_text)
prices = stream(cfg["kafka"]["topics"]["prices"], schema_price)

# For now, use Twitter only
texts = twitter
logger.info("Successfully unioned all text streams.")

scored = texts.withColumn("sentiment", dummy_score(col("text")))
logger.info("Applied sentiment scoring to text stream.")

# Console sink for live output
console_query = (scored
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", os.path.join(checkpoint_dir, "console"))
    .start())
logger.info("Started console output stream for debugging.")

# Kafka sink with logging
# write_query = (scored
#     .writeStream
#     .foreachBatch(process_batch)
#     .outputMode("append")
#     .format("kafka")
#     .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"]["internal"])
#     .option("topic", cfg["kafka"]["topics"]["sentiment_stream"])
#     .option("checkpointLocation", os.path.join(checkpoint_dir, "kafka"))
#     .start())

kafka_output = scored.select(to_json(struct("*")).alias("value"))

kafka_query = (kafka_output.writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"]["internal"])
    .option("topic", cfg["kafka"]["topics"]["sentiment_stream"])
    .option("checkpointLocation", os.path.join(checkpoint_dir, "kafka"))
    .start())
logger.info(f"Started writeStream to topic: {cfg['kafka']['topics']['sentiment_stream']}")

logger.info("All streams are now running. Waiting for termination...")
spark.streams.awaitAnyTermination()
