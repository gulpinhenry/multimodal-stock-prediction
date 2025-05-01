#!/usr/bin/env python
import logging
import json, random, yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StringType, DoubleType

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

cfg = yaml.safe_load(open("config/config.yaml"))
logger.info("Configuration loaded successfully.")

spark = (SparkSession.builder.appName(cfg["spark"]["app_name"])
         .master(cfg["spark"]["master"])
         .config("spark.jars.packages", cfg["spark"]["kafka_packages"])
         .getOrCreate())
logger.info("Spark session started.")

schema_text = StructType() \
    .add("id", StringType()) \
    .add("text", StringType()) \
    .add("ts", DoubleType())

schema_price = StructType() \
    .add("id", StringType()) \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("ts", DoubleType())

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

def stream(topic: str, schema: StructType):
    logger.info("Starting stream for topic: %s", topic)
    return (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"]["internal"])
             .option("subscribe", topic)
             # ▼ new lines are here, before .load()
             .option("startingOffsets", "earliest")
             .option("failOnDataLoss", "false")
             .load()
             .selectExpr("CAST(value AS STRING) AS json")
             .select(from_json(col("json"), schema).alias("data"))
             .select("data.*")
    )

twitter = stream(cfg["kafka"]["topics"]["twitter"], schema_text)
reddit  = stream(cfg["kafka"]["topics"]["reddit"],  schema_text)
news    = stream(cfg["kafka"]["topics"]["news"],    schema_text)

texts  = twitter.unionByName(reddit).unionByName(news)
logger.info("Unioned all text streams.")

scored = texts.withColumn("sentiment", dummy_score(col("text")))
logger.info("Applied dummy_score on text stream.")

write_query = (scored.selectExpr("to_json(struct(*)) AS value")
       .writeStream.format("kafka")
       .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
       .option("topic", cfg["kafka"]["topics"]["sentiment_stream"])
       .option("checkpointLocation", "checkpoints/sentiment")
       .start())
logger.info("Started writeStream to topic: %s", cfg["kafka"]["topics"]["sentiment_stream"])

spark.streams.awaitAnyTermination()
