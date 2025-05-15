#!/usr/bin/env python
import json
import logging
import os
import socket
import time
import yaml
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/mongo_consumer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "..", "config", "config.yaml")
cfg = yaml.safe_load(open(config_path))
logger.info("Configuration loaded successfully.")

KAFKA_BROKER_INTERNAL = cfg["kafka"]["bootstrap_servers"]["internal"]
KAFKA_BROKER_EXTERNAL = cfg["kafka"]["bootstrap_servers"]["external"]
TOPICS_TO_CONSUME = [
    cfg["kafka"]["topics"]["twitter"],
    cfg["kafka"]["topics"]["reddit"],
    cfg["kafka"]["topics"]["news"],
    cfg["kafka"]["topics"]["prices"],
    cfg["kafka"]["topics"]["sentiment_stream"],
]

MONGO_HOST = cfg["mongodb"]["host"]
MONGO_PORT = cfg["mongodb"]["port"]
MONGO_DB = cfg["mongodb"]["database"]
MONGO_COLLECTION = cfg["mongodb"]["collection"]

def pick_bootstrap_server():
    try:
        socket.gethostbyname("kafka")  # Check if 'kafka' hostname (Docker internal) resolves
        logger.info(f"Using internal Kafka bootstrap server: {KAFKA_BROKER_INTERNAL}")
        return KAFKA_BROKER_INTERNAL
    except socket.gaierror:
        logger.info(f"Using external Kafka bootstrap server: {KAFKA_BROKER_EXTERNAL}")
        return KAFKA_BROKER_EXTERNAL

def get_kafka_consumer(bootstrap_servers, topics):
    for _ in range(5): # Retry connection
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                consumer_timeout_ms=1000, # Stop iteration if no message for 1s
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"Successfully connected to Kafka and subscribed to topics: {topics}")
            return consumer
        except NoBrokersAvailable:
            logger.warning("No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)
    logger.error("Failed to connect to Kafka after multiple retries.")
    return None

def get_mongo_client(host, port):
    try:
        client = MongoClient(host, port)
        client.admin.command('ping') # Verify connection
        logger.info(f"Successfully connected to MongoDB at {host}:{port}")
        return client
    except ConnectionFailure:
        logger.error(f"Failed to connect to MongoDB at {host}:{port}")
        return None

def transform_data(message, topic):
    # Default structure - will be mostly replaced but good for initial thought
    base_transformed = {
        "source_topic": topic,
        "kafka_timestamp_ms": message.timestamp, # Renamed for clarity
        # "original_message": message.value # We will embed specific parts into data field
    }

    msg_value = message.value
    if not isinstance(msg_value, dict):
        logger.warning(f"Message value is not a dict: {msg_value}, for topic {topic}")
        # For non-dict messages, we can insert them with a generic type or skip
        base_transformed["symbol"] = msg_value.get("symbol") if isinstance(msg_value, dict) else None
        base_transformed["data"] = {
            "type": "unknown",
            "payload": msg_value
        }
        base_transformed["timestamp"] = datetime.utcnow().isoformat() # Add a processing timestamp
        return base_transformed

    # Initialize common fields
    transformed = {
        "source_topic": topic,
        "kafka_timestamp_ms": message.timestamp,
        "symbol": None, # Will be populated based on topic
        "timestamp": None, # This will be the main event timestamp for DataPreparer
        "data": {} # This is the nested structure DataPreparer expects
    }

    # Add specific fields based on topic heuristics
    if topic == cfg["kafka"]["topics"]["prices"]:
        transformed["symbol"] = msg_value.get("symbol")
        transformed["timestamp"] = msg_value.get("timestamp") # This is the price event timestamp
        transformed["data"] = {
            "type": "price",
            "closing_price": msg_value.get("closing_price"),
            "opening_price": msg_value.get("opening_price"),
            "high": msg_value.get("high"),
            "low": msg_value.get("low"),
            "volume": msg_value.get("volume")
            # timestamp is already at the top level
        }
    elif topic == cfg["kafka"]["topics"]["sentiment_stream"]:
        # sentiment_stream topic messages are from sentiment_analyzer.py
        # Structure is like: {'id': 'some_id', 'text': 'text', 'sentiment_label': 'NEUTRAL', 'sentiment_score': 0.0, 'company': 'microsoft', 'timestamp': 'YYYY-MM-DDTHH:MM:SS.ffffff'}
        transformed["symbol"] = msg_value.get("company") # This will be mapped to ticker by DataPreparer if needed
        transformed["timestamp"] = msg_value.get("timestamp") # Event timestamp from sentiment analyzer
        transformed["data"] = {
            "type": "sentiment",
            "text": msg_value.get("text"),
            "sentiment_label": msg_value.get("sentiment_label"),
            "sentiment_score": msg_value.get("sentiment_score"),
            "id": msg_value.get("id")
            # timestamp is already at the top level
        }
    elif topic in [cfg["kafka"]["topics"]["twitter"], cfg["kafka"]["topics"]["reddit"], cfg["kafka"]["topics"]["news"]]:
        # These are raw social data, may or may not be directly used by DataPreparer in current form
        # but we can store them with a type for potential future use or other consumers.
        # The sentiment_analyzer.py consumes these and produces to sentiment_stream.
        transformed["symbol"] = msg_value.get("company") # Heuristic
        transformed["timestamp"] = msg_value.get("timestamp") or datetime.utcnow().isoformat() # Event timestamp or processing time
        transformed["data"] = {
            "type": topic, # e.g., "twitter_raw"
            "text": msg_value.get("text"),
            "id": msg_value.get("id")
            # any other relevant fields from original_message for these types
        }
    else: # Generic handling for other topics
        logger.warning(f"Unhandled topic in transform_data: {topic}")
        transformed["symbol"] = msg_value.get("symbol") or msg_value.get("company")
        transformed["timestamp"] = msg_value.get("timestamp") or datetime.utcnow().isoformat()
        transformed["data"] = {
            "type": "unknown_topic",
            "payload": msg_value
        }
    
    # Ensure timestamp is always present, default to UTC now if not set by topic logic
    if transformed["timestamp"] is None:
        logger.warning(f"Timestamp not found in message for topic {topic}, symbol {transformed['symbol']}. Defaulting to current UTC time.")
        transformed["timestamp"] = datetime.utcnow().isoformat()

    return transformed

def main():
    logger.info("Starting MongoDB consumer...")
    
    bootstrap_servers = pick_bootstrap_server()
    kafka_consumer = get_kafka_consumer(bootstrap_servers, TOPICS_TO_CONSUME)
    mongo_client = get_mongo_client(MONGO_HOST, MONGO_PORT)

    if not kafka_consumer or not mongo_client:
        logger.error("Exiting due to failed Kafka or MongoDB connection.")
        return

    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    logger.info(f"MongoDB: Using database '{MONGO_DB}' and collection '{MONGO_COLLECTION}'")

    try:
        while True:
            for message in kafka_consumer:
                logger.info(f"Received message from topic {message.topic}: {message.value}")
                transformed_message = transform_data(message, message.topic)
                try:
                    collection.insert_one(transformed_message)
                    logger.info(f"Inserted message into MongoDB: {transformed_message.get('symbol')}, {transformed_message.get('source_topic')}")
                except Exception as e:
                    logger.error(f"Failed to insert message into MongoDB: {e}")
            # If consumer_timeout_ms is reached, the loop will break.
            # We can add a small sleep here if we want to continuously poll even with no messages.
            # For now, if no messages for 1s, it will re-check the outer while True loop.
            # This helps in graceful shutdown if needed.
            # time.sleep(0.1) # Optional: to reduce CPU usage if continuously polling empty topics

    except KeyboardInterrupt:
        logger.info("MongoDB consumer process interrupted by user.")
    finally:
        if kafka_consumer:
            kafka_consumer.close()
            logger.info("Kafka consumer closed.")
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB client closed.")
        logger.info("MongoDB consumer shut down.")

if __name__ == "__main__":
    main() 