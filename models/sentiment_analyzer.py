from transformers import pipeline
from pymongo import MongoClient
import json
import logging
import os
import socket
import time
import yaml
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Initialize logging and MongoDB connection (similar to your consumer)
logger = logging.getLogger(__name__)

# Load configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "..", "config", "config.yaml")
cfg = yaml.safe_load(open(config_path))
logger.info("Configuration loaded successfully.")

MONGO_HOST = cfg["mongodb"]["host"]
MONGO_PORT = cfg["mongodb"]["port"]
MONGO_DB = cfg["mongodb"]["database"]
MONGO_COLLECTION = cfg["mongodb"]["collection"]

def get_mongo_client(host, port):
    try:
        client = MongoClient(host, port)
        client.admin.command('ping') # Verify connection
        logger.info(f"Successfully connected to MongoDB at {host}:{port}")
        return client
    except ConnectionFailure:
        logger.error(f"Failed to connect to MongoDB at {host}:{port}")
        return None

class SentimentAnalyzer:
    def __init__(self):
        self.model = pipeline("text-classification", model="ProsusAI/finbert")
        self.mongo_client = get_mongo_client(MONGO_HOST, MONGO_PORT)
        self.db = self.mongo_client[MONGO_DB]
        
    def analyze_and_update(self):
        # Get unprocessed texts
        collection = self.db[MONGO_COLLECTION]
        logger.info(f"MongoDB: Using database '{MONGO_DB}' and collection '{MONGO_COLLECTION}'")
        query = {
            "data.type": "sentiment",
            "data.sentiment_score": {"$exists": False}  # Only unprocessed
        }
        
        for doc in collection.find(query):
            text = doc["data"]["text"]
            try:
                result = self.model(text)[0]
                update = {
                    "$set": {
                        "data.sentiment_score": result["score"],
                        "data.sentiment_label": result["label"],
                        "processed": True
                    }
                }
                collection.update_one({"_id": doc["_id"]}, update)
                logger.info(f"Processed sentiment for {doc['symbol']}")
            except Exception as e:
                logger.error(f"Error processing {doc['_id']}: {str(e)}")

if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    analyzer.analyze_and_update()