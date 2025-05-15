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
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/sentiment_analyzer.log'),
        logging.StreamHandler()
    ]
)
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
        logger.info("Initializing FinBERT sentiment analysis model...")
        self.model = pipeline("text-classification", model="ProsusAI/finbert")
        logger.info("Model loaded successfully")
        
        self.mongo_client = get_mongo_client(MONGO_HOST, MONGO_PORT)
        if not self.mongo_client:
            raise Exception("Failed to connect to MongoDB")
            
        self.db = self.mongo_client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]
        logger.info(f"Connected to MongoDB collection: {MONGO_COLLECTION}")
        
    def analyze_and_update(self):
        # Get unprocessed texts from social media sources
        query = {
            "data.type": {"$in": ["twitter_raw", "reddit_raw", "news_raw"]},
            "data.sentiment_score": {"$exists": False}  # Only unprocessed
        }
        
        try:
            cursor = self.collection.find(query)
            count = 0
            
            for doc in cursor:
                text = doc["data"]["text"]
                try:
                    # Analyze sentiment
                    result = self.model(text)[0]
                    
                    # Update document with sentiment
                    update = {
                        "$set": {
                            "data.type": "sentiment",  # Change type to sentiment
                            "data.sentiment_score": result["score"],
                            "data.sentiment_label": result["label"],
                            "data.original_text": text,  # Keep original text
                            "data.processed_at": datetime.utcnow().isoformat()
                        }
                    }
                    
                    self.collection.update_one({"_id": doc["_id"]}, update)
                    count += 1
                    logger.info(f"Processed sentiment for {doc['symbol']}: {result['label']} ({result['score']:.2f})")
                    
                except Exception as e:
                    logger.error(f"Error processing document {doc['_id']}: {str(e)}")
                    continue
                    
            if count > 0:
                logger.info(f"Processed {count} documents")
            else:
                logger.debug("No new documents to process")
                
        except Exception as e:
            logger.error(f"Error in analyze_and_update: {str(e)}")
            
    def run(self, interval=60):
        """Run the analyzer continuously with a specified interval"""
        logger.info(f"Starting sentiment analyzer service (interval: {interval}s)")
        while True:
            try:
                self.analyze_and_update()
                time.sleep(interval)
            except KeyboardInterrupt:
                logger.info("Sentiment analyzer service stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                time.sleep(interval)  # Still sleep on error to avoid tight loop

if __name__ == "__main__":
    try:
        analyzer = SentimentAnalyzer()
        analyzer.run()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise