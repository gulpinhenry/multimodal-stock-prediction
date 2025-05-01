#!/usr/bin/env python
import json, yaml, logging, os
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/elastic_indexer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "..", "config", "config.yaml")
cfg = yaml.safe_load(open(config_path))

# Initialize Elasticsearch client with proper headers
es = Elasticsearch(
    cfg["elastic"]["hosts"],
    headers={"Accept": "application/json", "Content-Type": "application/json"},
    request_timeout=30
)
logger.info(f"Connected to Elasticsearch at {cfg['elastic']['hosts']}")

# Health check: Verify Elasticsearch is working
try:
    health = es.cluster.health()
    logger.info(f"Elasticsearch cluster health: {health['status']}")
    
    # Test write capability
    test_doc = {
        "test": "health_check",
        "timestamp": "now"
    }
    response = es.index(index=cfg["elastic"]["index"], document=test_doc)
    logger.info(f"Health check: Successfully wrote test document. ID: {response['_id']}")
    
    # Clean up test document
    es.delete(index=cfg["elastic"]["index"], id=response['_id'])
    logger.info("Health check: Cleaned up test document")
except Exception as e:
    logger.error(f"Health check failed: {str(e)}")
    raise

# Initialize Kafka consumer
cons = KafkaConsumer(
    cfg["kafka"]["topics"]["sentiment_stream"],
    bootstrap_servers=cfg["kafka"]["bootstrap_servers"]["external"],
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)
logger.info(f"Connected to Kafka and subscribed to topic: {cfg['kafka']['topics']['sentiment_stream']}")

logger.info("⇢ Shipping Kafka → Elasticsearch …")
for msg in cons:
    try:
        # Log the received message
        logger.info(f"Received message: {json.dumps(msg.value)}")
        
        # Index the document
        response = es.index(index=cfg["elastic"]["index"], document=msg.value)
        
        # Log successful indexing
        logger.info(f"Successfully indexed document. ID: {response['_id']}, Index: {response['_index']}")
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        logger.error(f"Failed message content: {json.dumps(msg.value)}")
