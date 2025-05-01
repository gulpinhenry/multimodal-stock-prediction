#!/usr/bin/env python
import json, random, time, uuid, yaml, os
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import socket
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/news_stream.log'),
        logging.StreamHandler()
    ]
)

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "..", "config", "config.yaml")
cfg = yaml.safe_load(open(config_path))
fake = Faker()

def pick_bootstrap():
    # Simple heuristic: the Docker DNS name 'kafka' only resolves in-container
    try:
        socket.gethostbyname("kafka")
        return cfg["kafka"]["bootstrap_servers"]["internal"]
    except socket.gaierror:
        return cfg["kafka"]["bootstrap_servers"]["external"]
    

def lazy_producer():
    start_time = time.time()
    while time.time() - start_time < 10:  # Try for 10 seconds
        try:
            return KafkaProducer(
                bootstrap_servers=pick_bootstrap(),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            time.sleep(1)
    raise Exception("Failed to connect to Kafka after 10 seconds")

producer = lazy_producer()
HEADLINES = [
    "{tick} posts record quarterly revenue",
    "{tick} faces regulatory scrutiny over new product",
    "Market reacts to {tick} buy-back announcement",
    "{tick} partners with top AI startup to boost R&D",
    "{tick} shares tumble amid supply-chain fears"
]

print("⇢ Dummy News producer running …")
while True:
    tick = random.choice(cfg["symbols"]["watchlist"])
    headline = random.choice(HEADLINES).format(tick=tick)
    message = {
        "id": str(uuid.uuid4()),
        "text": headline,
        "ts": time.time()
    }
    producer.send(cfg["kafka"]["topics"]["news"], message)
    logging.info(f"News: {json.dumps(message)}")
    time.sleep(1)
