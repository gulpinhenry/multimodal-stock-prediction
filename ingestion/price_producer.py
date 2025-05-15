#!/usr/bin/env python
import json, time, uuid, yaml, os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import socket
import logging
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/price_stream.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(script_dir, "..", "config", "config.yaml")
cfg = yaml.safe_load(open(config_path))

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
# Start each ticker at an arbitrary base price
watchlist = cfg["symbols"]["watchlist"]

def fetch_with_retry(symbol, max_retries=5, initial_delay=2):
    """Fetch data with exponential backoff and retry logic"""
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            # Add jitter to avoid thundering herd
            jitter = random.uniform(0, 0.1 * delay)
            time.sleep(delay + jitter)
            
            data = yf.download(symbol, period="1d", interval="1m")
            if not data.empty:
                return data
            logger.warning(f"No data returned for {symbol} on attempt {attempt + 1}")
            
        except Exception as e:
            if "Too Many Requests" in str(e):
                logger.warning(f"Rate limit hit for {symbol} on attempt {attempt + 1}, waiting {delay:.1f}s")
                delay *= 2  # Exponential backoff
            else:
                logger.error(f"Error fetching {symbol}: {str(e)}")
                return None
                
    logger.error(f"Failed to fetch {symbol} after {max_retries} attempts")
    return None

# Fetch historical data (60 days)
end = datetime.now()
start = end - timedelta(days=60)
historical_data = {}

logging.info("Fetching historical data from yfinance...")

for symbol in watchlist:
    data = fetch_with_retry(symbol)
    if data is not None and not data.empty:
        historical_data[symbol] = data
        logging.info(f"Fetched historical price data for company: {symbol}")
    else:
        logging.warning(f"No data for symbol: {symbol}")
    time.sleep(1)

logging.info("⇢ Price producer streaming real historical data…")

for i in range(60):  # Assuming 60 data points per symbol (one per day)
    for symbol, df in historical_data.items():
        if i >= len(df):
            continue
        row = df.iloc[i]
        message = {
            # "id": str(uuid.uuid4()),
            "symbol": symbol,
            "closing_price": round(float(row["Close"]), 2),
            "opening_price": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "volume": round(float(row["Volume"]), 2),
            "timestamp": row.name.strftime("%Y-%m-%d")
        }
        producer.send(cfg["kafka"]["topics"]["prices"], message)
        logging.info(f"Sent price: {json.dumps(message)}")
    time.sleep(1)
