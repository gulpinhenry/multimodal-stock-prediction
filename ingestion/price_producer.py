#!/usr/bin/env python
import json, time, uuid, yaml, os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import socket
import logging
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/price_stream.log'),
        logging.StreamHandler()
    ]
)

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

# Fetch historical data (60 days)
end = datetime.now()
start = end - timedelta(days=60)
historical_data = {}

logging.info("Fetching historical data from yfinance...")

for symbol in watchlist:
    df = yf.download(symbol, start=start, end=end)
    if df.empty:
        logging.warning(f"No data for symbol: {symbol}")
        continue
    # historical_data[symbol] = df.reset_index()[["Date", "Close", "Open", "Volume"]]
    historical_data[symbol] = df
    logging.info(f"Fetched historical price data for company: {symbol}")
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
