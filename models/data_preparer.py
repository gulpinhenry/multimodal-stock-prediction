from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import yaml
import json
import numpy as np

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

class DataPreparer:
    def __init__(self, mongo_client=None):
        if mongo_client is None:
            self.mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
        else:
            self.mongo_client = mongo_client
        self.db = self.mongo_client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]
        logger.info(f"MongoDB: Using database '{MONGO_DB}' and collection '{MONGO_COLLECTION}'")


        
        # Company to ticker mapping
        self.company_map = {
            'microsoft': 'MSFT',
            # 'apple': 'AAPL',
            # Add all your companies here
        }

    def _aggregate_sentiment(self, lookback_days=30):
        """Aggregate sentiment scores on-demand"""
        logger.info(f"Aggregating sentiment data for last {lookback_days} days")
        
        # The 'timestamp' field at the top level of MongoDB documents is expected to be an ISO 8601 string.
        # The DataPreparer previously used datetime.utcnow() for the $gte match, so we ensure consistency.
        # The documents from mongo_consumer now have a top-level 'timestamp' (event time).
        min_timestamp = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()

        pipeline = [
            {
                "$match": {
                    "data.type": "sentiment", # Match on nested data.type
                    "timestamp": { # Match on top-level timestamp (event time)
                        "$gte": min_timestamp 
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "company": "$symbol", # Symbol is top-level
                        "date": {"$dateToString": {"format": "%Y-%m-%d", "date": {"$toDate": "$timestamp"}}} # Date from top-level timestamp
                    },
                    "avg_sentiment": {"$avg": "$data.sentiment_score"}, # Access nested field
                    "count": {"$sum": 1},
                    "positive_ratio": {
                        "$avg": {
                            "$cond": [
                                {"$eq": ["$data.sentiment_label", "POSITIVE"]}, # Access nested field
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            {"$sort": {"_id.date": 1}}
        ]
        
        result = list(self.collection.aggregate(pipeline))
        logger.info(f"Sentiment aggregation completed. Found {len(result)} records.")
        return result

    def _get_price_data(self):
        """Get all price data with all required fields"""
        # Fetches documents where data.type is 'price'
        # Projects top-level symbol, timestamp, and nested data fields
        price_documents = list(self.collection.find(
            {"data.type": "price"}, # Match on nested data.type
            {
                "_id": 0,
                "symbol": 1, # Top-level symbol
                "timestamp": 1, # Top-level timestamp (event time)
                "data.closing_price": 1,
                "data.opening_price": 1,
                "data.high": 1,
                "data.low": 1,
                "data.volume": 1
            }
        ))
        logger.info(f"Price data retrieval found {len(price_documents)} records.")
        return price_documents

    def prepare_training_data(self, lookback_days=30):
        """Main method to prepare combined dataset with all required columns"""
        sentiment_data = self._aggregate_sentiment(lookback_days)
        
        if sentiment_data:
            sentiment_df = pd.DataFrame([
                {
                    "date": item["_id"]["date"], # This is already YYYY-MM-DD from aggregation
                    "company": item["_id"]["company"].lower(),
                    "avg_sentiment": item["avg_sentiment"],
                    "sentiment_count": item["count"],
                    "positive_ratio": item["positive_ratio"]
                }
                for item in sentiment_data
            ])
            sentiment_df["symbol"] = sentiment_df["company"].map(self.company_map)
            sentiment_df.dropna(subset=["symbol"], inplace=True)
        else:
            sentiment_df = pd.DataFrame(columns=["date", "company", "avg_sentiment", "sentiment_count", "positive_ratio", "symbol"])

        price_data_from_db = self._get_price_data()
        if price_data_from_db:
            price_df = pd.DataFrame([
                {
                    # top-level timestamp is event time, e.g., 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS...'
                    # Ensure it's consistently formatted as YYYY-MM-DD for merging
                    "date": str(item["timestamp"]).split('T')[0],
                    "symbol": item["symbol"],
                    "close": item["data"].get("closing_price"), # Use .get() for safety
                    "open": item["data"].get("opening_price"),
                    "high": item["data"].get("high"),
                    "low": item["data"].get("low"),
                    "volume": item["data"].get("volume")
                }
                for item in price_data_from_db if "data" in item # Ensure item["data"] exists
            ])
        else:
            price_df = pd.DataFrame(columns=["date", "symbol", "close", "open", "high", "low", "volume"])
        
        # Merge data on date and symbol
        merged_df = pd.merge(
            price_df,
            sentiment_df,
            on=["date", "symbol"],
            how="left"  # Keep all price data even if no sentiment
        )
        
        # Sort by symbol and date
        merged_df.sort_values(["symbol", "date"], inplace=True)
        
        # Calculate daily returns
        merged_df["daily_return"] = merged_df.groupby("symbol")["close"].pct_change()

        # Fill missing sentiment data (forward fill within each symbol)
        # Ensure essential columns for ffill exist even if sentiment_df was empty
        for col in ["avg_sentiment", "sentiment_count", "positive_ratio"]:
            if col not in merged_df.columns:
                merged_df[col] = np.nan
                
        merged_df = merged_df.groupby("symbol").apply(
            lambda group: group.ffill().infer_objects(copy=False),
            include_groups=False
        ).reset_index(drop=True)
        
        # Select and order the columns we want
        # Ensure all selected columns exist, filling with NaN if necessary
        final_columns = [
            "date", "symbol", "open", "high", "low", "close", "volume",
            "avg_sentiment"
        ]
        for col in final_columns:
            if col not in merged_df.columns:
                merged_df[col] = np.nan # Or some other appropriate default

        final_df = merged_df[final_columns]
        
        return final_df