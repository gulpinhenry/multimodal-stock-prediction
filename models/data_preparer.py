from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import yaml
import json

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
        
        pipeline = [
            {
                "$match": {
                    "data.type": "sentiment",
                    "timestamp": {
                        "$gte": (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "company": "$symbol",
                        "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}}
                    },
                    "avg_sentiment": {"$avg": "$data.sentiment_score"},
                    "count": {"$sum": 1},
                    "positive_ratio": {
                        "$avg": {
                            "$cond": [
                                {"$eq": ["$data.sentiment_label", "POSITIVE"]},
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            {"$sort": {"_id.date": 1}}
        ]
        
        return list(self.collection.aggregate(pipeline))
        logger.info("Sentiment aggregation completed.")

    def _get_price_data(self):
        """Get all price data with all required fields"""
        return list(self.db.your_collection.find(
            {"data.type": "price"},
            {
                "_id": 0,
                "symbol": 1,
                "timestamp": 1,
                "data.closing_price": 1,
                "data.opening_price": 1,
                "data.high": 1,
                "data.low": 1,
                "data.volume": 1
            }
        ))

    def prepare_training_data(self, lookback_days=30):
        """Main method to prepare combined dataset with all required columns"""
        # Get aggregated sentiment
        sentiment_data = self._aggregate_sentiment(lookback_days)
        sentiment_df = pd.DataFrame([
            {
                "date": item["_id"]["timestamp"],
                "company": item["_id"]["company"].lower(),  # Normalize to lowercase
                "avg_sentiment": item["avg_sentiment"],
                "sentiment_count": item["count"],
                "positive_ratio": item["positive_ratio"]
            }
            for item in sentiment_data
        ])
        
        # Get price data with all fields
        price_data = self._get_price_data()
        price_df = pd.DataFrame([
            {
                "date": item["timestamp"],
                "symbol": item["symbol"],
                "close": item["data"]["closing_price"],
                "open": item["data"]["opening_price"],
                "high": item["data"]["high"],
                "low": item["data"]["low"],
                "volume": item["data"]["volume"]
            }
            for item in price_data
        ])
        
        # Map company names to tickers in sentiment data
        sentiment_df["symbol"] = sentiment_df["company"].map(self.company_map)
        sentiment_df.dropna(subset=["symbol"], inplace=True)  # Remove unmapped companies
        
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
        merged_df = merged_df.groupby("symbol").apply(
            lambda group: group.ffill()
        ).reset_index(drop=True)
        
        # Select and order the columns we want
        final_df = merged_df[[
            "date", "symbol", "open", "high", "low", "close", "volume",
            "avg_sentiment"
        ]]
        
        return final_df