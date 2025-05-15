import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import numpy as np # Added for NaN checks potentially

# Add the models directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models')))

from data_preparer import DataPreparer # This will import the updated DataPreparer

class TestDataPreparer(unittest.TestCase):

    # This helper now generates data as it would look in MongoDB after mongo_consumer.py processes it.
    def _generate_mock_mongo_price_docs(self, symbol, days=5):
        prices = []
        start_date = datetime(2023, 1, 1)
        for i in range(days):
            # mongo_consumer.py stores timestamp as string, often YYYY-MM-DD from price_producer or ISO if generated
            # DataPreparer expects to parse this. For price data, yfinance producer sends YYYY-MM-DD.
            event_timestamp = (start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
            prices.append({
                "symbol": symbol, # Top-level
                "timestamp": event_timestamp, # Top-level event timestamp
                "source_topic": "prices_raw",
                "kafka_timestamp_ms": (start_date + timedelta(days=i)).timestamp() * 1000,
                "data": { # Nested data field
                    "type": "price",
                    "closing_price": 100.0 + i,
                    "opening_price": 99.0 + i,
                    "high": 101.0 + i,
                    "low": 98.0 + i,
                    "volume": 1000000 + (i * 10000)
                }
            })
        return prices

    # This helper generates raw sentiment documents as they would look in MongoDB (consumed from sentiment_stream topic)
    def _generate_mock_mongo_sentiment_docs(self, company_name_in_message, days=5):
        sentiments = []
        start_date = datetime(2023, 1, 1)
        for i in range(days):
            event_timestamp = (start_date + timedelta(days=i)).isoformat() # sentiment_analyzer produces ISO format
            sentiments.append({
                "symbol": company_name_in_message, # mongo_consumer puts 'company' field here as symbol for sentiment_stream
                "timestamp": event_timestamp, # Top-level event timestamp
                "source_topic": "sentiment_scored",
                "kafka_timestamp_ms": (start_date + timedelta(days=i)).timestamp() * 1000,
                "data": { # Nested data field
                    "type": "sentiment",
                    "sentiment_score": 0.5 - (i * 0.1),
                    "sentiment_label": "POSITIVE" if (0.5 - (i * 0.1)) > 0 else ("NEUTRAL" if (0.5 - (i*0.1)) == 0 else "NEGATIVE"),
                    "text": "Some example text",
                    "id": f"sentiment_id_{i}"
                }
            })
        return sentiments
    
    @patch('data_preparer.MongoClient')
    @patch('data_preparer.datetime') # Patch the datetime module used within data_preparer.py
    def test_prepare_training_data_successful_merge(self, MockDPDateTime, MockMongoClient):
        # Configure the mocked datetime.utcnow() used by DataPreparer's _aggregate_sentiment
        # Mock data uses dates 2023-01-01 to 2023-01-05. lookback_days is 5.
        # Setting utcnow to 2023-01-06 ensures (utcnow - 5 days) = 2023-01-01, covering all mock data.
        mock_specific_utcnow_method = MagicMock(return_value=datetime(2023, 1, 6, 0, 0, 0))
        MockDPDateTime.utcnow = mock_specific_utcnow_method

        # Allow DataPreparer to still use other parts of datetime module if needed (e.g., if it constructed new datetimes)
        # The `data_preparer` imports `datetime` and `timedelta` from `datetime` directly.
        # We only need to control `datetime.utcnow()` from the `data_preparer`'s perspective.
        # For other attributes like `datetime.fromisoformat` or `datetime.strptime` or constructor `datetime(...)`:
        # if they were used as `data_preparer.datetime.fromisoformat`, this helps.
        original_datetime_class = datetime # Capture the real datetime class before it's fully mocked by @patch
        
        def side_effect_for_datetime_module(*args, **kwargs):
            if args or kwargs: # If called as a constructor datetime(Y,M,D)
                return original_datetime_class(*args, **kwargs)
            # If datetime.<something> is called, it's handled by attribute mocks like .utcnow
            # or falls through if not mocked. This part might be tricky for classmethods/staticmethods.
            return MockDPDateTime # Should return the mock object for attribute access
        
        MockDPDateTime.side_effect = side_effect_for_datetime_module
        MockDPDateTime.utcnow = mock_specific_utcnow_method # Re-assign after side_effect if side_effect overwrites it
        MockDPDateTime.fromisoformat = original_datetime_class.fromisoformat # Ensure this is available
        MockDPDateTime.strptime = original_datetime_class.strptime # Ensure this is available
        # Timedelta is imported directly in data_preparer.py, so it's not affected by patching 'data_preparer.datetime'

        mock_db_client = MagicMock()
        mock_collection = MagicMock()
        MockMongoClient.return_value = mock_db_client
        mock_db_client.__getitem__.return_value = mock_collection 
        mock_collection.__getitem__.return_value = mock_collection

        # Data as it would be stored by mongo_consumer.py
        # Price data uses 'MSFT' as symbol directly.
        # Sentiment data might use 'microsoft' (lowercase) as the symbol field if that comes from sentiment_analyzer's output 'company' field.
        mock_msft_price_docs = self._generate_mock_mongo_price_docs(symbol='MSFT', days=5)
        # Assuming sentiment analyzer outputs 'microsoft' as company, which mongo_consumer puts into 'symbol' field
        mock_msft_sentiment_docs_for_db = self._generate_mock_mongo_sentiment_docs(company_name_in_message='microsoft', days=5)

        def mock_find_side_effect(*args, **kwargs):
            query = args[0]
            if query.get("data.type") == "price":
                return mock_msft_price_docs
            return [] # Should not be called for sentiment by _get_price_data
        
        # _aggregate_sentiment is called on the raw documents from the DB.
        # So, this mock should simulate the aggregation pipeline on mock_msft_sentiment_docs_for_db.
        def mock_aggregate_side_effect(*args, **kwargs):
            # This mock should replicate the $group and $project logic of _aggregate_sentiment
            # based on the input `mock_msft_sentiment_docs_for_db`
            pipeline_args = args[0] # The pipeline descriptor
            # Simulate aggregation:
            aggregated_results = []
            daily_sentiments = {} # date_str -> list of scores, list of labels

            min_ts_str_from_pipeline = None
            for stage in pipeline_args:
                if "$match" in stage and "timestamp" in stage["$match"] and "$gte" in stage["$match"]["timestamp"]:
                    min_ts_str_from_pipeline = stage["$match"]["timestamp"]["$gte"]
                    break
            min_dt_from_pipeline = original_datetime_class.fromisoformat(min_ts_str_from_pipeline) if min_ts_str_from_pipeline else None

            for doc in mock_msft_sentiment_docs_for_db:
                if doc["data"]["type"] == "sentiment":
                    doc_event_dt = original_datetime_class.fromisoformat(doc["timestamp"])
                    if min_dt_from_pipeline and doc_event_dt < min_dt_from_pipeline:
                        continue # Skip docs outside lookback
                    
                    doc_date_str = doc_event_dt.strftime("%Y-%m-%d")
                    # Key for grouping in aggregation is (symbol, date_str)
                    # mongo_consumer stores what sentiment_analyzer provides as 'company' into the top-level 'symbol' field for sentiment docs.
                    group_key_symbol = doc["symbol"] 
                    if (group_key_symbol, doc_date_str) not in daily_sentiments:
                        daily_sentiments[(group_key_symbol, doc_date_str)] = {"scores": [], "labels": []}
                    daily_sentiments[(group_key_symbol, doc_date_str)]["scores"].append(doc["data"]["sentiment_score"])
                    daily_sentiments[(group_key_symbol, doc_date_str)]["labels"].append(doc["data"]["sentiment_label"])

            for (symbol_key, date_str), data in daily_sentiments.items():
                avg_sent = sum(data["scores"]) / len(data["scores"]) if data["scores"] else 0
                pos_ratio = sum(1 for label in data["labels"] if label == "POSITIVE") / len(data["labels"]) if data["labels"] else 0
                aggregated_results.append({
                    "_id": {"company": symbol_key, "date": date_str},
                    "avg_sentiment": avg_sent,
                    "count": len(data["scores"]),
                    "positive_ratio": pos_ratio
                })
            return sorted(aggregated_results, key=lambda x: x["_id"]["date"]) # ensure sorted like DB

        mock_collection.find.side_effect = mock_find_side_effect
        mock_collection.aggregate.side_effect = mock_aggregate_side_effect
        
        preparer = DataPreparer(mongo_client=mock_db_client)
        # DataPreparer uses self.company_map to map 'company' (from sentiment aggregation _id) to a ticker symbol.
        # Sentiment aggregation uses 'symbol' field from DB (which was 'microsoft' in this case).
        # So, company_map needs to map 'microsoft' (lowercase) to 'MSFT'.
        preparer.company_map = {'microsoft': 'MSFT'} 

        lookback_days = 5 # Should be consistent with generated data dates
        final_df = preparer.prepare_training_data(lookback_days=lookback_days)

        self.assertIsInstance(final_df, pd.DataFrame)
        self.assertFalse(final_df.empty, "DataFrame should not be empty with successful merge")
        
        print("--- Full Mocked DataFrame from test_prepare_training_data_successful_merge ---")
        print(final_df.to_string())
        print("-------------------------------------------------------------------------------")

        expected_columns = ["date", "symbol", "open", "high", "low", "close", "volume", "avg_sentiment"]
        self.assertListEqual(list(final_df.columns), expected_columns)
        
        self.assertEqual(len(final_df), 5, "Should have 5 days of merged data for MSFT")
        self.assertEqual(final_df['symbol'].iloc[0], 'MSFT')

        # Check date format (YYYY-MM-DD string)
        self.assertEqual(final_df['date'].iloc[0], (original_datetime_class(2023, 1, 1)).strftime("%Y-%m-%d"))

        self.assertFalse(final_df['avg_sentiment'].isnull().any(), "avg_sentiment should be filled")
        self.assertAlmostEqual(final_df['avg_sentiment'].iloc[0], 0.5, places=5) 
        self.assertAlmostEqual(final_df['close'].iloc[0], 100.0, places=5)

    @patch('data_preparer.MongoClient')
    @patch('data_preparer.datetime') # Patch for the second test as well
    def test_prepare_training_data_no_sentiment_data(self, MockDPDateTime, MockMongoClient):
        # Setup mock for utcnow for this test too, though less critical if aggregate returns empty
        MockDPDateTime.utcnow.return_value = datetime(2023, 1, 4, 0, 0, 0) # Example fixed date
        original_datetime_class = datetime
        MockDPDateTime.side_effect = lambda *args, **kwargs: original_datetime_class(*args, **kwargs)
        MockDPDateTime.fromisoformat = original_datetime_class.fromisoformat
        MockDPDateTime.strptime = original_datetime_class.strptime

        mock_db_client = MagicMock()
        mock_collection = MagicMock()
        MockMongoClient.return_value = mock_db_client
        mock_db_client.__getitem__.return_value = mock_collection
        mock_collection.__getitem__.return_value = mock_collection

        mock_msft_price_docs = self._generate_mock_mongo_price_docs(symbol='MSFT', days=3)

        mock_collection.find.return_value = mock_msft_price_docs
        mock_collection.aggregate.return_value = [] # No sentiment data from aggregation

        preparer = DataPreparer(mongo_client=mock_db_client)
        # company_map is used on sentiment_df. If sentiment_df is empty, this map is not critical.
        preparer.company_map = {'microsoft': 'MSFT'} 

        final_df = preparer.prepare_training_data(lookback_days=3)

        self.assertIsInstance(final_df, pd.DataFrame)
        self.assertFalse(final_df.empty, "DataFrame should contain price data")
        self.assertEqual(len(final_df), 3, "Should have 3 rows from price data")
        self.assertTrue(final_df['avg_sentiment'].isnull().all(), "avg_sentiment should be all NaN")
        self.assertEqual(final_df['symbol'].iloc[0], 'MSFT')


    @patch('data_preparer.MongoClient')
    @patch('data_preparer.datetime') # Patch for the third test
    def test_prepare_training_data_sentiment_data_some_missing_ffill(self, MockDPDateTime, MockMongoClient):
        MockDPDateTime.utcnow.return_value = datetime(2023, 1, 6, 0, 0, 0) # Align with data dates
        original_datetime_class = datetime
        MockDPDateTime.side_effect = lambda *args, **kwargs: original_datetime_class(*args, **kwargs)
        MockDPDateTime.fromisoformat = original_datetime_class.fromisoformat
        MockDPDateTime.strptime = original_datetime_class.strptime

        mock_db_client = MagicMock()
        mock_collection = MagicMock()
        MockMongoClient.return_value = mock_db_client
        mock_db_client.__getitem__.return_value = mock_collection
        mock_collection.__getitem__.return_value = mock_collection

        mock_msft_price_docs = self._generate_mock_mongo_price_docs(symbol='MSFT', days=5)
        
        # Aggregated sentiment results, as if _aggregate_sentiment produced this
        # Dates: 2023-01-01, 2023-01-02, 2023-01-03, 2023-01-04, 2023-01-05
        # Here, sentiment data for 2023-01-01 (0.8) and 2023-01-04 (0.2)
        # company field in _id from aggregation will be what was in 'symbol' field of mongo sentiment doc, e.g., 'microsoft'
        aggregated_sentiment_day1 = {
            "_id": {"company": "microsoft", "date": "2023-01-01"}, 
            "avg_sentiment": 0.8, "count": 5, "positive_ratio": 0.9
        }
        aggregated_sentiment_day4 = {
            "_id": {"company": "microsoft", "date": "2023-01-04"},
            "avg_sentiment": 0.2, "count": 5, "positive_ratio": 0.3
        }
        
        mock_collection.find.return_value = mock_msft_price_docs
        # This is the direct output of the aggregation pipeline
        mock_collection.aggregate.return_value = [aggregated_sentiment_day1, aggregated_sentiment_day4]

        preparer = DataPreparer(mongo_client=mock_db_client)
        preparer.company_map = {'microsoft': 'MSFT'} # Map 'microsoft' (from aggregation) to 'MSFT' (price symbol)

        final_df = preparer.prepare_training_data(lookback_days=5)
        
        self.assertFalse(final_df.empty)
        self.assertEqual(len(final_df), 5) 

        expected_sentiments = [0.8, 0.8, 0.8, 0.2, 0.2]
        if list(final_df['avg_sentiment']) != expected_sentiments:
            print("DEBUG: Mismatch in ffill test")
            print(final_df[['date', 'symbol', 'avg_sentiment']])
            
        for i in range(5):
            self.assertAlmostEqual(final_df['avg_sentiment'].iloc[i], expected_sentiments[i], msg=f"Sentiment mismatch at index {i}")
        self.assertEqual(final_df['symbol'].iloc[0], 'MSFT')


if __name__ == '__main__':
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    config_dir = os.path.join(os.path.dirname(__file__), '..', 'config')
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    # Ensure a minimal config.yaml for DataPreparer to load its MONGO_ constants for default constructor path
    # The actual mongo_client is mocked in tests, but DataPreparer class loads config on import.
    if not os.path.exists(os.path.join(config_dir, 'config.yaml')):
        with open(os.path.join(config_dir, 'config.yaml'), 'w') as f:
            f.write("""
mongodb:
  host: localhost
  port: 27017
  database: stock_sentiment_test 
  collection: test_collection_dp
kafka: # Add dummy kafka topics so DataPreparer doesn't fail on import if it tries to access cfg["kafka"] for some reason
  topics:
    twitter: none
    reddit: none
    news: none
    prices: none
    sentiment_stream: none
""")

    unittest.main() 