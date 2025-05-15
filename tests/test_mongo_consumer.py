import unittest
from unittest.mock import patch, MagicMock, ANY
import json
import time
import socket
from ingestion import mongo_consumer # Assuming mongo_consumer.py is in the ingestion directory

# Sample configuration for testing
TEST_CONFIG = {
    "kafka": {
        "bootstrap_servers": {
            "internal": "kafka:29092",
            "external": "localhost:9092"
        },
        "topics": {
            "twitter": "test_twitter_raw",
            "reddit": "test_reddit_raw",
            "news": "test_news_raw",
            "prices": "test_prices_raw",
            "sentiment_stream": "test_sentiment_scored"
        }
    },
    "mongodb": {
        "host": "localhost",
        "port": 27017,
        "database": "test_stock_data",
        "collection": "test_stream_data"
    }
}

class MockKafkaMessage:
    def __init__(self, topic, value, timestamp):
        self.topic = topic
        self.value = value
        self.timestamp = timestamp

class TestMongoConsumer(unittest.TestCase):

    @patch('ingestion.mongo_consumer.open', MagicMock()) # Mock open to prevent file access
    @patch('ingestion.mongo_consumer.yaml.safe_load')
    def setUp(self, mock_yaml_safe_load):
        # Patch yaml.safe_load to return TEST_CONFIG for the entire module's scope during tests
        mock_yaml_safe_load.return_value = TEST_CONFIG
        
        # Directly update mongo_consumer.cfg for functions that use it directly
        mongo_consumer.cfg = TEST_CONFIG 
        
        # Update module-level constants that are derived from cfg at import time
        # This is important if any functions use these constants instead of cfg directly.
        mongo_consumer.KAFKA_BROKER_INTERNAL = TEST_CONFIG["kafka"]["bootstrap_servers"]["internal"]
        mongo_consumer.KAFKA_BROKER_EXTERNAL = TEST_CONFIG["kafka"]["bootstrap_servers"]["external"]
        mongo_consumer.TOPICS_TO_CONSUME = [
            TEST_CONFIG["kafka"]["topics"]["twitter"],
            TEST_CONFIG["kafka"]["topics"]["reddit"],
            TEST_CONFIG["kafka"]["topics"]["news"],
            TEST_CONFIG["kafka"]["topics"]["prices"],
            TEST_CONFIG["kafka"]["topics"]["sentiment_stream"],
        ]
        mongo_consumer.MONGO_HOST = TEST_CONFIG["mongodb"]["host"]
        mongo_consumer.MONGO_PORT = TEST_CONFIG["mongodb"]["port"]
        mongo_consumer.MONGO_DB = TEST_CONFIG["mongodb"]["database"]
        mongo_consumer.MONGO_COLLECTION = TEST_CONFIG["mongodb"]["collection"]
        
        self.mock_kafka_consumer = MagicMock()
        self.mock_mongo_client = MagicMock()  # Returned by get_mongo_client
        self.mock_db_object = MagicMock()     # Returned by client[db_name]
        self.mock_mongo_collection = MagicMock() # Returned by db[collection_name]

        # Configure the chain of __getitem__ calls
        self.mock_mongo_client.__getitem__.return_value = self.mock_db_object
        self.mock_db_object.__getitem__.return_value = self.mock_mongo_collection
        

    @patch('ingestion.mongo_consumer.socket.gethostbyname')
    def test_pick_bootstrap_server_internal(self, mock_gethostbyname):
        mock_gethostbyname.return_value = "some_ip_for_kafka_internal" # Simulate Docker DNS resolution
        self.assertEqual(mongo_consumer.pick_bootstrap_server(), TEST_CONFIG["kafka"]["bootstrap_servers"]["internal"])

    @patch('ingestion.mongo_consumer.socket.gethostbyname', side_effect=socket.gaierror)
    def test_pick_bootstrap_server_external(self, mock_gethostbyname):
        self.assertEqual(mongo_consumer.pick_bootstrap_server(), TEST_CONFIG["kafka"]["bootstrap_servers"]["external"])

    @patch('ingestion.mongo_consumer.KafkaConsumer')
    def test_get_kafka_consumer_success(self, mock_kafka_consumer_class):
        mock_kafka_consumer_class.return_value = self.mock_kafka_consumer
        consumer = mongo_consumer.get_kafka_consumer("localhost:9092", ["test_topic"])
        self.assertEqual(consumer, self.mock_kafka_consumer)
        mock_kafka_consumer_class.assert_called_once_with(
            "test_topic",
            bootstrap_servers="localhost:9092",
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
            value_deserializer=ANY
        )

    @patch('ingestion.mongo_consumer.KafkaConsumer', side_effect=mongo_consumer.NoBrokersAvailable)
    @patch('ingestion.mongo_consumer.time.sleep') # Mock sleep to speed up test
    def test_get_kafka_consumer_failure(self, mock_sleep, mock_kafka_consumer_class):
        consumer = mongo_consumer.get_kafka_consumer("localhost:9092", ["test_topic"])
        self.assertIsNone(consumer)
        self.assertEqual(mock_kafka_consumer_class.call_count, 5) # Check retry logic

    @patch('ingestion.mongo_consumer.MongoClient')
    def test_get_mongo_client_success(self, mock_mongo_client_class):
        # Mock the client and its admin.command method
        mock_client_instance = MagicMock()
        mock_client_instance.admin.command.return_value = {"ok": 1} # Simulate successful ping
        mock_mongo_client_class.return_value = mock_client_instance
        
        client = mongo_consumer.get_mongo_client("localhost", 27017)
        self.assertEqual(client, mock_client_instance)
        mock_mongo_client_class.assert_called_once_with("localhost", 27017)
        client.admin.command.assert_called_once_with('ping')

    @patch('ingestion.mongo_consumer.MongoClient', side_effect=mongo_consumer.ConnectionFailure)
    def test_get_mongo_client_failure(self, mock_mongo_client_class):
        client = mongo_consumer.get_mongo_client("localhost", 27017)
        self.assertIsNone(client)

    def test_transform_data_prices(self):
        topic = TEST_CONFIG["kafka"]["topics"]["prices"]
        raw_message = {
            "symbol": "MSFT", "closing_price": 150.0, "opening_price": 149.0, 
            "high": 151.0, "low": 148.0, "volume": 1000000, "timestamp": "2023-01-01"
        }
        kafka_msg = MockKafkaMessage(topic, raw_message, time.time() * 1000)
        transformed = mongo_consumer.transform_data(kafka_msg, topic)
        self.assertEqual(transformed["source_topic"], topic)
        self.assertEqual(transformed["symbol"], "MSFT")
        self.assertEqual(transformed["price_data"]["closing_price"], 150.0)
        self.assertEqual(transformed["original_message"], raw_message)

    def test_transform_data_sentiment(self):
        topic = TEST_CONFIG["kafka"]["topics"]["sentiment_stream"]
        raw_message = {"company": "TSLA", "text": "Stock good", "sentiment": "POSITIVE", "id": "s1", "timestamp": "2023-01-01T10:00:00Z"}
        kafka_msg = MockKafkaMessage(topic, raw_message, time.time() * 1000)
        transformed = mongo_consumer.transform_data(kafka_msg, topic)
        self.assertEqual(transformed["source_topic"], topic)
        self.assertEqual(transformed["symbol"], "TSLA")
        self.assertEqual(transformed["sentiment_data"]["sentiment"], "POSITIVE")

    def test_transform_data_social(self):
        topic = TEST_CONFIG["kafka"]["topics"]["twitter"]
        raw_message = {"company": "AMZN", "text": "Big news!", "id": "t1", "timestamp": "2023-01-01T11:00:00Z"}
        kafka_msg = MockKafkaMessage(topic, raw_message, time.time() * 1000)
        transformed = mongo_consumer.transform_data(kafka_msg, topic)
        self.assertEqual(transformed["source_topic"], topic)
        self.assertEqual(transformed["symbol"], "AMZN")
        self.assertEqual(transformed["social_data"]["text"], "Big news!")

    def test_transform_data_unknown_format(self):
        topic = "unknown_topic"
        raw_message = "This is a string, not a dict"
        kafka_msg = MockKafkaMessage(topic, raw_message, time.time() * 1000)
        transformed = mongo_consumer.transform_data(kafka_msg, topic)
        self.assertEqual(transformed["source_topic"], topic)
        self.assertEqual(transformed["original_message"], raw_message)
        self.assertNotIn("symbol", transformed) # Or check if it's None based on implementation

    @patch('ingestion.mongo_consumer.get_kafka_consumer')
    @patch('ingestion.mongo_consumer.get_mongo_client')
    def test_main_flow_inserts_data(self, mock_get_mongo_client, mock_get_kafka_consumer):
        # mongo_consumer.cfg is already patched by setUp
        mock_get_kafka_consumer.return_value = self.mock_kafka_consumer
        mock_get_mongo_client.return_value = self.mock_mongo_client # This sets up the chain for collection access

        kafka_messages = [
            MockKafkaMessage(TEST_CONFIG["kafka"]["topics"]["prices"], {"symbol": "MSFT", "closing_price": 150.0, "timestamp": "t1"}, 12345),
            MockKafkaMessage(TEST_CONFIG["kafka"]["topics"]["twitter"], {"company": "TSLA", "text": "Hello", "timestamp": "t2"}, 12346)
        ]
        
        def side_effect_for_iter(*args, **kwargs):
            yield from kafka_messages
            raise KeyboardInterrupt 
        self.mock_kafka_consumer.__iter__.side_effect = side_effect_for_iter
        
        mongo_consumer.main()

        self.assertEqual(self.mock_mongo_collection.insert_one.call_count, 2)
        args_call_1 = self.mock_mongo_collection.insert_one.call_args_list[0][0][0]
        self.assertEqual(args_call_1["symbol"], "MSFT")
        self.assertEqual(args_call_1["source_topic"], TEST_CONFIG["kafka"]["topics"]["prices"])
        args_call_2 = self.mock_mongo_collection.insert_one.call_args_list[1][0][0]
        self.assertEqual(args_call_2["symbol"], "TSLA")
        self.assertEqual(args_call_2["source_topic"], TEST_CONFIG["kafka"]["topics"]["twitter"])

        mock_get_kafka_consumer.assert_called_once()
        mock_get_mongo_client.assert_called_once()
        self.mock_kafka_consumer.close.assert_called_once()
        self.mock_mongo_client.close.assert_called_once()

    @patch('ingestion.mongo_consumer.get_kafka_consumer', return_value=None)
    @patch('ingestion.mongo_consumer.get_mongo_client')
    def test_main_kafka_connection_failure(self, mock_get_mongo_client, mock_get_kafka_consumer):
        mongo_consumer.main()
        mock_get_mongo_client.assert_called_once() 
        self.mock_mongo_collection.insert_one.assert_not_called()


if __name__ == "__main__":
    unittest.main() 