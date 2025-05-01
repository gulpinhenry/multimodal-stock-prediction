#!/usr/bin/env python
"""
Batch job that:
 1. Pulls last N hours of sentiment scores (Elasticsearch or files)
 2. Joins with OHLCV price series
 3. Builds features (rolling sentiment mean, etc.)
 4. Trains XGBoost or LSTM
 5. Saves model artifacts for live inference
"""
# TODO: implement data join, feature engineering, model training
