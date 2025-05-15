# inference.py
import torch
import pandas as pd
import os
import logging
from data_preparer import DataPreparer
from model_training import StockSentimentModel, TemporalStockDataset

logger = logging.getLogger(__name__)

class StockPredictor:
    def __init__(self, model_path=None):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")
        
        self.preparer = DataPreparer()
        
        # Default model path if none provided
        if model_path is None:
            model_path = os.path.join(os.path.dirname(__file__), "stock_sentiment_model.pth")
        
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model file not found at {model_path}")
            
        self.model = self._load_model(model_path)
        
    def _load_model(self, path):
        # Initialize model with correct parameters
        model = StockSentimentModel(
            input_size=6,  # Adjust based on your features
            hidden_size=64,
            num_layers=2,
            output_size=1
        ).to(self.device)
        model.load_state_dict(torch.load(path))
        model.eval()
        return model
    
    def predict_next_day(self, symbol, lookback_days=30):
        # Get fresh data
        df = self.preparer.prepare_training_data(lookback_days)
        
        if df.empty:
            raise ValueError(f"No data found for symbol {symbol} in the last {lookback_days} days")
            
        # Filter for specific symbol
        if symbol not in df['symbol'].unique():
            raise ValueError(f"Symbol {symbol} not found in the data")
            
        symbol_df = df[df['symbol'] == symbol].copy()
        symbol_df['date'] = pd.to_datetime(symbol_df['date'])
        
        if len(symbol_df) < lookback_days:
            raise ValueError(f"Insufficient data points for {symbol}. Found {len(symbol_df)}, need {lookback_days}")
        
        # Create dataset (reusing your existing class)
        dataset = TemporalStockDataset(
            symbol_df,
            sequence_length=lookback_days,
            feature_scaler=None,  # You should save/load your scalers
            target_scaler=None,
            mode='inference'
        )
        
        # Get the most recent sequence
        last_sequence = dataset[-1][0].unsqueeze(0)  # Add batch dimension
        
        # Make prediction
        with torch.no_grad():
            prediction = self.model(last_sequence)
            prediction = dataset.target_scaler.inverse_transform(
                prediction.cpu().numpy()
            )
        
        return prediction[0][0]

if __name__ == "__main__":
    try:
        symbol = "MSFT"
        model_path = os.path.join(os.path.dirname(__file__), f"{symbol}_stock_sentiment_model.pth")
        predictor = StockPredictor(model_path)
        prediction = predictor.predict_next_day(symbol)
        print(f"Predicted next day price for {symbol}: {prediction}")
    except Exception as e:
        logger.error(f"Error during inference: {str(e)}")
        raise