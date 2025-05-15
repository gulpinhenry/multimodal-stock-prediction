# inference.py
import torch
from data_preparer import DataPreparer
from model_training import StockSentimentModel, TemporalStockDataset

class StockPredictor:
    def __init__(self, model_path="stock_sentiment_model_temporal.pth"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.preparer = DataPreparer()
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
        
        # Filter for specific symbol
        symbol_df = df.xs(symbol, axis=1, level=1).reset_index()
        symbol_df['date'] = pd.to_datetime(symbol_df['date'])
        
        # Create dataset (reusing your existing class)
        dataset = TemporalStockDataset(
            symbol_df,
            sequence_length=30,
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
    symbol = "MSFT"
    model_path = f"{symbol}_stock_sentiment_model.pth"
    predictor = StockPredictor(model_path)
    print(f"Predicted next day price for {symbol}: {predictor.predict_next_day(f'{symbol}')}")