from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, rand, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import yfinance as yf
import matplotlib.pyplot as plt
import pandas as pd

# Design Choice 1: Use Spark MLlib for scalability
#                 (GBT for temporal patterns instead of LSTM)

# Initialize Spark
spark = SparkSession.builder \
    .appName("StockPrediction") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# =====================
# Data Ingestion & Prep
# =====================

# Design Choice 2: Maintain yfinance for data quality but process in Spark
ticker = "NVDA"
data = yf.download(ticker, period="10000d", interval="1d")
pdf = data.reset_index()
pdf.columns = [c[0].lower() for c in pdf.columns]

# Convert to Spark DataFrame with proper date handling
df = spark.createDataFrame(pdf) \
    .withColumn("date", col("date").cast("timestamp")) \
    .withColumn("average_sentiment", (rand() * 2 - 1))  # Simulated sentiment

# Design Choice 3: Explicit window ordering for temporal integrity
window_spec = Window.orderBy("date")

df = df.withColumn("target_price", lag("open", -1).over(window_spec)) \
       .na.drop() \
       .withColumn("row_id", row_number().over(window_spec))

# =====================
# Temporal Split
# =====================

# Design Choice 4: Row-based split for true temporal validation
total_rows = df.count()
train_end = int(0.7 * total_rows)
val_end = int(0.85 * total_rows)

train = df.filter(col("row_id") <= train_end)
val = df.filter((col("row_id") > train_end) & (col("row_id") <= val_end))
test = df.filter(col("row_id") > val_end)

# =====================
# Feature Engineering
# =====================

# Design Choice 5: Simple feature set to demonstrate pipeline
feature_cols = ['open', 'high', 'low', 'close', 'volume', 'average_sentiment']

# Design Choice 6: Spark ML Pipeline for production readiness
pipeline = Pipeline(stages=[
    VectorAssembler(inputCols=feature_cols, outputCol="features"),
    MinMaxScaler(inputCol="features", outputCol="scaled_features"),
    GBTRegressor(
        featuresCol="scaled_features",
        labelCol="target_price",
        maxIter=100,
        maxDepth=5,
        stepSize=0.05,
        seed=42
    )
])

# =====================
# Training & Evaluation
# =====================

# Fit on train, validate on validation set
model = pipeline.fit(train)
val_pred = model.transform(val)

# Design Choice 7: Custom metrics for financial context
def calculate_metrics(pred_df):
    pred_pdf = pred_df.select("prediction", "target_price").toPandas()
    actual = pred_pdf['target_price']
    pred = pred_pdf['prediction']
    
    mse = ((actual - pred) ** 2).mean()
    mae = abs(actual - pred).mean()
    mape = (abs((actual - pred)/actual)).mean() * 100
    
    return mse, mae, mape

val_mse, val_mae, val_mape = calculate_metrics(val_pred)
print(f"Validation MSE: {val_mse:.4f}")
print(f"Validation MAE: {val_mae:.4f}")
print(f"Validation MAPE: {val_mape:.2f}%")

# =====================
# Final Testing & Viz
# =====================

test_pred = model.transform(test)
test_mse, test_mae, test_mape = calculate_metrics(test_pred)
print(f"\nTest MSE: {test_mse:.4f}")
print(f"Test MAE: {test_mae:.4f}")
print(f"Test MAPE: {test_mape:.2f}%")

# Design Choice 8: Use Arrow for efficient Spark->Pandas conversion
test_viz = test_pred.select("date", "open", "target_price", "prediction").toPandas()

plt.figure(figsize=(15, 6))
plt.plot(test_viz['date'], test_viz['target_price'], label='Actual')
plt.plot(test_viz['date'], test_viz['prediction'], label='Predicted', alpha=0.7)
plt.title(f"{ticker} Price Predictions")
plt.xlabel("Date")
plt.ylabel("Price")
plt.legend()
plt.show()

# Stop Spark session
spark.stop()