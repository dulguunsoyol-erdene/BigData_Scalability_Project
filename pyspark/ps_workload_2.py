import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("Accuracy Benchmark").getOrCreate()

files = [
    ("100MB", "data/churn_seed_2_1_2M.csv"),
    ("512MB", "data/churn_seed_2_5_6M.csv"),
    ("1GB",   "data/churn_seed_2_11_2M.csv")
]

results = []

for label, path in files:
    print(f"\nProcessing {label} dataset: {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Test 2.1
    start = time.perf_counter()
    df.select(avg((
        (col("Monthly_Fee") >= 0) &
        (col("Age") >= 18)
    ).cast("int"))).collect()
    t1 = time.perf_counter() - start
    print(f"A1 Spark: {t1:.6f}s")

    # Test 2.2
    start = time.perf_counter()
    df.select(avg((
        (col("Monthly_Fee") >= 0) &
        (col("Customer_Tenure_Months") >= 0) &
        (col("Login_Frequency_Per_Month") >= 0)
    ).cast("int"))).collect()
    t2 = time.perf_counter() - start
    print(f"A2 Spark: {t2:.6f}s")

    # Test 2.3
    start = time.perf_counter()
    df.select(avg((
        (col("Monthly_Fee") >= 10) &
        (col("Avg_Monthly_Usage_Hours") <= 80) &
        ((col("Late_Payments_Count") >= 0) | (col("Billing_Cycle") == "Annual"))
    ).cast("int"))).collect()
    t3 = time.perf_counter() - start
    print(f"A3 Spark: {t3:.6f}s")

    results.append({'size': label, 'test_1': t1, 'test_2': t2, 'test_3': t3,
                    'avg': (t1 + t2 + t3) / 3})

print("\n", pd.DataFrame(results))
spark.stop()