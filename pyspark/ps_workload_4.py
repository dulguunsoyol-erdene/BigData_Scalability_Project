import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit, to_date

spark = SparkSession.builder.appName("Timeliness+Completeness Benchmark").getOrCreate()

files = [
    ("100MB", "data/churn_seed_2_1_2M.csv"),
    ("512MB", "data/churn_seed_2_5_6M.csv"),
    ("1GB",   "data/churn_seed_2_11_2M.csv")
]

results = []

for label, path in files:
    print(f"\nProcessing {label} dataset: {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Test 4.1
    start = time.perf_counter()
    df.select(avg((
        col("Monthly_Fee").isNotNull() &
        (col("Customer_Tenure_Months") >= 0)
    ).cast("int"))).collect()
    t1 = time.perf_counter() - start
    print(f"TC1 Spark: {t1:.6f}s")

    # Test 4.2
    start = time.perf_counter()
    df.select(avg((
        col("Monthly_Fee").isNotNull() &
        (to_date(col("Signup_Date")) <= to_date(lit("2025-01-01")))
    ).cast("int"))).collect()
    t2 = time.perf_counter() - start
    print(f"TC2 Spark: {t2:.6f}s")

    # Test 4.3
    ref = df.select("Customer_ID").withColumn("Last_Update", lit("2025-01-01"))

    start = time.perf_counter()
    df.join(ref, "Customer_ID") \
      .select(avg((
          col("Monthly_Fee").isNotNull() &
          (to_date(col("Last_Update")) >= to_date(col("Signup_Date")))
      ).cast("int"))).collect()
    t3 = time.perf_counter() - start
    print(f"TC3 Spark: {t3:.6f}s")

    results.append({'size': label, 'test_1': t1, 'test_2': t2, 'test_3': t3,
                    'avg': (t1 + t2 + t3) / 3})

print("\n", pd.DataFrame(results))
spark.stop()