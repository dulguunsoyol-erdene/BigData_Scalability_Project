import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Completeness Benchmark").getOrCreate()

files = [
    ("100MB", "data/1.2M_churn_data.csv"),
    ("512MB", "data/5.6M_churn_data.csv"),
    ("1GB",   "data/11.2M_churn_data.csv")
]

results = []

for label, path in files:
    print(f"\nProcessing {label} dataset: {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Test 1.1
    start = time.perf_counter()
    df.select(
        spark_sum(when(col("Age").isNull(), 1)),
        spark_sum(when(col("Gender").isNull(), 1))
    ).collect()
    t1 = time.perf_counter() - start
    print(f"C1 Spark: {t1:.6f}s")

    # Test 1.2 — type-aware null/"None" check to match Pandas
    start = time.perf_counter()
    exprs = []
    for field in df.schema.fields:
        c = field.name
        if isinstance(field.dataType, StringType):
            exprs.append(spark_sum(when(col(c).isNull() | (col(c) == "None"), 1)).alias(c))
        else:
            exprs.append(spark_sum(when(col(c).isNull(), 1)).alias(c))
    df.select(exprs).collect()
    t2 = time.perf_counter() - start
    print(f"C2 Spark: {t2:.6f}s")

    # Test 1.3
    start = time.perf_counter()
    df.select(
        spark_sum(when(col("Monthly_Fee") > 0, 1)),
        spark_sum(when(col("Age") > 20, 1)),
        spark_sum(when(col("Gender") == "None", 1))
    ).collect()
    t3 = time.perf_counter() - start
    print(f"C3 Spark: {t3:.6f}s")

    results.append({'size': label, 'test_1': t1, 'test_2': t2, 'test_3': t3,
                    'avg': (t1 + t2 + t3) / 3})

import pandas as pd
print("\n", pd.DataFrame(results))
spark.stop()