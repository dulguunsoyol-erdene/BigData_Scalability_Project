import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, datediff, lit, avg

# Create Spark session
spark = SparkSession.builder \
    .appName("Subscription Filtering") \
    .getOrCreate()

# Read data
df = spark.read.csv("data/churn_seed_1_1_2M.csv", header=True, inferSchema=True)
# df = spark.read.csv("data/churn_seed_1_5_6M.csv", header=True, inferSchema=True)
# df = spark.read.csv("data/churn_seed_1_11_2M.csv", header=True, inferSchema=True)

### Workload 5: ACCURACY + TIMELINESS
# Test 5.1: Basic (few columns)
start = time.perf_counter()

df.withColumn("score",
    (col("Monthly_Fee") >= 0) &
    (col("Customer_Tenure_Months") >= 0)
).count()

end = time.perf_counter()
print("TA1 Spark:", end - start)

# Test 5.2: Medium (Medium rules)
start = time.perf_counter()

df.withColumn("score",
    (col("Monthly_Fee") >= 10) &
    (col("Avg_Monthly_Usage_Hours") <= 80) &
    (col("Signup_Date") <= lit("2025-01-01"))
).count()

end = time.perf_counter()
print("TA2 Spark:", end - start)

# Test 5.3: Hard (full pipeline)
ref = df.select("Customer_ID") \
    .withColumn("Last_Update", lit("2025-01-01"))

start = time.perf_counter()

result = df.join(ref, "Customer_ID") \
    .withColumn("score",
        (col("Monthly_Fee") >= 10) &
        (col("Avg_Monthly_Usage_Hours") <= 80) &
        (col("Last_Update") >= col("Signup_Date"))
    ) \
    .select(avg(col("score").cast("double")).alias("score")) \
    .collect()

end = time.perf_counter()

print("TA3 Spark:", end - start)
# print("Score:", result[0]["score"])

# Stop Spark session
spark.stop()