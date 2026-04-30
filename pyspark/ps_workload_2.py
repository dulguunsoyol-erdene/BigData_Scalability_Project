import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum

# Create Spark session
spark = SparkSession.builder \
    .appName("Subscription Filtering") \
    .getOrCreate()

# Read data
df = spark.read.csv("data/churn_seed_1_1_2M.csv", header=True, inferSchema=True)
# df = spark.read.csv("data/churn_seed_1_5_6M.csv", header=True, inferSchema=True)
# df = spark.read.csv("data/churn_seed_1_11_2M.csv", header=True, inferSchema=True)

### Workload 2: ACCURACY
# Test 2.1: Basic (Simple rules)
start = time.perf_counter()

df.withColumn("valid",
    (col("Monthly_Fee") >= 0) &
    (col("Age") >= 18)
).count()

end = time.perf_counter()
print("A1 Spark:", end - start)

# Test 2.2: Medium (Medium rules)
start = time.perf_counter()

df.withColumn("valid",
    (col("Monthly_Fee") >= 0) &
    (col("Customer_Tenure_Months") >= 0) &
    (col("Login_Frequency_Per_Month") >= 0)
).count()

end = time.perf_counter()
print("A2 Spark:", end - start)

# Test 2.3: Complex (Complex rules)
start = time.perf_counter()

df.withColumn("valid",
    (col("Monthly_Fee") >= 10) &
    (col("Avg_Monthly_Usage_Hours") <= 80) &
    ((col("Late_Payments_Count") >= 0) | (col("Billing_Cycle") == "Annual"))
).count()

end = time.perf_counter()
print("A3 Spark:", end - start)

# Stop Spark session
spark.stop()