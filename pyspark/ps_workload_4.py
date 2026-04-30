import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum, datediff, lit

# Create Spark session
spark = SparkSession.builder \
    .appName("Subscription Filtering") \
    .getOrCreate()

# Read data
df = spark.read.csv("data/churn_seed_1_1_2M.csv", header=True, inferSchema=True)
# df = spark.read.csv("data/churn_seed_1_5_6M.csv", header=True, inferSchema=True)
# df = spark.read.csv("data/churn_seed_1_11_2M.csv", header=True, inferSchema=True)

### Workload 4: TIMELINESS + COMPLETENESS
# Test 4.1: Basic (few columns)
start = time.perf_counter()

df.withColumn("score",
    col("Monthly_Fee").isNotNull() &
    (col("Customer_Tenure_Months") >= 0)
).count()

end = time.perf_counter()
print("TC1 Spark:", end - start)

# Test 4.2: Medium (Medium rules)
start = time.perf_counter()

df.withColumn("score",
    col("Monthly_Fee").isNotNull() &
    (col("Signup_Date") <= lit("2025-01-01"))
).count()

end = time.perf_counter()
print("TC2 Spark:", end - start)

# Test 4.3: Hard (join + checks)
ref = df.select("Customer_ID") \
    .withColumn("Last_Update", lit("2025-01-01"))

start = time.perf_counter()

df.join(ref, "Customer_ID") \
    .withColumn("score",
        col("Monthly_Fee").isNotNull() &
        (col("Last_Update") >= col("Signup_Date"))
    ).count()

end = time.perf_counter()
print("TC3 Spark:", end - start)

# Stop Spark session
spark.stop()