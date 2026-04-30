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

### Workload 3: TIMELINESS
# Test 3.1: Basic (few columns)

start = time.perf_counter()

df.withColumn("timely",
    col("Customer_Tenure_Months") >= 0
).count()

end = time.perf_counter()
print("T1 Spark:", end - start)

# Test 3.2: Data logic (Medium rules)
start = time.perf_counter()

df.withColumn("timely",
    col("Churn_Date").isNull() |
    (col("Churn_Date") >= col("Signup_Date"))
).count()

end = time.perf_counter()
print("T2 Spark:", end - start)

# Test 3.3: Join based
ref = df.select("Customer_ID") \
    .withColumn("Last_Update", lit("2025-01-01"))

start = time.perf_counter()

df.join(ref, "Customer_ID") \
    .withColumn("timely",
        col("Last_Update") >= col("Signup_Date")
    ).count()

end = time.perf_counter()
print("T3 Spark:", end - start)

# Stop Spark session
spark.stop()