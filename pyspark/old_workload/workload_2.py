import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize Spark
spark = SparkSession.builder.appName("GroupByTest").getOrCreate()

# Read data file
#df = spark.read.csv("data/1.2M_churn_data.csv", header=True, inferSchema=True)
#df = spark.read.csv("data/5.6M_churn_data.csv", header=True, inferSchema=True)
df = spark.read.csv("data/11.2M_churn_data.csv", header=True, inferSchema=True)

### Workload 2: GROUPBY Operations

# Test 2.1: Single-Key Grouping
# start_time = time.perf_counter()

# grouped_single = df.groupBy("Subscription_Type").count()
# grouped_single.show()

# end_time = time.perf_counter()
# print(f"Test 1 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 2.2: Basic Grouping + Single Aggregation
# start_time = time.perf_counter()

# grouped_agg = df.groupBy("Subscription_Type").agg(
#     avg("Monthly_Fee").alias("avg_monthly_fee")
# )
# grouped_agg.show()

# end_time = time.perf_counter()
# print(f"Test 2 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 2.3: Multi-Key Grouping
start_time = time.perf_counter()

grouped_multi = df.groupBy("Subscription_Type", "Billing_Cycle").count()
grouped_multi.show()

end_time = time.perf_counter()
print(f"Test 3 Time Taken: {end_time - start_time:.9f} seconds\n")


# Stop Spark session when done
spark.stop()