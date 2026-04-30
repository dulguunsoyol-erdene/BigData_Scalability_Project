import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg, stddev, max as _max

# Initialize Spark
spark = SparkSession.builder.appName("AggregationTest").getOrCreate()

# Read data file
#df = spark.read.csv("data/1.2M_churn_data.csv", header=True, inferSchema=True)
#df = spark.read.csv("data/5.6M_churn_data.csv", header=True, inferSchema=True)
df = spark.read.csv("data/11.2M_churn_data.csv", header=True, inferSchema=True)

### Workload 3: AGGREGATION Operations


# Test 3.1: Simple Aggregation (SUM)
# start_time = time.perf_counter()

# total_fee = df.agg(_sum("Monthly_Fee").alias("total_fee"))
# total_fee.show()

# end_time = time.perf_counter()
# print(f"Test 1 Time Taken: {end_time - start_time:.9f} seconds\n")



# Test 3.2: Multiple Aggregates (Mean and Std)
# start_time = time.perf_counter()

# multi_agg = df.agg(
#     avg("Monthly_Fee").alias("mean"),
#     stddev("Monthly_Fee").alias("std")
# )
# multi_agg.show()

# end_time = time.perf_counter()
# print(f"Test 2 Time Taken: {end_time - start_time:.9f} seconds\n")



# Test 3.3: Named Aggregation (GroupBy + Multiple Metrics)
start_time = time.perf_counter()

named_agg = df.groupBy("Subscription_Type").agg(
    avg("Monthly_Fee").alias("avg_fee"),
    _sum("Avg_Monthly_Usage_Hours").alias("total_usage"),
    _max("Login_Frequency_Per_Month").alias("max_logins")
)
named_agg.show()

end_time = time.perf_counter()
print(f"Test 3 Time Taken: {end_time - start_time:.9f} seconds\n")


# Stop Spark session
spark.stop()