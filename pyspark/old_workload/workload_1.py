import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("Subscription Filtering") \
    .getOrCreate()

# Read data
#df = spark.read.csv("data/1.2M_churn_data.csv", header=True, inferSchema=True)
#df = spark.read.csv("data/5.6M_churn_data.csv", header=True, inferSchema=True)
df = spark.read.csv("data/11.2M_churn_data.csv", header=True, inferSchema=True)

### Workload 1: SELECT and FILTERING Operations

# Test 1.1: Select Subscription_Type column
# start_time = time.perf_counter()

# subscription_col = df.select("Subscription_Type")

# end_time = time.perf_counter()
# print(f"Test 1.1 Time Taken: {end_time - start_time:.9f} seconds\n")



# Test 1.2: Filter Premium subscriptions
# start_time = time.perf_counter()

# premium_df = df.filter(col("Subscription_Type") == "Premium")

# end_time = time.perf_counter()
# print(f"Test 1.2 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 1.3: Filter Premium + Monthly billing
start_time = time.perf_counter()

premium_monthly_df = df.filter(
    (col("Subscription_Type") == "Premium") & 
    (col("Billing_Cycle") == "Monthly")
)

end_time = time.perf_counter()
print(f"Test 1.3 Time Taken: {end_time - start_time:.9f} seconds\n")