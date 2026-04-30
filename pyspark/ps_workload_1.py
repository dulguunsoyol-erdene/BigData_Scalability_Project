import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum
from torch import StringType

# Create Spark session
spark = SparkSession.builder \
    .appName("Subscription Filtering") \
    .getOrCreate()

# Read data
df = spark.read.csv("data/churn_seed_1_1_2M.csv", header=True, inferSchema=True)
# df = spark.read.csv("data/churn_seed_1_5_6M.csv", header=True, inferSchema=True)
# df = spark.read.csv("data/churn_seed_1_11_2M.csv", header=True, inferSchema=True)

### Workload 1: COMPLETENESS
# Test 1.1: Basic (few columns)
start = time.perf_counter()

df.select(
    spark_sum(when(col("Age").isNull(),1)),
    spark_sum(when(col("Gender").isNull(),1))
).collect()

end = time.perf_counter()
print("C1 Spark:", end - start)

# Test 1.2: Medium (all columns + "None")
start = time.perf_counter()

exprs = []
for field in df.schema.fields:
    c = field.name
    
    if isinstance(field.dataType, StringType):
        # String columns → check null OR "None"
        exprs.append(
            spark_sum(when(col(c).isNull() | (col(c) == "None"), 1)).alias(c)
        )
    else:
        # Numeric/date columns → only check null
        exprs.append(
            spark_sum(when(col(c).isNull(), 1)).alias(c)
        )

df.select(exprs).collect()

end = time.perf_counter()
print("C2 Spark:", end - start)


# Test 1.3: Complex (all columns + Mixed conditions)
start = time.perf_counter()

df.select(
    spark_sum(when(col("Monthly_Fee") > 0,1)),
    spark_sum(when(col("Age") > 20,1)),
    spark_sum(when(col("Gender") == "None",1))
).collect()

end = time.perf_counter()
print("C3 Spark:", end - start)

# Stop Spark session
spark.stop()
