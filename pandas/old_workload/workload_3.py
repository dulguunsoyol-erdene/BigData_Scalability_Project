import time
import pandas as pd

# Read data file
#df = pd.read_csv("data/1.2M_churn_data.csv") #100MB dataset
#df = pd.read_csv("data/5.6M_churn_data.csv") #512MB dataset
df = pd.read_csv("data/11.2M_churn_data.csv") #1GB dataset

### Workload 3: AGGREGATION Operations

# Test 3.1: Simple Aggregation (Total Monthly Fee)
# start_time = time.perf_counter()

# total_fee = df['Monthly_Fee'].sum()
# print("Total Monthly Fee:", total_fee)

# end_time = time.perf_counter()
# print(f"Test 1 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 3.2: Applying Multiple Standard Aggregates (Mean and Std of Monthly Fee)
# start_time = time.perf_counter()

# multi_agg = df['Monthly_Fee'].agg(['mean', 'std'])
# print(multi_agg)

# end_time = time.perf_counter()
# print(f"Test 2 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 3.3: Named Aggregation in tuples (Group by Subscription_Type with custom column names)
# start_time = time.perf_counter()

# named_agg = df.groupby('Subscription_Type').agg(
#     avg_fee=('Monthly_Fee', 'mean'),
#     total_usage=('Avg_Monthly_Usage_Hours', 'sum'),
#     max_logins=('Login_Frequency_Per_Month', 'max')
# )
# print(named_agg)

# end_time = time.perf_counter()
# print(f"Test 3 Time Taken: {end_time - start_time:.9f} seconds\n")
