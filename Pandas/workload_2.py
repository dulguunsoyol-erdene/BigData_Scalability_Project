import time
import pandas as pd

# Read data file
df = pd.read_csv("data/100K_sub_churn.csv")

### Workload 2: GROUPBY Operations

# Test 2.1: Single-Key Grouping (Subscription_Type)
start_time = time.perf_counter()

grouped_single = df.groupby('Subscription_Type')
print(grouped_single.size())  # count per group

end_time = time.perf_counter()
print(f"Step 1 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 2.2: Basic Grouping + Single Aggregation (Average Monthly Fee per Subscription_Type)
start_time = time.perf_counter()

grouped_agg = df.groupby('Subscription_Type')['Monthly_Fee'].mean()
print(grouped_agg)

end_time = time.perf_counter()
print(f"Step 2 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 2.3: Multi-Key Grouping (Subscription_Type + Billing_Cycle)
start_time = time.perf_counter()

grouped_multi = df.groupby(['Subscription_Type', 'Billing_Cycle']).size()
print(grouped_multi)

end_time = time.perf_counter()
print(f"Step 3 Time Taken: {end_time - start_time:.9f} seconds\n")
