import time
import pandas as pd

# Read data file
#df = pd.read_csv("data/1.2M_churn_data.csv") #100MB dataset
#df = pd.read_csv("data/5.6M_churn_data.csv") #512MB dataset
df = pd.read_csv("data/11.2M_churn_data.csv") #1GB dataset

### Workload 1: SELECT and FILTERING Operations

# Test 1.1: Select Subscription_Type column
# Record start time
# start_time = time.perf_counter()

# subscription_col = df['Subscription_Type']
# print(subscription_col.head())

# # Record end time
# end_time = time.perf_counter()
# print(f"Test 1 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 1.2: Filter Premium subscriptions
# start_time = time.perf_counter()

# premium_df = df[df['Subscription_Type'] == 'Premium']
# print(premium_df['Subscription_Type'].head())

# end_time = time.perf_counter()
# print(f"Test 2 Time Taken: {end_time - start_time:.9f} seconds\n")


# Test 1.3: Filter Premium + Monthly billing
# start_time = time.perf_counter()

# premium_monthly_df = df[
#     (df['Subscription_Type'] == 'Premium') & 
#     (df['Billing_Cycle'] == 'Monthly')
# ]
# print(premium_monthly_df[['Subscription_Type', 'Billing_Cycle']].head())

# end_time = time.perf_counter()
# print(f"Test 3 Time Taken: {end_time - start_time:.9f} seconds\n")
