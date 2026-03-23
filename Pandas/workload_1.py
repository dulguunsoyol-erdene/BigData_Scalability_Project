import time
import pandas as pd

# Read data file
df = pd.read_csv("data/100K_sub_churn.csv")

### Workload 1: SELECT and FILTERING Operations
### Test 1.1: Select Subscription_Type column

# Record start time
start_time = time.time()

subscription_col = df['Subscription_Type']
print(subscription_col.head())

# Record end time
end_time = time.time()
print(f"Test 1.1 Time Taken: {end_time - start_time:.6f} seconds\n")


### Test 1.2: Filter Premium subscriptions
start_time = time.time()

premium_df = df[df['Subscription_Type'] == 'Premium']
print(premium_df.head())

end_time = time.time()
print(f"Test 1.2 Time Taken: {end_time - start_time:.6f} seconds\n")


### Test 1.3: Filter Premium + Monthly billing
start_time = time.time()

premium_monthly_df = df[
    (df['Subscription_Type'] == 'Premium') & 
    (df['Billing_Cycle'] == 'Monthly')
]
print(premium_monthly_df.head())

end_time = time.time()
print(f"Step 1.3 Time Taken: {end_time - start_time:.6f} seconds\n")
