import pandas as pd
import numpy as np
import time


files = [
    ("100MB", "data/churn_seed_2_1_2M.csv"),
    ("512MB", "data/churn_seed_2_5_6M.csv"),
    ("1GB", "data/churn_seed_2_11_2M.csv")
]

for label, path in files:
    print(f"\nProcessing {label} dataset: {path}")
    
    df = pd.read_csv(path)
    print("Shape:", df.shape)

    ### Workload 2: ACCURACY
    # Test 2.1: Basic (Simple rules)
    start = time.perf_counter()

    valid = (
        (df["Monthly_Fee"] >= 0) &
        (df["Age"] >= 18)
    ).mean()

    end = time.perf_counter()
    print("A1 Pandas:", end - start)

    # Test 2.2: Medium (All columns + "None")
    start = time.perf_counter()

    valid = (
        (df["Monthly_Fee"] >= 0) &
        (df["Customer_Tenure_Months"] >= 0) &
        (df["Login_Frequency_Per_Month"] >= 0)
    ).mean()

    end = time.perf_counter()
    print("A2 Pandas:", end - start)

    # Test 2.3: Complex (All columns + Mixed conditions)
    start = time.perf_counter()

    valid = (
        (df["Monthly_Fee"] >= 10) &
        (df["Avg_Monthly_Usage_Hours"] <= 80) &
        ((df["Late_Payments_Count"] >= 0) | (df["Billing_Cycle"]=="Annual"))
    ).mean()

    end = time.perf_counter()
    print("A3 Pandas:", end - start)
