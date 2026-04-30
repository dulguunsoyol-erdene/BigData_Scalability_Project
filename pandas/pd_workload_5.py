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

    ### Workload 5: ACCURACY + TIMELINESS
    # Test 5.1: Basic (few columns)
    start = time.perf_counter()

    score = (
        (df["Monthly_Fee"] >= 0) &
        (df["Customer_Tenure_Months"] >= 0)
    ).mean()

    end = time.perf_counter()
    print("TA1 Pandas:", end - start)

    # Test 5.2: Medium (all columns + "None")
    df["Signup_Date_dt"] = pd.to_datetime(df["Signup_Date"], errors="coerce")

    start = time.perf_counter()

    score = (
        (df["Monthly_Fee"] >= 10) &
        (df["Avg_Monthly_Usage_Hours"] <= 80) &
        (df["Signup_Date_dt"] <= pd.Timestamp("2025-01-01"))
    ).mean()

    end = time.perf_counter()
    print("TA2 Pandas:", end - start)

    # Test 5.3: Complex (full pipeline)
    ref = df[["Customer_ID"]].copy()
    ref["Last_Update"] = pd.Timestamp("2025-01-01")

    start = time.perf_counter()

    merged = df.merge(ref, on="Customer_ID")

    score = (
        (merged["Monthly_Fee"] >= 10) &
        (merged["Avg_Monthly_Usage_Hours"] <= 80) &
        (merged["Last_Update"] >= merged["Signup_Date"])
    ).mean()

    end = time.perf_counter()
    print("TA3 Pandas:", end - start)