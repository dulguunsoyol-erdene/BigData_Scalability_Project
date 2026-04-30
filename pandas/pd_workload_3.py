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

    ### Workload 3: TIMELINESS
    # Test 3.1: Basic (few columns)
    start = time.perf_counter()

    timely = (df["Customer_Tenure_Months"] >= 0).mean()

    end = time.perf_counter()
    print("T1 Pandas:", end - start)

    # Test 3.2: Medium (all columns + "None")
    start = time.perf_counter()

    timely = (
        (df["Churn_Date"].isna()) |
        (df["Churn_Date"] >= df["Signup_Date"])
    ).mean()

    end = time.perf_counter()
    print("T2 Pandas:", end - start)

    # Test 3.3: Complex (all columns + Mixed conditions)
    ref = df[["Customer_ID"]].copy()
    ref["Last_Update"] = pd.Timestamp("2025-01-01")

    start = time.perf_counter()

    merged = df.merge(ref, on="Customer_ID")
    merged["timely"] = merged["Last_Update"] >= merged["Signup_Date"]

    end = time.perf_counter()
    print("T3 Pandas:", end - start)