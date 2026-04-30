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

    ### Workload 1: COMPLETENESS
    # Test 1.1: Basic (few columns)
    start = time.perf_counter()

    missing_age = df["Age"].isnull().sum()
    missing_gender = df["Gender"].isnull().sum()

    end = time.perf_counter()
    print("C1 Pandas:", end - start)


    # Test 1.2: Medium (all columns + "None")
    start = time.perf_counter()

    invalid = (df == "None").sum().sum()

    end = time.perf_counter()

    print("C2 Pandas:", end - start)


    # Test 1.3: Complex (same 3 columns as Spark)
    start = time.perf_counter()

    invalid = (
        (df["Monthly_Fee"] > 0).sum() +
        (df["Age"] > 20).sum() +
        (df["Gender"] == "None").sum()
    )

    end = time.perf_counter()
    print("C3 Pandas:", end - start)