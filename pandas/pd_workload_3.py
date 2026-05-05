import pandas as pd
import time

files = [
    ("100MB", "data/churn_seed_2_1_2M.csv"),
    ("512MB", "data/churn_seed_2_5_6M.csv"),
    ("1GB",   "data/churn_seed_2_11_2M.csv")
]

results = []

for label, path in files:
    print(f"\nProcessing {label} dataset: {path}")
    df = pd.read_csv(path)
    print("Shape:", df.shape)

    # Test 3.1
    start = time.perf_counter()
    timely = (df["Customer_Tenure_Months"] >= 0).mean()
    t1 = time.perf_counter() - start
    print(f"T1 Pandas: {t1:.6f}s")

    # Test 3.2 — date parsing inside timer to match PySpark
    start = time.perf_counter()
    timely = (
        (pd.to_datetime(df["Churn_Date"], errors="coerce").isna()) |
        (pd.to_datetime(df["Churn_Date"], errors="coerce") >=
         pd.to_datetime(df["Signup_Date"], errors="coerce"))
    ).mean()
    t2 = time.perf_counter() - start
    print(f"T2 Pandas: {t2:.6f}s")

    # Test 3.3
    ref = df[["Customer_ID"]].copy()
    ref["Last_Update"] = pd.Timestamp("2025-01-01")

    start = time.perf_counter()
    merged = df.merge(ref, on="Customer_ID")
    merged["timely"] = merged["Last_Update"] >= pd.to_datetime(
        merged["Signup_Date"], errors="coerce")
    t3 = time.perf_counter() - start
    print(f"T3 Pandas: {t3:.6f}s")

    results.append({'size': label, 'test_1': t1, 'test_2': t2, 'test_3': t3,
                    'avg': (t1 + t2 + t3) / 3})

print("\n", pd.DataFrame(results))