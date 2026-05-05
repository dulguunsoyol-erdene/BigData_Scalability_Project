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

    # Test 2.1
    start = time.perf_counter()
    valid = (
        (df["Monthly_Fee"] >= 0) &
        (df["Age"] >= 18)
    ).mean()
    t1 = time.perf_counter() - start
    print(f"A1 Pandas: {t1:.6f}s")

    # Test 2.2
    start = time.perf_counter()
    valid = (
        (df["Monthly_Fee"] >= 0) &
        (df["Customer_Tenure_Months"] >= 0) &
        (df["Login_Frequency_Per_Month"] >= 0)
    ).mean()
    t2 = time.perf_counter() - start
    print(f"A2 Pandas: {t2:.6f}s")

    # Test 2.3
    start = time.perf_counter()
    valid = (
        (df["Monthly_Fee"] >= 10) &
        (df["Avg_Monthly_Usage_Hours"] <= 80) &
        ((df["Late_Payments_Count"] >= 0) | (df["Billing_Cycle"] == "Annual"))
    ).mean()
    t3 = time.perf_counter() - start
    print(f"A3 Pandas: {t3:.6f}s")

    results.append({'size': label, 'test_1': t1, 'test_2': t2, 'test_3': t3,
                    'avg': (t1 + t2 + t3) / 3})

print("\n", pd.DataFrame(results))