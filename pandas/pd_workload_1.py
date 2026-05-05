import pandas as pd
import time

files = [
    ("100MB", "data/1.2M_churn_data.csv"),
    ("512MB", "data/5.6M_churn_data.csv"),
    ("1GB",   "data/11.2M_churn_data.csv")
]

results = []

for label, path in files:
    print(f"\nProcessing {label} dataset: {path}")
    df = pd.read_csv(path)
    print("Shape:", df.shape)

    # Test 1.1
    start = time.perf_counter()
    missing_age    = df["Age"].isnull().sum()
    missing_gender = df["Gender"].isnull().sum()
    t1 = time.perf_counter() - start
    print(f"C1 Pandas: {t1:.6f}s")

    # Test 1.2 — type-aware null/"None" check to match PySpark
    start = time.perf_counter()
    total = 0
    for col in df.columns:
        if df[col].dtype == object:
            total += (df[col].isnull() | (df[col] == "None")).sum()
        else:
            total += df[col].isnull().sum()
    t2 = time.perf_counter() - start
    print(f"C2 Pandas: {t2:.6f}s")

    # Test 1.3
    start = time.perf_counter()
    invalid = (
        (df["Monthly_Fee"] > 0).sum() +
        (df["Age"] > 20).sum() +
        (df["Gender"] == "None").sum()
    )
    t3 = time.perf_counter() - start
    print(f"C3 Pandas: {t3:.6f}s")

    results.append({'size': label, 'test_1': t1, 'test_2': t2, 'test_3': t3,
                    'avg': (t1 + t2 + t3) / 3})

print("\n", pd.DataFrame(results))