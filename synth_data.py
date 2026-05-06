import pandas as pd
import numpy as np
import os

# ---- SET SEED HERE ----
n_seed = 3   # change this manually (1, 2, 3, ..., 30)

np.random.seed(n_seed)

# Dataset sizes
sizes = {
    "1_2M": 1_200_000,
    "5_6M": 5_600_000,
    "11_2M": 11_200_000
}

# Output folder
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# Static date setup
start_date = np.datetime64('2018-01-01')
end_date = np.datetime64('2025-01-01')
date_range_days = (end_date - start_date).astype(int)
today = np.datetime64('2025-01-01')

# ---- MAIN LOOP (ONLY OVER SIZES) ----
for label, n in sizes.items():
    print(f"Generating dataset for seed {n_seed}: {label} ({n:,} rows)")

    signup_dates = start_date + np.random.randint(0, date_range_days, n).astype('timedelta64[D]')

    df = pd.DataFrame({
        "Customer_ID": np.arange(1, n + 1),
        "Age": np.random.randint(18, 70, n),
        "Gender": np.random.choice(["Male","Female","Other","None"], n, p=[0.46,0.46,0.04,0.04]),
        "Location": np.random.choice(["USA","Canada","UK","Australia","Germany","None"], n, p=[0.4,0.2,0.1,0.1,0.1,0.1]),
        "Signup_Date": signup_dates,
        "Subscription_Type": np.random.choice(["Basic","Standard","Premium"], n, p=[0.4,0.35,0.25]),
        "Billing_Cycle": np.random.choice(["Monthly","Annual"], n, p=[0.75,0.25]),
        "Monthly_Fee": np.round(np.random.normal(50,15,n).clip(10,120),2),
        "Avg_Monthly_Usage_Hours": np.round(np.random.normal(20,10,n).clip(0,80),1),
        "Login_Frequency_Per_Month": np.random.poisson(15,n),
        "Support_Tickets_Count": np.random.poisson(1.5,n),
        "Payment_Method": np.random.choice(["Credit Card","Debit Card","PayPal","Bank Transfer"], n),
        "Late_Payments_Count": np.random.poisson(0.5,n),
        "Discount_Applied": np.random.choice([0,1], n, p=[0.7,0.3])
    })

    # Feature engineering
    df["Customer_Tenure_Months"] = ((today - df["Signup_Date"]).astype(int)/30).astype(int)

    churn_prob = (
        0.15
        + 0.002 * df["Support_Tickets_Count"]
        + 0.003 * df["Late_Payments_Count"]
        - 0.002 * df["Avg_Monthly_Usage_Hours"]
        - 0.05 * (df["Billing_Cycle"] == "Annual").astype(int)
    )

    churn_prob = np.clip(churn_prob, 0.05, 0.8)

    df["Churn_Status"] = np.random.binomial(1, churn_prob)

    churn_offsets = np.random.randint(30, 900, n).astype('timedelta64[D]')

    df["Churn_Date"] = np.where(
        df["Churn_Status"] == 1,
        df["Signup_Date"] + churn_offsets,
        np.datetime64('NaT')
    )

    # File name
    filename = f"churn_seed_{n_seed}_{label}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    print(f"Saved: {filepath}")

print("\nDone.")