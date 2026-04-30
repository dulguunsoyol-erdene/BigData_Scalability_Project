# BigData_Scalability_Project

## Pandas vs PySpark: Data Quality Workload Performance Comparison

## Overview

This project benchmarks the performance and scalability of **Pandas** and **PySpark** when executing data quality (DQ) workloads on large-scale datasets.

The goal is to evaluate how **data size and workload complexity** impact:

* Runtime performance
* Computational efficiency
* Scalability behavior

---

## Objectives

* Compare **Pandas vs PySpark** across increasing dataset sizes
* Evaluate performance under **different data qualities**
* Analyze how **workload complexity affects runtime**
* Identify the **crossover point** where PySpark outperforms Pandas

---

## Dataset

A synthetic customer churn dataset was generated using NumPy and Pandas.
Generation code can be found in the data folder.

### Key Characteristics:

* Mixed data types (numeric, categorical, datetime)
* Built-in data quality challenges:
  * Invalid categorical values (`"None"`)

* Human input and time features

### Dataset Sizes:

* **1.2M rows (~100MB)**
* **5.6M rows (~512MB)**
* **11.2M rows (~1GB)**

---

## Data Schema

| Column Name               | Description                          |
| ------------------------- | ------------------------------------ |
| Customer_ID               | Unique customer identifier           |
| Age                       | Customer age (18–70)                 |
| Gender                    | Gender identity                      |
| Location                  | Customer location                    |
| Signup_Date               | Subscription start date              |
| Subscription_Type         | Plan type (Basic, Standard, Premium) |
| Billing_Cycle             | Monthly or Annual                    |
| Monthly_Fee               | Subscription fee                     |
| Avg_Monthly_Usage_Hours   | Usage intensity                      |
| Login_Frequency_Per_Month | Login frequency                      |
| Support_Tickets_Count     | Support interactions                 |
| Payment_Method            | Payment type                         |
| Late_Payments_Count       | Late payment frequency               |
| Discount_Applied          | Discount indicator                   |
| Customer_Tenure_Months    | Length of subscription               |
| Churn_Status              | Churn indicator                      |
| Churn_Date                | Date of churn                        |

---

## Workloads

Five data quality workloads were implemented:

### 1. Completeness

Measures missing or invalid values.

### 2. Accuracy

Evaluates correctness based on business rules.

### 3. Timeliness

Validates temporal consistency of data.

### 4. Timeliness + Completeness

Combines missing value checks with time-based validation.

### 5. Timeliness + Accuracy

Combines business rules with temporal constraints.

---

## Complexity Levels

Each workload includes **three levels of complexity**:

| Level   | Description                                |
| ------- | ------------------------------------------ |
| Simple  | Basic column operations                    |
| Medium  | Multiple conditions across columns         |
| Hard    | Joins + multiple conditions + aggregations |

---

## Example Workload (TA3 – Hard)

### Pandas

```python
merged = df.merge(ref, on="Customer_ID")

score = (
    (merged["Monthly_Fee"] >= 10) &
    (merged["Avg_Monthly_Usage_Hours"] <= 80) &
    (merged["Last_Update"] >= merged["Signup_Date"])
).mean()
```

### PySpark

```python
df.join(ref, "Customer_ID") \
  .withColumn("score",
      (col("Monthly_Fee") >= 10) &
      (col("Avg_Monthly_Usage_Hours") <= 80) &
      (col("Last_Update") >= col("Signup_Date"))
  ) \
  .agg(avg(col("score").cast("double")))
```

---

## Performance Measurement

All workloads are timed using:

```python
import time

start = time.perf_counter()
# workload
end = time.perf_counter()
```

### Important Notes:

* PySpark uses lazy evaluation, so `.count()` is required to run everything
* Pandas executes everything without prompting

---

## Experimental Design

Each workload is executed across:

* Multiple dataset sizes (100MB, 512MB, 1GB)
* Three complexity levels (Low, Medium, Hard)
* Both Pandas and PySpark implementations

### Metrics Collected:

* Runtime (seconds)
* Relative performance comparisons between Pandas and PySpark
* Efficiency across workloads and within workloads

---

## Expected Insights

* Pandas performs well on smaller datasets
* PySpark scales better with large datasets
* Join-heavy workloads significantly increase runtime
* Code complexity affects performance

---

## Considerations

* Spark execution prep time for small datasets
* Memory limitations in Pandas for bigger datasets

---

## Future Work

* Evaluate Spark optimizations:

  * Broadcast joins
  * Partitioning strategies
* Add distributed cluster testing
* Include additional frameworks (e.g., Dask, Polars)
* Visualize performance trends

---

## Conclusion

This project demonstrates that:

* **Pandas is efficient for small-to-medium datasets.** Datasets that can be run within the memory capasity of the processing machine.
* **PySpark becomes advantageous at bigger scales.** Datasets that exceed the capabilities of a single processing machine should be implemented using PySpark.
* **Workload complexity plays a critical role in performance outcomes.** For example, loading all the data in at once is performs better in PySpark compared to Pandas.
* There is no observable crossover point.

---

## Author Notes

This project was developed as part of an independent study on **big data processing scalability and performance comparison**.

---
