import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display

sizes = ['100MB', '512MB', '1GB']

pandas_avg  = [0.0801, 0.3350, 0.6867]
pyspark_avg = [0.7081, 1.2620, 1.8224]

results = pd.DataFrame({
    'pandas_avg_time':  pandas_avg,
    'pyspark_avg_time': pyspark_avg
}, index=sizes)

display(results)

fig, ax = plt.subplots(figsize=(8, 5))

ax.plot(sizes, pandas_avg,  color='#185fa5', linewidth=2.5, marker='o', markersize=7, label='Pandas')
ax.plot(sizes, pyspark_avg, color='#a32d2d', linewidth=2.5, marker='D', markersize=6, label='PySpark')

for i, (p, s) in enumerate(zip(pandas_avg, pyspark_avg)):
    ax.annotate(f'{p:.3f}s', xy=(i, p),  xytext=(0, 10), textcoords='offset points',
                ha='center', fontsize=10, color='#185fa5')
    ax.annotate(f'{s:.3f}s', xy=(i, s), xytext=(0, 10), textcoords='offset points',
                ha='center', fontsize=10, color='#a32d2d')

ax.set_xlabel('Data size', fontsize=12)
ax.set_ylabel('Avg time (seconds)', fontsize=12)
ax.set_title('Data size vs average execution time — Pandas vs PySpark', fontsize=13, fontweight='normal')
ax.set_ylim(bottom=0, top=2.2)
ax.legend(fontsize=11)
ax.grid(axis='y', linestyle=':', alpha=0.5)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.show()