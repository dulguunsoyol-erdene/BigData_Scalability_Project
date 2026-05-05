import matplotlib.pyplot as plt

sizes = ['100MB', '512MB', '1GB']

workloads = {
    'Completeness':            {'pandas': [0.1198, 0.5559, 1.0813], 'pyspark': [0.8513, 1.4838, 2.0000]},
    'Accuracy':                {'pandas': [0.0097, 0.0458, 0.1010], 'pyspark': [0.4511, 0.6623, 0.8654]},
    'Timeliness':              {'pandas': [0.0615, 0.2721, 0.6055], 'pyspark': [0.7383, 1.1900, 1.8415]},
    'Timeliness+Completeness': {'pandas': [0.0548, 0.2510, 0.5718], 'pyspark': [0.7127, 1.3274, 1.6617]},
    'Accuracy+Timeliness':     {'pandas': [0.0546, 0.2504, 0.5639], 'pyspark': [0.8473, 1.8461, 2.5337]},
}

BLUES = ['#0c447c', '#185fa5', '#378add']
REDS  = ['#501313', '#a32d2d', '#e24b4a']

workload_names = list(workloads.keys())

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

for i, size in enumerate(sizes):
    pandas_vals  = [workloads[w]['pandas'][i]  for w in workload_names]
    pyspark_vals = [workloads[w]['pyspark'][i] for w in workload_names]
    axes[0].plot(workload_names, pandas_vals,  color=BLUES[i], linewidth=2, marker='o', markersize=6, label=size)
    axes[1].plot(workload_names, pyspark_vals, color=REDS[i],  linewidth=2, marker='D', markersize=6, label=size)

for ax, title in zip(axes, ['Pandas', 'PySpark']):
    ax.set_title(f'{title} — Workload vs Time by Data Size', fontsize=12)
    ax.set_xlabel('Workload', fontsize=11)
    ax.set_ylabel('Avg time (seconds)', fontsize=11)
    ax.set_xticks(range(len(workload_names)))
    ax.set_xticklabels(workload_names, rotation=15, ha='right', fontsize=9)
    ax.set_ylim(bottom=0)
    ax.legend(title='Data size', fontsize=10)
    ax.grid(axis='y', linestyle=':', alpha=0.5)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig('workload_vs_time.png', dpi=150, bbox_inches='tight')
plt.show()