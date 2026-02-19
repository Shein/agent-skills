import subprocess
import csv
import io
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap

# Pull data from postgres
result = subprocess.run(
    ["psql", "postgres://localhost:5433/agm", "--csv", "-c", """
    SELECT day_of_week, hour_opened,
      SUM(subtotal)::numeric/100 as revenue,
      COUNT(*) as checks
    FROM checks
    WHERE hour_opened IS NOT NULL
    GROUP BY day_of_week, hour_opened
    ORDER BY day_of_week, hour_opened;
    """],
    capture_output=True, text=True
)

df = pd.read_csv(io.StringIO(result.stdout))

day_labels = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
hour_range = range(11, 24)  # 11am to 11pm (meaningful hours)

# Build pivot for revenue
pivot = df.pivot(index='hour_opened', columns='day_of_week', values='revenue').fillna(0)
pivot = pivot.loc[pivot.index.isin(hour_range)]
pivot.columns = [day_labels[int(c)] for c in pivot.columns]

# Also build check-count pivot for annotations
pivot_checks = df.pivot(index='hour_opened', columns='day_of_week', values='checks').fillna(0)
pivot_checks = pivot_checks.loc[pivot_checks.index.isin(hour_range)]
pivot_checks.columns = [day_labels[int(c)] for c in pivot_checks.columns]

# Format hour labels
hour_labels = []
for h in pivot.index:
    if h == 0:
        hour_labels.append('12am')
    elif h < 12:
        hour_labels.append(f'{h}am')
    elif h == 12:
        hour_labels.append('12pm')
    else:
        hour_labels.append(f'{h-12}pm')

# Create figure
fig, ax = plt.subplots(figsize=(12, 10))

# Custom colormap: dark background feel
cmap = LinearSegmentedColormap.from_list('revenue', [
    '#1a1a2e',  # dark navy
    '#16213e',  # dark blue
    '#0f3460',  # blue
    '#e94560',  # warm red
    '#ff6b6b',  # coral
    '#ffd93d',  # gold
])

# Create annotation text: revenue + check count
annot_text = []
for i in pivot.index:
    row = []
    for c in pivot.columns:
        rev = pivot.loc[i, c]
        chk = int(pivot_checks.loc[i, c])
        if rev >= 1000:
            row.append(f'${rev/1000:.0f}K\n({chk})')
        else:
            row.append(f'${rev:.0f}\n({chk})')
    annot_text.append(row)

annot_df = pd.DataFrame(annot_text, index=pivot.index, columns=pivot.columns)

sns.heatmap(
    pivot,
    annot=annot_df,
    fmt='',
    cmap=cmap,
    linewidths=1.5,
    linecolor='#0a0a1a',
    cbar_kws={
        'label': 'Revenue ($)',
        'format': '${x:,.0f}',
        'shrink': 0.8,
    },
    ax=ax,
    annot_kws={'size': 8, 'color': 'white', 'fontweight': 'bold'},
)

ax.set_yticklabels(hour_labels, rotation=0, fontsize=11)
ax.set_xticklabels(pivot.columns, rotation=0, fontsize=12, fontweight='bold')
ax.set_xlabel('')
ax.set_ylabel('Hour Opened', fontsize=12, fontweight='bold')
ax.set_title('Quality Italian — Revenue Heatmap by Hour & Day of Week\n(check count in parentheses)',
             fontsize=16, fontweight='bold', pad=20)

# Style the colorbar
cbar = ax.collections[0].colorbar
cbar.ax.tick_params(labelsize=10)

plt.tight_layout()
plt.savefig('/Users/shein/Desktop/agent-skills/toast-check-extractor/output/revenue-heatmap.png', dpi=200, bbox_inches='tight')
print('Saved to revenue-heatmap.png')
