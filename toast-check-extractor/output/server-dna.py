import subprocess
import csv
import io
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

DB = "postgres://localhost:5433/agm"

def query(sql):
    r = subprocess.run(["psql", DB, "--csv", "-c", sql], capture_output=True, text=True)
    return pd.read_csv(io.StringIO(r.stdout))

# ── Category-level data ──
categories = {
    'Category': ['Starters', 'Entrees', 'Sides', 'Desserts', 'Wine BTG', 'Cocktails', 'Spirits', 'Prem. Water', 'After-Dinner'],
    'TOP': [0.399, 0.657, 0.204, 0.118, 0.299, 0.158, 0.265, 0.139, 0.135],
    'BOTTOM': [0.365, 0.612, 0.180, 0.116, 0.264, 0.172, 0.250, 0.109, 0.129],
}
cat_df = pd.DataFrame(categories)
cat_df['diff_pct'] = ((cat_df['TOP'] / cat_df['BOTTOM'] - 1) * 100).round(1)

# ── Attach rate data ──
attach = {
    'Category': ['Starter', 'Side', 'Dessert', 'Wine BTG', 'Cocktail', 'Spirit', 'Prem. Water', 'After-Dinner'],
    'TOP': [66.4, 46.5, 31.5, 45.6, 28.1, 35.7, 27.1, 22.7],
    'BOTTOM': [64.2, 42.6, 30.9, 43.8, 30.2, 34.8, 23.3, 22.0],
}
att_df = pd.DataFrame(attach)

# ── Top items where TOP outsells BOTTOM (revenue-weighted) ──
top_items = {
    'item': ['Brussels Sprouts', 'Tuscan Fries', 'Pellegrino Water', 'Panna Water',
             'Spinach', 'Broccoli Rabe', 'Lobster Pasta MC', 'Veal Milanese',
             'Yellowtail Crudo', 'Branzino', 'GL Faust Cab', 'NY Chopped Salad',
             'Filet Classic', 'Tomato & Straciatella', 'Grilled Octopus'],
    'top_rate': [67.1, 63.1, 70.3, 67.0, 34.7, 35.8, 108.4, 28.1, 28.9, 36.2, 17.2, 33.9, 48.5, 25.3, 35.0],
    'bot_rate': [51.4, 50.3, 53.4, 53.0, 23.6, 24.9, 98.6, 19.8, 20.9, 28.3, 10.3, 26.8, 41.8, 19.5, 29.3],
    'price': [17.62, 16.63, 11.90, 11.88, 15.63, 15.60, 53.00, 49.00, 25.25, 48.00, 34.75, 22.62, 62.47, 24.25, 28.00]
}
top_df = pd.DataFrame(top_items)
top_df['pct_diff'] = ((top_df['top_rate'] / top_df['bot_rate'] - 1) * 100).round(1)

# ── Bottom-heavy items ──
bot_items = {
    'item': ['Corn Brulee', 'Espresso Martini', 'Shirley Temple', 'Meatballs APP',
             'Pocket Dial', 'Lemonade', 'Pasta Vodka', 'Jr. Chicken Fingers',
             'Jr. Kid\'s Filet', 'Focaccia', 'Sprite'],
    'top_rate': [55.1, 37.2, 16.6, 20.6, 16.0, 9.1, 4.8, 3.0, 3.3, 3.4, 7.7],
    'bot_rate': [62.5, 44.5, 23.2, 27.1, 20.6, 12.9, 7.9, 5.6, 5.0, 5.5, 10.0],
}
bot_df = pd.DataFrame(bot_items)

# ════════════════════════════════════════════════════════
# BUILD THE FIGURE
# ════════════════════════════════════════════════════════

fig = plt.figure(figsize=(20, 24))
fig.patch.set_facecolor('#0d1117')
gs = gridspec.GridSpec(4, 2, figure=fig, hspace=0.35, wspace=0.3,
                       height_ratios=[0.8, 1, 1.3, 0.7])

colors = {'top': '#58a6ff', 'bot': '#f78166', 'accent': '#7ee787', 'text': '#c9d1d9', 'dim': '#8b949e'}

def style_ax(ax, title):
    ax.set_facecolor('#161b22')
    ax.set_title(title, color='white', fontsize=14, fontweight='bold', pad=12)
    ax.tick_params(colors=colors['text'], labelsize=10)
    for spine in ax.spines.values():
        spine.set_color('#30363d')

# ── 1. HEADLINE STATS ──
ax0 = fig.add_subplot(gs[0, :])
ax0.set_facecolor('#0d1117')
ax0.axis('off')
ax0.text(0.5, 0.92, 'SERVER ITEM DNA', transform=ax0.transAxes, fontsize=28,
         fontweight='bold', color='white', ha='center', va='top')
ax0.text(0.5, 0.75, 'Quality Italian  •  Dining Room Dinner  •  ~10,700 checks per tier  •  Party size 3.5 avg (matched)',
         transform=ax0.transAxes, fontsize=12, color=colors['dim'], ha='center', va='top')

# Headline numbers
stats = [
    ('$105.09', '$94.33', 'Revenue\nper Guest', '+11.4%'),
    ('3.36', '3.11', 'Items\nper Guest', '+8.0%'),
    ('46.5%', '42.6%', 'Side\nAttach Rate', '+3.9pp'),
    ('27.1%', '23.3%', 'Water\nAttach Rate', '+3.8pp'),
]
for i, (top_val, bot_val, label, delta) in enumerate(stats):
    x = 0.12 + i * 0.22
    ax0.text(x, 0.52, label, transform=ax0.transAxes, fontsize=11,
             color=colors['dim'], ha='center', va='top')
    ax0.text(x - 0.05, 0.25, top_val, transform=ax0.transAxes, fontsize=18,
             color=colors['top'], ha='center', va='top', fontweight='bold')
    ax0.text(x + 0.05, 0.25, bot_val, transform=ax0.transAxes, fontsize=18,
             color=colors['bot'], ha='center', va='top', fontweight='bold')
    ax0.text(x, 0.05, delta, transform=ax0.transAxes, fontsize=13,
             color=colors['accent'], ha='center', va='top', fontweight='bold')

ax0.text(0.92, 0.35, 'TOP 7', transform=ax0.transAxes, fontsize=12,
         color=colors['top'], ha='center', fontweight='bold')
ax0.text(0.92, 0.18, 'BOT 7', transform=ax0.transAxes, fontsize=12,
         color=colors['bot'], ha='center', fontweight='bold')

# ── 2. CATEGORY BAR CHART ──
ax1 = fig.add_subplot(gs[1, 0])
style_ax(ax1, 'Items per Guest by Category')
y = np.arange(len(cat_df))
bar_h = 0.35
ax1.barh(y + bar_h/2, cat_df['TOP'], bar_h, color=colors['top'], alpha=0.85, label='Top 7')
ax1.barh(y - bar_h/2, cat_df['BOTTOM'], bar_h, color=colors['bot'], alpha=0.85, label='Bottom 7')
ax1.set_yticks(y)
ax1.set_yticklabels(cat_df['Category'], color=colors['text'])
ax1.set_xlabel('Items per Guest', color=colors['text'], fontsize=10)
ax1.legend(loc='lower right', fontsize=10, facecolor='#161b22', edgecolor='#30363d', labelcolor=colors['text'])
ax1.invert_yaxis()
# Add % diff annotations
for i, row in cat_df.iterrows():
    if row['diff_pct'] > 0:
        ax1.text(max(row['TOP'], row['BOTTOM']) + 0.008, i,
                 f'+{row["diff_pct"]:.0f}%', color=colors['accent'], va='center', fontsize=9, fontweight='bold')

# ── 3. ATTACH RATE COMPARISON ──
ax2 = fig.add_subplot(gs[1, 1])
style_ax(ax2, 'Check Attach Rate (% of checks)')
y = np.arange(len(att_df))
ax2.barh(y + bar_h/2, att_df['TOP'], bar_h, color=colors['top'], alpha=0.85, label='Top 7')
ax2.barh(y - bar_h/2, att_df['BOTTOM'], bar_h, color=colors['bot'], alpha=0.85, label='Bottom 7')
ax2.set_yticks(y)
ax2.set_yticklabels(att_df['Category'], color=colors['text'])
ax2.set_xlabel('% of checks', color=colors['text'], fontsize=10)
ax2.legend(loc='lower right', fontsize=10, facecolor='#161b22', edgecolor='#30363d', labelcolor=colors['text'])
ax2.invert_yaxis()
for i, row in att_df.iterrows():
    diff = row['TOP'] - row['BOTTOM']
    if diff > 0:
        ax2.text(max(row['TOP'], row['BOTTOM']) + 0.5, i,
                 f'+{diff:.1f}pp', color=colors['accent'], va='center', fontsize=9, fontweight='bold')
    elif diff < 0:
        ax2.text(max(row['TOP'], row['BOTTOM']) + 0.5, i,
                 f'{diff:.1f}pp', color=colors['bot'], va='center', fontsize=9, fontweight='bold')

# ── 4. TOP SERVERS OUTSELL: specific items ──
ax3 = fig.add_subplot(gs[2, 0])
style_ax(ax3, 'Items TOP Servers Push Harder\n(per 1,000 guests)')
top_df_sorted = top_df.sort_values('pct_diff', ascending=True)
y = np.arange(len(top_df_sorted))
ax3.barh(y, top_df_sorted['top_rate'], 0.35, color=colors['top'], alpha=0.85, label='Top 7')
ax3.barh(y - 0.35, top_df_sorted['bot_rate'], 0.35, color=colors['bot'], alpha=0.85, label='Bottom 7')
ax3.set_yticks(y - 0.175)
labels = [f"{r['item']}  (${r['price']:.0f})" for _, r in top_df_sorted.iterrows()]
ax3.set_yticklabels(labels, color=colors['text'], fontsize=9)
ax3.set_xlabel('Per 1,000 guests', color=colors['text'], fontsize=10)
ax3.legend(loc='lower right', fontsize=9, facecolor='#161b22', edgecolor='#30363d', labelcolor=colors['text'])
for i, (_, row) in enumerate(top_df_sorted.iterrows()):
    ax3.text(max(row['top_rate'], row['bot_rate']) + 1, i - 0.175,
             f'+{row["pct_diff"]:.0f}%', color=colors['accent'], va='center', fontsize=9, fontweight='bold')

# ── 5. BOTTOM SERVERS SELL MORE OF ──
ax4 = fig.add_subplot(gs[2, 1])
style_ax(ax4, 'Items BOTTOM Servers Sell More Of\n(per 1,000 guests)')
bot_df['pct_diff'] = ((bot_df['bot_rate'] / bot_df['top_rate'] - 1) * 100).round(1)
bot_df_sorted = bot_df.sort_values('pct_diff', ascending=True)
y = np.arange(len(bot_df_sorted))
ax4.barh(y, bot_df_sorted['top_rate'], 0.35, color=colors['top'], alpha=0.85, label='Top 7')
ax4.barh(y - 0.35, bot_df_sorted['bot_rate'], 0.35, color=colors['bot'], alpha=0.85, label='Bottom 7')
ax4.set_yticks(y - 0.175)
ax4.set_yticklabels(bot_df_sorted['item'], color=colors['text'], fontsize=9)
ax4.set_xlabel('Per 1,000 guests', color=colors['text'], fontsize=10)
ax4.legend(loc='lower right', fontsize=9, facecolor='#161b22', edgecolor='#30363d', labelcolor=colors['text'])
for i, (_, row) in enumerate(bot_df_sorted.iterrows()):
    ax4.text(max(row['top_rate'], row['bot_rate']) + 0.5, i - 0.175,
             f'+{row["pct_diff"]:.0f}%', color=colors['bot'], va='center', fontsize=9, fontweight='bold')

# ── 6. KEY INSIGHTS ──
ax5 = fig.add_subplot(gs[3, :])
ax5.set_facecolor('#161b22')
ax5.axis('off')
for spine in ax5.spines.values():
    spine.set_visible(False)

insights = [
    "THE SIDE HUSTLE: Top servers sell 31% more Brussels Sprouts & 25% more Tuscan Fries per guest. At ~$17 each, that's the easiest upsell on the menu.",
    "PREMIUM HYDRATION: Top servers get water on 27% of checks vs 23% — a 16% gap. Pellegrino/Panna at $12 is pure margin with zero kitchen effort.",
    "PROTEIN UPGRADES: Top servers push Veal Milanese (+42%), Branzino (+28%), and Filet (+16%) over cheaper entrees — steering guests toward premium.",
    "KID TAX: Bottom servers sell 87% more chicken fingers and 51% more kid's filets — suggesting they serve more families, but also don't pivot parents to adult items.",
    "COCKTAIL PARADOX: Bottom servers sell MORE Espresso Martinis (+19%) and Pocket Dials (+28%). Top servers push spirits neat ($18-23) and wine BTG instead — higher margin, faster to pour.",
]

for i, text in enumerate(insights):
    y_pos = 0.88 - i * 0.19
    ax5.text(0.02, y_pos, f'►  {text}', transform=ax5.transAxes,
             fontsize=11, color=colors['text'], va='top', wrap=True,
             fontfamily='monospace')

plt.savefig('/Users/shein/Desktop/agent-skills/toast-check-extractor/output/server-item-dna.png',
            dpi=180, bbox_inches='tight', facecolor=fig.get_facecolor())
print('Saved to server-item-dna.png')
