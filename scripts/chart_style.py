"""
Shared dark-theme styling for the chart scripts in this folder, so the
correlation heatmap and the bar/scatter charts in generate_charts.py look
like one consistent set instead of each picking its own palette.
"""

import matplotlib.pyplot as plt

BACKGROUND = "#0f172a"      # slate-900
FOREGROUND = "#e2e8f0"      # slate-200
GRID_COLOR = "#334155"      # slate-700

ACCENT_POSITIVE = "#2dd4bf"  # teal-400 — used for "favorable"/below-market values
ACCENT_NEGATIVE = "#fb7185"  # rose-400 — used for "risk"/above-market values
ACCENT_NEUTRAL = "#94a3b8"   # slate-400 — used for "stable"/ordinary points
ACCENT_HIGHLIGHT = "#fbbf24"  # amber-400 — used to call out event/outlier points

# Diverging colormap whose midpoint is dark rather than white, unlike
# RdBu_r — built for exactly this dark-background use case.
HEATMAP_CMAP = "icefire"


def apply_dark_theme():
    plt.rcParams.update({
        "figure.facecolor": BACKGROUND,
        "axes.facecolor": BACKGROUND,
        "savefig.facecolor": BACKGROUND,
        "axes.edgecolor": GRID_COLOR,
        "axes.labelcolor": FOREGROUND,
        "text.color": FOREGROUND,
        "xtick.color": FOREGROUND,
        "ytick.color": FOREGROUND,
        "axes.grid": True,
        "grid.color": GRID_COLOR,
        "grid.alpha": 0.4,
        "legend.facecolor": BACKGROUND,
        "legend.edgecolor": GRID_COLOR,
        "legend.labelcolor": FOREGROUND,
        "font.size": 11,
    })