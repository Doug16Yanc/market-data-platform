"""
Generates a correlation heatmap (PNG) from the daily_features Gold table,
for embedding in the README. Run locally, pointing at a local copy of
warehouse.duckdb.

Usage:
    pip install duckdb pandas matplotlib seaborn --break-system-packages
    docker cp market-data-platform-metabase:/data/warehouse/warehouse.duckdb ./warehouse.duckdb
    python -m scripts.generate_correlation_heatmap
"""

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from .duckdb_utils import get_connection
from .chart_style import apply_dark_theme, BACKGROUND, FOREGROUND

OUTPUT_PATH = "docs/images/correlation_matrix.png"

QUERY = """
        SELECT symbol, date, return_1d
        FROM gold_daily_features
        WHERE return_1d IS NOT NULL \
        """


def main():
    apply_dark_theme()

    con = get_connection()
    df = con.execute(QUERY).fetchdf()
    con.close()

    # Long -> wide: one column per symbol, one row per date
    wide = df.pivot(index="date", columns="symbol", values="return_1d")
    corr = wide.corr()

    # Mask the upper triangle AND the diagonal: the matrix is symmetric
    # (upper half repeats the lower half) and the diagonal is always 1.0,
    # which isn't informative and would otherwise eat up the brightest
    # end of the colormap.
    mask = np.triu(np.ones_like(corr, dtype=bool), k=0)

    # This asset universe is tech-heavy, so correlations here are all
    # positive — a sequential colormap uses the full palette for the
    # actual data range, unlike a diverging one (-1..1) that would waste
    # its bottom half. Falls back to a diverging map only if a negative
    # correlation genuinely shows up (e.g. after adding a very different
    # asset class).
    off_diagonal = corr.values[~mask]
    data_min = float(np.nanmin(off_diagonal))
    data_max = float(np.nanmax(off_diagonal))

    if data_min < 0:
        cmap = sns.color_palette("icefire", as_cmap=True)
        vmin, vmax = -1.0, 1.0
    else:
        cmap = sns.color_palette("rocket", as_cmap=True)
        vmin, vmax = 0.0, max(1.0, data_max)

    norm = plt.Normalize(vmin=vmin, vmax=vmax)

    plt.figure(figsize=(9, 7), facecolor=BACKGROUND)
    ax = sns.heatmap(
        corr,
        mask=mask,
        annot=False,  # annotated manually below, with per-cell text contrast
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        square=True,
        linewidths=0.5,
        linecolor=BACKGROUND,
        cbar_kws={"label": "Correlation (daily returns)"},
    )

    # Per-cell text color based on the luminance of that cell's own
    # background color, so numbers stay legible whether the cell landed
    # on a dark or a bright part of the colormap.
    for i in range(len(corr)):
        for j in range(len(corr)):
            if mask[i, j]:
                continue
            value = corr.values[i, j]
            r, g, b, _ = cmap(norm(value))
            luminance = 0.299 * r + 0.587 * g + 0.114 * b
            text_color = BACKGROUND if luminance > 0.6 else FOREGROUND
            ax.text(
                j + 0.5, i + 0.5, f"{value:.2f}",
                ha="center", va="center", color=text_color, fontsize=9,
                )

    ax.set_title("Asset Return Correlation Matrix", color=FOREGROUND, fontsize=13)
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH, dpi=150, facecolor=BACKGROUND)
    plt.close()
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()