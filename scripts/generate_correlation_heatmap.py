"""
Generates a correlation heatmap (PNG) from the daily_features Gold table,
for embedding in the README. Run locally, pointing at a local copy of
warehouse.duckdb.

Usage:
    pip install duckdb pandas matplotlib seaborn --break-system-packages
    docker cp market-data-platform-metabase:/data/warehouse/warehouse.duckdb ./warehouse.duckdb
    python scripts/generate_correlation_heatmap.py
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from .duckdb_utils import get_connection

OUTPUT_PATH = "docs/images/correlation_matrix.png"

QUERY = """
        SELECT symbol, date, return_1d
        FROM gold_daily_features
        WHERE return_1d IS NOT NULL \
        """


def main():
    con = get_connection()
    df = con.execute(QUERY).fetchdf()
    con.close()

    # Long -> wide: one column per symbol, one row per date
    wide = df.pivot(index="date", columns="symbol", values="return_1d")
    corr = wide.corr()

    plt.figure(figsize=(9, 7))
    sns.heatmap(
        corr,
        annot=True,
        fmt=".2f",
        cmap="RdBu_r",
        vmin=-1,
        vmax=1,
        square=True,
        cbar_kws={"label": "Correlation (daily returns)"},
    )
    plt.title("Asset Return Correlation Matrix")
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH, dpi=150)
    plt.close()
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()