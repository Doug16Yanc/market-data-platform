"""
Generates the remaining static charts referenced in the README, each
derived from a query in docs/example_queries.sql. Run after
generate_correlation_heatmap.py, or standalone — it opens its own
connection.

Usage:
    pip install duckdb pandas matplotlib seaborn --break-system-packages
    docker cp market-data-platform-metabase:/data/warehouse/warehouse.duckdb ./warehouse.duckdb
    python -m scripts.generate_charts
"""

import pandas as pd
import matplotlib.pyplot as plt

from .duckdb_utils import get_connection
from .chart_style import (
    apply_dark_theme,
    BACKGROUND,
    FOREGROUND,
    GRID_COLOR,
    ACCENT_POSITIVE,
    ACCENT_NEGATIVE,
    ACCENT_NEUTRAL,
    ACCENT_HIGHLIGHT,
)

IMAGES_DIR = "docs/images"


def chart_capm_beta(con):
    """Query #2 in example_queries.sql — beta against SPY, as a sorted bar chart."""
    query = """
            WITH spy AS (
                SELECT date, return_1d AS spy_return
            FROM gold_daily_features
            WHERE symbol = 'SPY'
                ),
                assets AS (
            SELECT symbol, date, return_1d
            FROM gold_daily_features
            WHERE symbol != 'SPY'
                )
            SELECT
                a.symbol,
                COVAR_SAMP(a.return_1d, s.spy_return) / VAR_SAMP(s.spy_return) AS beta
            FROM assets a
                     JOIN spy s ON a.date = s.date
            GROUP BY a.symbol
            ORDER BY beta DESC \
            """
    df = con.execute(query).fetchdf()

    fig, ax = plt.subplots(figsize=(8, 6), facecolor=BACKGROUND)
    colors = [ACCENT_NEGATIVE if b > 1 else ACCENT_POSITIVE for b in df["beta"]]
    ax.barh(df["symbol"], df["beta"], color=colors)
    ax.axvline(1.0, color=FOREGROUND, linestyle="--", linewidth=1, label="Market beta (SPY = 1.0)")
    ax.set_xlabel("Beta vs. SPY")
    ax.set_title("CAPM Beta by Symbol", color=FOREGROUND)
    ax.legend()
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(f"{IMAGES_DIR}/capm_beta.png", dpi=150, facecolor=BACKGROUND)
    plt.close()
    print("Saved capm_beta.png")


def chart_interest_rate_regime(con):
    """Query #3 — average return by Fed Funds Rate regime, grouped bar chart."""
    query = """
            SELECT
                symbol,
                CASE
                    WHEN fed_funds_chg > 0 THEN 'tightening'
                    WHEN fed_funds_chg < 0 THEN 'easing'
                    ELSE 'stable'
                    END AS regime,
                AVG(return_1d) AS avg_return
            FROM gold_daily_features
            WHERE fed_funds_chg IS NOT NULL
            GROUP BY symbol, regime \
            """
    df = con.execute(query).fetchdf()
    pivoted = df.pivot(index="symbol", columns="regime", values="avg_return")

    regime_colors = {
        "easing": ACCENT_POSITIVE,
        "stable": ACCENT_NEUTRAL,
        "tightening": ACCENT_NEGATIVE,
    }
    colors = [regime_colors.get(col, ACCENT_NEUTRAL) for col in pivoted.columns]

    ax = pivoted.plot(kind="bar", figsize=(10, 6), color=colors)
    ax.set_facecolor(BACKGROUND)
    ax.figure.set_facecolor(BACKGROUND)
    ax.axhline(0, color=FOREGROUND, linewidth=0.8)
    ax.set_ylabel("Average daily return")
    ax.set_title("Average Return by Interest Rate Regime", color=FOREGROUND)
    ax.legend(title="Regime")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(f"{IMAGES_DIR}/interest_rate_regime.png", dpi=150, facecolor=BACKGROUND)
    plt.close()
    print("Saved interest_rate_regime.png")


def chart_sector_alpha(con):
    """Query #4 — average daily alpha vs. sector, with volatility as error bars."""
    query = """
            SELECT
                symbol,
                AVG(alpha_1d) AS avg_daily_alpha,
                STDDEV(alpha_1d) AS alpha_volatility
            FROM gold_sector_comparison
            GROUP BY symbol
            ORDER BY avg_daily_alpha DESC \
            """
    df = con.execute(query).fetchdf()

    fig, ax = plt.subplots(figsize=(8, 6), facecolor=BACKGROUND)
    colors = [ACCENT_POSITIVE if a > 0 else ACCENT_NEGATIVE for a in df["avg_daily_alpha"]]
    ax.barh(
        df["symbol"], df["avg_daily_alpha"],
        xerr=df["alpha_volatility"], color=colors, ecolor=GRID_COLOR, capsize=3,
    )
    ax.axvline(0, color=FOREGROUND, linewidth=0.8)
    ax.set_xlabel("Average daily alpha vs. sector")
    ax.set_title("Sector-Relative Alpha (error bars = alpha volatility)", color=FOREGROUND)
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(f"{IMAGES_DIR}/sector_alpha.png", dpi=150, facecolor=BACKGROUND)
    plt.close()
    print("Saved sector_alpha.png")


def chart_event_days(con):
    """Query #5 — high-volume, high-move days, as a scatter plot."""
    query = """
            SELECT symbol, date, return_1d, volume_rel_20d
            FROM gold_daily_features
            WHERE volume_rel_20d IS NOT NULL AND return_1d IS NOT NULL \
            """
    df = con.execute(query).fetchdf()
    df["is_event"] = (df["volume_rel_20d"] > 2.0) & (df["return_1d"].abs() > 0.03)

    fig, ax = plt.subplots(figsize=(9, 6), facecolor=BACKGROUND)
    ax.scatter(
        df.loc[~df["is_event"], "volume_rel_20d"],
        df.loc[~df["is_event"], "return_1d"],
        alpha=0.3, s=15, color=ACCENT_NEUTRAL, label="Ordinary day",
    )
    ax.scatter(
        df.loc[df["is_event"], "volume_rel_20d"],
        df.loc[df["is_event"], "return_1d"],
        alpha=0.9, s=35, color=ACCENT_HIGHLIGHT, label="Event day (vol > 2x, |move| > 3%)",
    )
    ax.axhline(0, color=GRID_COLOR, linewidth=0.8)
    ax.set_xlabel("Volume relative to 20-day average")
    ax.set_ylabel("Daily return")
    ax.set_title("Volume vs. Return — Isolating Likely Catalyst Days", color=FOREGROUND)
    ax.legend()
    plt.tight_layout()
    plt.savefig(f"{IMAGES_DIR}/event_days.png", dpi=150, facecolor=BACKGROUND)
    plt.close()
    print("Saved event_days.png")


def chart_bad_market_days(con):
    """Query #7 — average return on SPY's worst days, sorted bar chart."""
    query = """
            SELECT
                a.symbol,
                AVG(a.return_1d) FILTER (WHERE s.return_1d < -0.01) AS avg_return_on_bad_days
            FROM gold_daily_features a
                     JOIN gold_daily_features s ON a.date = s.date AND s.symbol = 'SPY'
            WHERE a.symbol != 'SPY'
            GROUP BY a.symbol
            ORDER BY avg_return_on_bad_days \
            """
    df = con.execute(query).fetchdf()

    fig, ax = plt.subplots(figsize=(8, 6), facecolor=BACKGROUND)
    ax.barh(df["symbol"], df["avg_return_on_bad_days"], color=ACCENT_NEGATIVE)
    ax.axvline(0, color=FOREGROUND, linewidth=0.8)
    ax.set_xlabel("Average return on days SPY fell > 1%")
    ax.set_title("Tail-Risk Behavior on the Market's Worst Days", color=FOREGROUND)
    plt.tight_layout()
    plt.savefig(f"{IMAGES_DIR}/bad_market_days.png", dpi=150, facecolor=BACKGROUND)
    plt.close()
    print("Saved bad_market_days.png")


def main():
    apply_dark_theme()

    con = get_connection()

    chart_capm_beta(con)
    chart_interest_rate_regime(con)
    chart_sector_alpha(con)
    chart_event_days(con)
    chart_bad_market_days(con)

    con.close()


if __name__ == "__main__":
    main()