-- ============================================================================
-- Example analytical queries for the market-data-platform warehouse
-- (DuckDB, run against warehouse.duckdb — see README.md for setup)
--
-- All queries assume `gold_daily_features` (one row per symbol + date) and,
-- where noted, `gold_sector_comparison` (a view adding sector-relative
-- return/alpha columns on top of the same Gold table).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. Asset-to-asset correlation matrix (diversification check)
-- ----------------------------------------------------------------------------
-- Pairwise correlation of daily returns across every symbol. Useful for
-- checking whether a set of "different" tickers is actually diversified, or
-- just the same underlying bet repeated N times. `a.symbol < b.symbol` avoids
-- duplicate pairs and self-comparisons.
--
-- Best visualized as a heatmap, not a pie/sunburst — see
-- generate_correlation_heatmap.py for a ready-made script that renders this
-- as a PNG for docs/images/correlation_matrix.png.
WITH returns AS (
    SELECT symbol, date, return_1d
FROM gold_daily_features
WHERE return_1d IS NOT NULL
    )
SELECT
    a.symbol AS symbol_a,
    b.symbol AS symbol_b,
    CORR(a.return_1d, b.return_1d) AS correlation
FROM returns a
         JOIN returns b
              ON a.date = b.date AND a.symbol < b.symbol
GROUP BY a.symbol, b.symbol
ORDER BY correlation DESC;


-- ----------------------------------------------------------------------------
-- 2. CAPM-style beta and alpha against a broad-market proxy (SPY)
-- ----------------------------------------------------------------------------
-- Requires SPY to be included in TWELVE_SYMBOLS. Beta measures how much a
-- symbol amplifies (>1) or dampens (<1) market-wide moves; naive_alpha is
-- the average daily excess return over the market, unadjusted for beta.
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
    COVAR_SAMP(a.return_1d, s.spy_return) / VAR_SAMP(s.spy_return) AS beta,
    CORR(a.return_1d, s.spy_return) AS correlation_to_spy,
    AVG(a.return_1d) - AVG(s.spy_return) AS naive_alpha
FROM assets a
         JOIN spy s ON a.date = s.date
GROUP BY a.symbol
ORDER BY beta DESC;


-- ----------------------------------------------------------------------------
-- 3. Average return by interest-rate regime (tightening vs. easing)
-- ----------------------------------------------------------------------------
-- Groups days into discrete regimes based on the direction of the Fed Funds
-- Rate change, rather than assuming a linear relationship (more robust than
-- a raw correlation when the history window is still short).
SELECT
    symbol,
    CASE
        WHEN fed_funds_chg > 0 THEN 'tightening'
        WHEN fed_funds_chg < 0 THEN 'easing'
        ELSE 'stable'
        END AS regime,
    AVG(return_1d) AS avg_return,
    COUNT(*) AS days
FROM gold_daily_features
WHERE fed_funds_chg IS NOT NULL
GROUP BY symbol, regime
ORDER BY symbol, regime;


-- ----------------------------------------------------------------------------
-- 4. Sector-relative alpha (requires gold_sector_comparison)
-- ----------------------------------------------------------------------------
-- alpha_1d = the symbol's return minus its sector's average return that day.
-- A consistently positive alpha_medio means the asset outperforms its peers,
-- not just the broad market.
SELECT
    symbol,
    AVG(alpha_1d) AS avg_daily_alpha,
    STDDEV(alpha_1d) AS alpha_volatility
FROM gold_sector_comparison
GROUP BY symbol
ORDER BY avg_daily_alpha DESC;


-- ----------------------------------------------------------------------------
-- 5. High-volume, high-move days ("event" screen)
-- ----------------------------------------------------------------------------
-- Isolates days that look like a real catalyst (earnings, macro surprise,
-- news) rather than ordinary noise, by requiring both abnormal volume and
-- an abnormal price move on the same day.
SELECT symbol, date, close, return_1d, volume_rel_20d, vol_ann_21d
FROM gold_daily_features
WHERE volume_rel_20d > 2.0        -- volume at least 2x its 20-day average
  AND ABS(return_1d) > 0.03       -- move of more than 3% in a single day
ORDER BY volume_rel_20d DESC;


-- ----------------------------------------------------------------------------
-- 6. Mean-reversion candidates (extreme RSI + far from the 50-day average)
-- ----------------------------------------------------------------------------
SELECT symbol, date, close, rsi_14, dist_sma_50, dist_sma_200
FROM gold_daily_features
WHERE (rsi_14 < 30 OR rsi_14 > 70)
  AND ABS(dist_sma_50) > 0.10
ORDER BY date DESC;


-- ----------------------------------------------------------------------------
-- 7. Behavior on the market's worst days (tail-risk screen)
-- ----------------------------------------------------------------------------
-- Average return of each symbol specifically on days SPY fell more than 1%.
-- Distinct from beta: this captures asymmetric ("crashes harder than the
-- market") behavior that an average-based beta can mask.
SELECT
    a.symbol,
    AVG(a.return_1d) FILTER (WHERE s.return_1d < -0.01) AS avg_return_on_bad_market_days,
    COUNT(*) FILTER (WHERE s.return_1d < -0.01) AS bad_days
FROM gold_daily_features a
         JOIN gold_daily_features s ON a.date = s.date AND s.symbol = 'SPY'
WHERE a.symbol != 'SPY'
GROUP BY a.symbol
ORDER BY avg_return_on_bad_market_days;