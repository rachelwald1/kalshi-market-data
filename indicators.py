"""
indicators.py

Compute prediction-market indicators from snapshot data (Kalshi-like markets).

Design goals:
- Operate on a pandas DataFrame of market snapshots.
- Compute indicators per market (grouped by 'ticker'), ordered by time.
- Be robust to missing quotes (no book), missing values, and mixed price formats.
- Avoid lookahead bias (only use past values in rolling windows).

Typical usage:

    import pandas as pd
    from indicators import add_indicators

    df = pd.read_csv("kalshi_markets.csv")
    df = add_indicators(df)
    df.to_csv("kalshi_markets_with_indicators.csv", index=False)

"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class IndicatorConfig:
    """
    Configuration for rolling windows etc.

    These windows are in "number of rows per ticker" (i.e. snapshot steps),
    NOT time-based. If you collect every 60s, then window=60 ~ last hour.
    """
    z_window: int = 60
    vol_window: int = 60
    range_window: int = 60
    momentum_lag: int = 30

    ema_fast: int = 10
    ema_slow: int = 30

    # If p is below eps or above 1-eps, trading/execution can get weird
    near_bounds_eps: float = 0.05


# =============================================================================
# Helper functions (vectorised)
# =============================================================================

def _to_numeric(series: pd.Series) -> pd.Series:
    """
    Convert a column to numeric safely, coercing bad values to NaN.
    """
    return pd.to_numeric(series, errors="coerce")


def _normalize_price_to_prob(x: pd.Series) -> pd.Series:
    """
    Normalize Kalshi-style prices into probabilities in [0, 1].

    In Kalshi, prices are often quoted in "cents" (0..100) or integers.
    But sometimes you may already have 0..1 floats depending on your pipeline.

    Rule-of-thumb:
    - If value looks like > 1.5, interpret as cents and divide by 100.
    - Else assume it's already a probability-like number in [0, 1].

    This keeps your code robust even if your collector changes format later.
    """
    x = _to_numeric(x)
    # If > 1.5, assume cents (e.g. 63 means 63 cents -> 0.63)
    return np.where(x > 1.5, x / 100.0, x).astype(float)


def _has_book(bid: pd.Series, ask: pd.Series) -> pd.Series:
    """
    A 'book exists' if both bid and ask are present and > 0.
    """
    bid = _to_numeric(bid)
    ask = _to_numeric(ask)
    return (bid > 0) & (ask > 0)


def _mid(bid: pd.Series, ask: pd.Series) -> pd.Series:
    """
    Midpoint, but only valid where both bid and ask exist.
    Otherwise NaN.
    """
    has = _has_book(bid, ask)
    bidp = _normalize_price_to_prob(bid)
    askp = _normalize_price_to_prob(ask)
    mid = (bidp + askp) / 2.0
    return mid.where(has, np.nan)


def _spread(bid: pd.Series, ask: pd.Series) -> pd.Series:
    """
    Absolute spread (in probability units, e.g. 0.04),
    valid only where book exists. Otherwise NaN.
    """
    has = _has_book(bid, ask)
    bidp = _normalize_price_to_prob(bid)
    askp = _normalize_price_to_prob(ask)
    spr = askp - bidp
    return spr.where(has, np.nan)


def _rel_spread(mid: pd.Series, spread: pd.Series) -> pd.Series:
    """
    Relative spread = spread / mid.

    Note: mid can be near 0; we guard by requiring mid > 0.
    """
    mid = _to_numeric(mid)
    spread = _to_numeric(spread)
    rel = spread / mid.replace(0, np.nan)
    return rel


def _zscore(series: pd.Series, window: int) -> pd.Series:
    """
    Rolling z-score: (x - rolling_mean) / rolling_std.
    Uses past window observations; min_periods=window for stability.
    """
    s = _to_numeric(series)
    mu = s.rolling(window, min_periods=window).mean()
    sd = s.rolling(window, min_periods=window).std()
    return (s - mu) / sd


def _rolling_vol(delta_series: pd.Series, window: int) -> pd.Series:
    """
    Rolling volatility of returns/differences (std of delta).
    """
    d = _to_numeric(delta_series)
    return d.rolling(window, min_periods=window).std()


def _rolling_range(series: pd.Series, window: int) -> pd.Series:
    """
    Rolling max-min over window.
    """
    s = _to_numeric(series)
    rmax = s.rolling(window, min_periods=window).max()
    rmin = s.rolling(window, min_periods=window).min()
    return rmax - rmin


def _ema(series: pd.Series, span: int) -> pd.Series:
    """
    Exponential moving average.
    adjust=False is typical for trading signals.
    """
    s = _to_numeric(series)
    return s.ewm(span=span, adjust=False).mean()


# =============================================================================
# Core indicator computation per ticker
# =============================================================================

def _compute_group_indicators(g: pd.DataFrame, cfg: IndicatorConfig) -> pd.DataFrame:
    """
    Compute indicators for a single ticker group 'g' (already sorted by timestamp).
    Returns a copy with new columns.
    """
    g = g.copy()

    # --- Basic book existence flags ---
    g["has_yes_book"] = _has_book(g["yes_bid"], g["yes_ask"])
    g["has_no_book"] = _has_book(g["no_bid"], g["no_ask"])

    # --- Mid and spread for YES and NO ---
    g["mid_yes"] = _mid(g["yes_bid"], g["yes_ask"])
    g["mid_no"] = _mid(g["no_bid"], g["no_ask"])

    g["spread_yes"] = _spread(g["yes_bid"], g["yes_ask"])
    g["spread_no"] = _spread(g["no_bid"], g["no_ask"])

    # A simple "relative spread" based on YES mid/spread.
    # If YES book missing, rel_spread will be NaN.
    g["rel_spread_yes"] = _rel_spread(g["mid_yes"], g["spread_yes"])

    # --- Implied probability p (YES) ---
    # Preferred: use both YES and NO mids and normalize.
    # This reduces distortion when one side is slightly off.
    denom = (g["mid_yes"] + g["mid_no"])
    g["p_yes"] = (g["mid_yes"] / denom).where(denom > 0, np.nan)

    # If you sometimes don't have NO book, fallback to YES mid directly.
    g["p_yes"] = g["p_yes"].fillna(g["mid_yes"])

    # --- Overround / consistency ---
    # Ideally mid_yes + mid_no == 1. Deviations indicate friction/staleness.
    g["overround"] = (g["mid_yes"] + g["mid_no"]) - 1.0

    # --- Time to expiry (hours) ---
    # close_time might be missing for some markets; coerce to numeric.
    ts = _to_numeric(g["timestamp"])
    ct = _to_numeric(g["close_time"])
    g["tte_hours"] = (ct - ts) / 3600.0

    # --- Volume / open interest (levels & changes) ---
    g["volume"] = _to_numeric(g.get("volume", np.nan))
    g["open_interest"] = _to_numeric(g.get("open_interest", np.nan))

    # Changes are often more informative than levels
    g["d_volume"] = g["volume"].diff()
    g["d_open_interest"] = g["open_interest"].diff()

    # --- Price changes (returns) ---
    g["delta_p"] = _to_numeric(g["p_yes"]).diff()

    # --- Mean reversion / normalization ---
    g["z_p"] = _zscore(g["p_yes"], cfg.z_window)

    # --- Volatility & range ---
    g["vol_p"] = _rolling_vol(g["delta_p"], cfg.vol_window)
    g["range_p"] = _rolling_range(g["p_yes"], cfg.range_window)

    # --- Momentum & trend ---
    g["momentum_p"] = _to_numeric(g["p_yes"]) - _to_numeric(g["p_yes"]).shift(cfg.momentum_lag)

    ema_fast = _ema(g["p_yes"], cfg.ema_fast)
    ema_slow = _ema(g["p_yes"], cfg.ema_slow)
    g["ema_fast"] = ema_fast
    g["ema_slow"] = ema_slow
    g["ema_diff"] = ema_fast - ema_slow

    # --- Acceleration (second difference) ---
    g["accel_p"] = g["delta_p"].diff()

    # --- Near-bounds flag (execution often worse near 0/1) ---
    p = _to_numeric(g["p_yes"])
    eps = cfg.near_bounds_eps
    g["near_bounds"] = (p < eps) | (p > 1.0 - eps)

    # --- A simple "staleness proxy" ---
    # If price doesn't change across snapshots, it may be stale.
    # (This is NOT perfect staleness, but it's cheap and useful.)
    g["is_unchanged"] = (p == p.shift(1))

    return g


# =============================================================================
# Public API
# =============================================================================

def add_indicators(df: pd.DataFrame, cfg: Optional[IndicatorConfig] = None) -> pd.DataFrame:
    """
    Add indicator columns to a market snapshot DataFrame.

    Important expectations:
    - df contains multiple tickers.
    - df may contain repeated snapshots for the same ticker across time.
    - df has a 'timestamp' column so we can order within each ticker.

    Returns:
    - A new DataFrame with indicator columns appended.
    """
    if cfg is None:
        cfg = IndicatorConfig()

    required = ["ticker", "timestamp", "close_time", "yes_bid", "yes_ask", "no_bid", "no_ask"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Sort once globally to make group sorting consistent and stable
    df_sorted = df.copy()
    df_sorted["timestamp"] = _to_numeric(df_sorted["timestamp"])
    df_sorted = df_sorted.sort_values(["ticker", "timestamp"], kind="mergesort")

    # Compute per-ticker indicators
    out = (
        df_sorted
        .groupby("ticker", group_keys=False)
        .apply(lambda g: _compute_group_indicators(g, cfg))
    )

    return out
