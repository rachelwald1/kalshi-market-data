"""
main.py

Orchestrates the data pipeline:

1) Load raw market snapshots from a CSV (produced by collect.py)
2) Compute indicators (time-series features) per ticker
3) Optionally compute tradability features / score (snapshot features)
4) Save an enriched CSV for analysis/backtesting

Why this file exists:
- indicators.py should NOT read/write files (pure transformation)
- collect.py should NOT compute indicators (pure data collection)
- main.py ties the stages together
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Your modules
from indicators import add_indicators, IndicatorConfig

# Optional: only import these if they exist in your repo.
# If you don't have them yet, leave commented out.
# from tradability import add_tradability  # (example name)


# ---------------------------
# Helper functions
# ---------------------------

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments so you can run:

      python main.py --input kalshi_markets.csv --output features.csv

    and tweak windows without editing code.
    """
    p = argparse.ArgumentParser(description="Compute indicators/tradability from Kalshi snapshot CSV")

    p.add_argument("--input", type=str, default="kalshi_markets.csv",
                   help="Path to input CSV of raw snapshots")
    p.add_argument("--output", type=str, default="kalshi_markets_with_indicators.csv",
                   help="Path to output CSV with added features")

    # Rolling window parameters (in number of snapshots)
    p.add_argument("--z_window", type=int, default=60, help="Window for z-score (rows)")
    p.add_argument("--vol_window", type=int, default=60, help="Window for rolling volatility (rows)")
    p.add_argument("--range_window", type=int, default=60, help="Window for rolling range (rows)")
    p.add_argument("--momentum_lag", type=int, default=30, help="Lag for momentum (rows)")

    # EMA parameters
    p.add_argument("--ema_fast", type=int, default=10, help="Fast EMA span")
    p.add_argument("--ema_slow", type=int, default=30, help="Slow EMA span")

    # Misc
    p.add_argument("--only_active", action="store_true",
                   help="If set, keep only markets that look 'active' (status == ACTIVE if present)")
    p.add_argument("--head", type=int, default=0,
                   help="If > 0, show first N rows of output preview in terminal")

    return p.parse_args()


def load_csv(path: Path) -> pd.DataFrame:
    """
    Load input CSV robustly and fail loudly if missing.
    """
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path.resolve()}")

    df = pd.read_csv(path)

    # Minimal sanity check: must have these columns for indicators to work
    required = ["ticker", "timestamp", "close_time", "yes_bid", "yes_ask", "no_bid", "no_ask"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Input CSV missing required columns: {missing}")

    return df


def save_csv(df: pd.DataFrame, path: Path) -> None:
    """
    Save output CSV.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


# ---------------------------
# Main pipeline
# ---------------------------

def main() -> int:
    args = parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    # 1) Load raw snapshots
    df = load_csv(in_path)

    # Optional: keep only active markets if you have a 'status' column
    if args.only_active and "status" in df.columns:
        df = df[df["status"].astype(str).str.upper().eq("ACTIVE")].copy()

    # 2) Compute indicators
    cfg = IndicatorConfig(
        z_window=args.z_window,
        vol_window=args.vol_window,
        range_window=args.range_window,
        momentum_lag=args.momentum_lag,
        ema_fast=args.ema_fast,
        ema_slow=args.ema_slow,
    )

    df_feat = add_indicators(df, cfg=cfg)

    # 3) Optional: compute tradability features/score
    # Uncomment if/when you have a function that adds tradability columns.
    #
    # df_feat = add_tradability(df_feat)

    # 4) Save enriched output
    save_csv(df_feat, out_path)

    # 5) Quick terminal summary (helps you confirm it worked)
    print(f"Loaded rows: {len(df):,} from {in_path}")
    print(f"Wrote rows : {len(df_feat):,} to   {out_path}")
    print(f"Columns now: {len(df_feat.columns)} (added indicators)")

    # Show a preview if requested
    if args.head and args.head > 0:
        cols_preview = [
            "timestamp", "ticker", "mid_yes", "mid_no", "p_yes",
            "spread_yes", "rel_spread_yes", "overround",
            "delta_p", "z_p", "vol_p", "ema_diff", "tte_hours",
        ]
        cols_preview = [c for c in cols_preview if c in df_feat.columns]
        print("\nPreview:")
        print(df_feat[cols_preview].head(args.head).to_string(index=False))

    return 0


if __name__ == "__main__":
    # Ensures nice exit codes if something fails
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
