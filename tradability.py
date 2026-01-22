from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
import math
from microstructure import (
    yes_bid, yes_ask, no_bid, no_ask,
    has_yes_book, has_no_book,
    mid_yes, spread_yes,
    volume, open_interest,
)


def clamp(x: float, lo: float, hi: float) -> float:
    """
    Clamp a value into a closed interval [lo, hi].
    Used to keep component scores bounded.
    """
    return max(lo, min(hi, x))


def rel_spread_yes(m: Dict[str, Any]) -> Optional[float]:
    """
    Relative spread = spread / mid price.
    Penalises wide spreads at low prices more heavily.
    """
    mid = mid_yes(m)
    spr = spread_yes(m)
    if mid is None or spr is None or mid <= 0:
        return None
    return spr / mid


def best_spread(m: Dict[str, Any]) -> Optional[int]:
    """
    Choose which side of the book to evaluate:
    - Prefer YES book (more intuitive probability interpretation)
    - Fall back to NO book if YES is unavailable
    """
    if has_yes_book(m):
        return spread_yes(m)
    if has_no_book(m):
        return no_ask(m) - no_bid(m)
    return None


def book_completeness_factor(m: Dict[str, Any]) -> float:
    """
    Markets with both YES and NO books are structurally healthier.
    One-sided markets get a penalty but are not discarded.
    """
    return 1.0 if (has_yes_book(m) and has_no_book(m)) else 0.7


# ============================================================
# Normalised components (each maps to 0–1)
# ============================================================

def spread_component(spread_cents: int, max_ok: int = 10) -> float:
    """
    Convert absolute spread into a 0–1 score.
    Spreads above `max_ok` are treated as unusable.
    """
    return clamp(1.0 - (spread_cents / float(max_ok)), 0.0, 1.0)


def rel_spread_component(rel: float, worst: float = 0.30) -> float:
    """
    Convert relative spread into a 0–1 score.
    Relative spreads above `worst` are considered prohibitively wide.
    """
    return clamp(1.0 - (rel / worst), 0.0, 1.0)


def log_saturating_component(x: int, denom: float) -> float:
    """
    Diminishing-returns transform using log scaling.
    Early activity matters much more than marginal late activity.
    """
    return clamp(math.log10(1 + max(0, x)) / denom, 0.0, 1.0)


# ============================================================
# Feature bundle
# ============================================================

@dataclass(frozen=True)
class TradabilityFeatures:
    """
    Snapshot of the key quantities used to judge tradability.
    """
    has_yes_book: bool
    has_no_book: bool
    spread: Optional[int]
    rel_spread: Optional[float]
    volume: int
    open_interest: int
    book_factor: float


def features(m: Dict[str, Any]) -> TradabilityFeatures:
    """
    Collect all tradability-relevant features in one place.
    Keeps scoring logic clean and readable.
    """
    return TradabilityFeatures(
        has_yes_book=has_yes_book(m),
        has_no_book=has_no_book(m),
        spread=best_spread(m),
        rel_spread=rel_spread_yes(m),
        volume=volume(m),
        open_interest=open_interest(m),
        book_factor=book_completeness_factor(m),
    )


# ============================================================
# Public decision functions
# ============================================================

def is_tradable(
    m: Dict[str, Any],
    max_spread: int = 6,
    max_rel_spread: float = 0.15,
    min_volume: int = 20,
    min_open_interest: int = 50,
) -> bool:
    """
    Conservative binary filter:
    answers 'is this market worth considering at all?'
    """
    f = features(m)

    # No complete order book → cannot reliably trade
    if not (f.has_yes_book or f.has_no_book):
        return False

    # Spread too wide → friction dominates any edge
    if f.spread is None or f.spread > max_spread:
        return False

    # Penalise low-price markets with large relative spreads
    if f.rel_spread is not None and f.rel_spread > max_rel_spread:
        return False

    # Require evidence of participation (historical or current)
    if (f.volume < min_volume) and (f.open_interest < min_open_interest):
        return False

    return True


def tradability_score(m: Dict[str, Any]) -> int:
    """
    Continuous 0–100 score estimating execution quality.
    Higher = tighter pricing, more activity, healthier structure.
    """
    f = features(m)

    # If nothing can be executed, score is zero
    if not (f.has_yes_book or f.has_no_book) or f.spread is None:
        return 0

    # Spread quality (absolute and relative)
    abs_sp = spread_component(f.spread, max_ok=10)
    rel = f.rel_spread if f.rel_spread is not None else 0.20
    rel_sp = rel_spread_component(rel, worst=0.30)

    # Activity signals (log-scaled to avoid domination by large markets)
    vol_c = log_saturating_component(f.volume, denom=2.0)
    oi_c = log_saturating_component(f.open_interest, denom=3.0)

    # Weighted sum: spread dominates, activity and structure support
    score = (
        40 * abs_sp +
        15 * rel_sp +
        20 * vol_c +
        15 * oi_c +
        10 * f.book_factor
    )

    return int(round(clamp(score, 0.0, 100.0)))
