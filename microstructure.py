from typing import Any, Dict, Optional

def as_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except (TypeError, ValueError):
        return default

def yes_bid(m: Dict[str, Any]) -> int:
    return as_int(m.get("yes_bid"))

def yes_ask(m: Dict[str, Any]) -> int:
    return as_int(m.get("yes_ask"))

def no_bid(m: Dict[str, Any]) -> int:
    return as_int(m.get("no_bid"))

def no_ask(m: Dict[str, Any]) -> int:
    return as_int(m.get("no_ask"))

def has_yes_book(m: Dict[str, Any]) -> bool:
    return yes_bid(m) > 0 and yes_ask(m) > 0

def mid_yes(m: Dict[str, Any]) -> Optional[float]:
    if not has_yes_book(m):
        return None
    return (yes_bid(m) + yes_ask(m)) / 2.0

def spread_yes(m: Dict[str, Any]) -> Optional[int]:
    if not has_yes_book(m):
        return None
    return yes_ask(m) - yes_bid(m)

def volume(m: Dict[str, Any]) -> int:
    return as_int(m.get("volume"))

def open_interest(m: Dict[str, Any]) -> int:
    return as_int(m.get("open_interest"))
