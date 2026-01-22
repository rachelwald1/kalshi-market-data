import requests
import csv
import time
from tradability import tradability_score

# Kalshi API endpoint for market data
URL = "https://api.elections.kalshi.com/trade-api/v2/markets"

# Request all currently open markets from Kalshi
response = requests.get(URL, params={"status": "open"})
data = response.json()

# Open a CSV file to store market data
with open("kalshi_markets.csv", "w", newline="") as f:
    writer = csv.writer(f)

    # Write CSV header
    writer.writerow([
        "timestamp",
        "ticker",
        "title",
        "event_ticker",
        "category",
        "status",
        "close_time",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "volume",
        "open_interest",
        "last_trade_price",
    ])

    # Loop through each market returned by the API
    for market in data["markets"]:
        # Skip illiquid markets (no YES-side orders)
        if (market.get("yes_bid", 0) > 0 or market.get("yes_ask", 0) > 0):
            writer.writerow([
                time.time(),                        # Time data was collected
                market.get("ticker"),               # Unique market identifier
                market.get("title"),                # Human-readable description
                market.get("event_ticker"),         # Parent event
                market.get("category"),             # Market category
                market.get("status"),               # Market status
                market.get("close_time"),           # Expiry timestamp
                market.get("yes_bid"),              # Best bid for YES
                market.get("yes_ask"),              # Best ask for YES
                market.get("no_bid"),               # Best bid for NO
                market.get("no_ask"),               # Best ask for NO
                market.get("volume"),               # Total traded volume
                market.get("open_interest"),        # Open contracts
                market.get("last_trade_price"),     # Most recent execution price
            ])
            
def simplify_title(title: str, max_items: int = 3) -> str:
    parts = [p.strip() for p in title.split(",")]
    if len(parts) <= max_items:
        return title
    return ", ".join(parts[:max_items]) + f", … (+{len(parts) - max_items} more)"
        
markets = data["markets"]

# Score every market
for market in markets:
    score = tradability_score(market)
    market["tradability_score"] = score

# Sort by score
ranked = sorted(
    markets,
    key=lambda m: m["tradability_score"],
    reverse=True
)

# Create a list of only tradable markets
tradable = [m for m in ranked if m["tradability_score"] >= 50]

for m in tradable[:20]:
    title = simplify_title(m['title'])
    print(f"{m['tradability_score']:3d}  {title}")