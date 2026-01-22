import requests
import csv
import time

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