import requests
import csv
import time

# Kalshi API endpoint for market data
URL = "https://api.kalshi.com/trade-api/v2/markets"

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
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "volume",
        "open_interest"
    ])

    # Loop through each market returned by the API
    for market in data["markets"]:
        writer.writerow([
            time.time(),                  # Time data was collected
            market["ticker"],             # Unique market identifier
            market["yes_bid"],            # Best bid for YES
            market["yes_ask"],            # Best ask for YES
            market["no_bid"],             # Best bid for NO
            market["no_ask"],             # Best ask for NO
            market["volume"],             # Total traded volume
            market["open_interest"]       # Open contracts
        ])