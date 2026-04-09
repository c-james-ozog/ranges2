import json
from urllib.request import urlopen, Request

def fetch_yahoo_history(symbol: str):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=3mo&interval=1d"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    with urlopen(req, timeout=30) as resp:
        data = json.load(resp)

    result = data["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]

    rows = []
    for ts, high, low in zip(result["timestamp"], quote["high"], quote["low"]):
        if high and low:
            rows.append({
                "timestamp": ts,
                "high": float(high),
                "low": float(low)
            })

    rows.sort(key=lambda x: x["timestamp"], reverse=True)
    return rows
