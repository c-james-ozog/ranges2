import json
from urllib.request import urlopen, Request
from src.config import TICK_SIZES

def round_to_tick(value: float, tick: float) -> float:
    return round(round(value / tick) * tick, 10)

def format_tick(value: float, tick: float) -> str:
    decimals = len(str(tick).split(".")[1]) if "." in str(tick) else 0
    out = f"{round(value, decimals):.{decimals}f}" if decimals else str(int(round(value)))
    return out.rstrip("0").rstrip(".") if "." in out else out

def fetch_yahoo_history(symbol: str):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=3mo&interval=1d&includePrePost=false&events=div%2Csplits"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        data = json.load(resp)
    result = data["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]
    rows = []
    for ts, high, low in zip(result.get("timestamp", []), quote.get("high", []), quote.get("low", [])):
        if high is None or low is None:
            continue
        rows.append({"timestamp": ts, "high": float(high), "low": float(low)})
    rows.sort(key=lambda x: x["timestamp"], reverse=True)
    return rows

def parse_rows(rows, commodity):
    tick = TICK_SIZES[commodity]
    latest_day = rows[0]
    previous_daily_ranges = [format_tick(round_to_tick(r["high"] - r["low"], tick), tick) for r in rows[1:4]]
    latest_five = rows[:5]
    weekly_high = format_tick(max(r["high"] for r in latest_five), tick) if len(latest_five) == 5 else ""
    weekly_low = format_tick(min(r["low"] for r in latest_five), tick) if len(latest_five) == 5 else ""
    previous_weekly_ranges = []
    for start in (5, 10, 15):
        block = rows[start:start+5]
        if len(block) < 5:
            continue
        previous_weekly_ranges.append(
            format_tick(
                round_to_tick(max(r["high"] for r in block) - min(r["low"] for r in block), tick),
                tick
            )
        )
    return {
        "dailyHigh": format_tick(round_to_tick(latest_day["high"], tick), tick),
        "dailyLow": format_tick(round_to_tick(latest_day["low"], tick), tick),
        "weeklyHigh": weekly_high,
        "weeklyLow": weekly_low,
        "previousDailyRanges": previous_daily_ranges,
        "previousWeeklyRanges": previous_weekly_ranges,
    }
