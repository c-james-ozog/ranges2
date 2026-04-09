import json
import math
from datetime import datetime, timezone
from statistics import mean, pstdev
from urllib.request import Request, urlopen

from .config import DEFAULT_IMPLIED_VOL, SETTINGS, TICK_SIZES


def round_to_tick(value: float, tick: float) -> float:
    return round(round(value / tick) * tick, 10)


def format_tick(value: float, tick: float) -> str:
    decimals = len(str(tick).split(".")[1]) if "." in str(tick) else 0
    out = f"{round(value, decimals):.{decimals}f}" if decimals else str(int(round(value)))
    return out.rstrip("0").rstrip(".") if "." in out else out


def as_float(value):
    if value in (None, "", "-"):
        return None
    return float(value)


def format_percent(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}%"


def ts_to_datestr(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def fetch_yahoo_history(symbol: str):
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?range={SETTINGS['history_range']}&interval={SETTINGS['history_interval']}"
        "&includePrePost=false&events=div%2Csplits"
    )
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        data = json.load(resp)
    result = data["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]
    rows = []
    for ts, high, low, close in zip(
        result.get("timestamp", []),
        quote.get("high", []),
        quote.get("low", []),
        quote.get("close", []),
    ):
        if high is None or low is None or close is None:
            continue
        rows.append(
            {
                "timestamp": ts,
                "date": ts_to_datestr(ts),
                "high": float(high),
                "low": float(low),
                "close": float(close),
            }
        )
    rows.sort(key=lambda x: x["timestamp"], reverse=True)
    return rows


def daily_range_value(row: dict) -> float:
    return float(row["high"] - row["low"])


def historic_vol_percent(window_rows: list[dict]) -> float | None:
    if len(window_rows) < 5:
        return None
    closes = [r["close"] for r in window_rows]
    returns = []
    for prev, curr in zip(closes[1:], closes[:-1]):
        if prev <= 0 or curr <= 0:
            continue
        returns.append(math.log(curr / prev))
    if len(returns) < 2:
        return None
    return pstdev(returns) * math.sqrt(252) * 100


def implied_target_from_close(close: float, implied_vol_percent: float) -> float:
    return close * (implied_vol_percent / 100.0) / math.sqrt(252)


def build_history_rows(rows: list[dict], commodity: str):
    tick = TICK_SIZES[commodity]
    lookback = SETTINGS["historic_lookback_days"]
    implied_vol = DEFAULT_IMPLIED_VOL.get(commodity)
    history = []

    for idx, row in enumerate(rows):
        daily_range = round_to_tick(daily_range_value(row), tick)
        target_daily = None
        if idx + 1 < len(rows):
            prev_window = rows[idx + 1: idx + 1 + lookback]
            if len(prev_window) >= 5:
                target_daily = round_to_tick(mean(daily_range_value(r) for r in prev_window), tick)

        hv = historic_vol_percent(rows[idx: idx + lookback])
        implied_daily_target = None
        if implied_vol is not None:
            implied_daily_target = round_to_tick(implied_target_from_close(row["close"], implied_vol), tick)

        week_block = rows[idx: idx + 5]
        weekly_range = None
        if len(week_block) == 5:
            weekly_range = round_to_tick(max(r["high"] for r in week_block) - min(r["low"] for r in week_block), tick)

        target_weekly = None
        prior_week_blocks = []
        for start in range(idx + 5, len(rows), 5):
            block = rows[start:start + 5]
            if len(block) == 5:
                prior_week_blocks.append(block)
            if len(prior_week_blocks) == 4:
                break
        if prior_week_blocks:
            target_weekly = round_to_tick(
                mean(max(b["high"] for b in block) - min(b["low"] for b in block) for block in prior_week_blocks),
                tick,
            )

        daily_achievement = None
        if target_daily and target_daily > 0:
            daily_achievement = (daily_range / target_daily) * 100
        weekly_achievement = None
        if target_weekly and weekly_range and target_weekly > 0:
            weekly_achievement = (weekly_range / target_weekly) * 100

        history.append(
            {
                "date": row["date"],
                "dailyTarget": format_tick(target_daily, tick) if target_daily is not None else "-",
                "dailyRange": format_tick(daily_range, tick),
                "dailyRangeLabel": f"{format_tick(round_to_tick(row['low'], tick), tick)} - {format_tick(round_to_tick(row['high'], tick), tick)}",
                "dailyAchievement": format_percent(daily_achievement),
                "historicVol": format_percent(hv),
                "impliedVol": format_percent(implied_vol),
                "impliedTarget": format_tick(implied_daily_target, tick) if implied_daily_target is not None else "-",
                "weeklyRange": format_tick(weekly_range, tick) if weekly_range is not None else "-",
                "weeklyRangeLabel": (
                    f"{format_tick(round_to_tick(min(r['low'] for r in week_block), tick), tick)} - "
                    f"{format_tick(round_to_tick(max(r['high'] for r in week_block), tick), tick)}"
                ) if len(week_block) == 5 else "-",
                "weeklyAchievement": format_percent(weekly_achievement),
                "weeklyTarget": format_tick(target_weekly, tick) if target_weekly is not None else "-",
            }
        )
    return history


def latest_summary(history_rows: list[dict]):
    latest = history_rows[0]
    prev_daily = [r["dailyRange"] for r in history_rows[1:1 + SETTINGS["previous_daily_count"]]]
    prev_weekly = []
    weekly_seen = 0
    i = 5
    while i < len(history_rows) and weekly_seen < SETTINGS["previous_weekly_count"]:
        row = history_rows[i]
        if row["weeklyRange"] != "-":
            prev_weekly.append(row["weeklyRange"])
            weekly_seen += 1
        i += 5
    return {
        "dailyHigh": latest["dailyRangeLabel"].split(" - ")[1] if " - " in latest["dailyRangeLabel"] else "",
        "dailyLow": latest["dailyRangeLabel"].split(" - ")[0] if " - " in latest["dailyRangeLabel"] else "",
        "weeklyHigh": latest["weeklyRangeLabel"].split(" - ")[1] if " - " in latest["weeklyRangeLabel"] else "",
        "weeklyLow": latest["weeklyRangeLabel"].split(" - ")[0] if " - " in latest["weeklyRangeLabel"] else "",
        "previousDailyRanges": prev_daily,
        "previousWeeklyRanges": prev_weekly,
        "historicVol": latest["historicVol"],
        "impliedVol": latest["impliedVol"],
        "dailyTarget": latest["dailyTarget"],
        "weeklyTarget": latest["weeklyTarget"],
        "dailyRange": latest["dailyRange"],
        "weeklyRange": latest["weeklyRange"],
        "dailyAchievement": latest["dailyAchievement"],
        "weeklyAchievement": latest["weeklyAchievement"],
        "impliedTarget": latest["impliedTarget"],
        "date": latest["date"],
    }
