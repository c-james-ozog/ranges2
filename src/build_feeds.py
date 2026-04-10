from pathlib import Path
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from src.config import CONTRACTS, TICK_SIZES
from src.scraper import fetch_yahoo_history, parse_rows, round_to_tick, format_tick


HOME_ORDER = [
    "CCK26", "KCK26", "HGK26", "ZCN26", "ZCZ26", "CTK26", "CLM26", "GFK26",
    "GCJ26", "KEN26", "HEM26", "LEJ26", "NQM26", "NGM26", "ZRN26", "ESM26",
    "SIM26", "ZMN26", "ZLN26", "ZSN26", "ZSX26", "DXM26", "ZWN26",
]


def chicago_date_from_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")


def pct_str(numerator: float, denominator: float) -> str:
    if not denominator:
        return ""
    return f"{round((numerator / denominator) * 100, 1)}%"


def historic_vol_str(current_range: float, lookback_rows: list, tick: float) -> str:
    """
    Historic vol = current daily range relative to average of next 10 historical daily ranges.
    Uses tick-rounded daily ranges for parity with displayed values.
    """
    if len(lookback_rows) < 10:
        return ""

    lookback_ranges = [
        round_to_tick(r["high"] - r["low"], tick)
        for r in lookback_rows[:10]
    ]
    avg_range = sum(lookback_ranges) / 10

    if avg_range == 0:
        return ""

    return f"{round((current_range / avg_range) * 100, 1)}%"


def compute_daily_target(rows: list, i: int, tick: float):
    """
    Daily target = 0.8 * average of previous 3 trading-day daily ranges,
    with each prior range tick-rounded first, then result tick-rounded again.
    """
    prev3 = rows[i + 1:i + 4]
    if len(prev3) < 3:
        return None

    prev3_ranges = [
        round_to_tick(x["high"] - x["low"], tick)
        for x in prev3
    ]

    target_raw = (sum(prev3_ranges) / 3) * 0.8
    return round_to_tick(target_raw, tick)


def compute_weekly_block(rows: list, start_idx: int, tick: float):
    """
    A weekly block is a 5-trading-day rolling window starting at start_idx.
    Returns rounded weekly high, low, and range.
    """
    block = rows[start_idx:start_idx + 5]
    if len(block) < 5:
        return None

    weekly_high_raw = max(x["high"] for x in block)
    weekly_low_raw = min(x["low"] for x in block)

    weekly_high = round_to_tick(weekly_high_raw, tick)
    weekly_low = round_to_tick(weekly_low_raw, tick)
    weekly_range = round_to_tick(weekly_high - weekly_low, tick)

    return {
        "high": weekly_high,
        "low": weekly_low,
        "range": weekly_range,
    }


def compute_weekly_target(rows: list, i: int, tick: float):
    """
    Weekly target = 0.8 * average of previous 3 weekly ranges,
    where each weekly range comes from the next 3 prior 5-trading-day blocks.
    Each prior weekly range is tick-rounded first, then result tick-rounded again.
    """
    prior_blocks = []
    for start in (i + 5, i + 10, i + 15):
        block = compute_weekly_block(rows, start, tick)
        if block is None:
            return None
        prior_blocks.append(block["range"])

    target_raw = (sum(prior_blocks) / 3) * 0.8
    return round_to_tick(target_raw, tick)


def build_history(rows: list, contract: dict) -> list:
    tick = TICK_SIZES[contract["commodity"]]
    history_rows = []

    for i, r in enumerate(rows):
        # Daily values
        daily_high = round_to_tick(r["high"], tick)
        daily_low = round_to_tick(r["low"], tick)
        daily_range = round_to_tick(daily_high - daily_low, tick)

        daily_target_value = compute_daily_target(rows, i, tick)
        daily_target = format_tick(daily_target_value, tick) if daily_target_value is not None else ""
        daily_achievement = pct_str(daily_range, daily_target_value) if daily_target_value else ""

        hist_vol = historic_vol_str(daily_range, rows[i + 1:i + 21], tick)

        # Weekly values
        weekly_block = compute_weekly_block(rows, i, tick)
        if weekly_block is not None:
            weekly_high = weekly_block["high"]
            weekly_low = weekly_block["low"]
            weekly_range = weekly_block["range"]

            weekly_target_value = compute_weekly_target(rows, i, tick)
            weekly_target = format_tick(weekly_target_value, tick) if weekly_target_value is not None else ""
            weekly_achievement = pct_str(weekly_range, weekly_target_value) if weekly_target_value else ""
        else:
            weekly_high = None
            weekly_low = None
            weekly_range = None
            weekly_target_value = None
            weekly_target = ""
            weekly_achievement = ""

        history_rows.append({
            "date": chicago_date_from_ts(r["timestamp"]),
            "dailyTarget": daily_target,
            "dailyRange": format_tick(daily_range, tick),
            "dailyHigh": format_tick(daily_high, tick),
            "dailyLow": format_tick(daily_low, tick),
            "dailyAchievement": daily_achievement,
            "historicVol": hist_vol,
            "impliedVol": "",
            "weeklyRange": format_tick(weekly_range, tick) if weekly_range is not None else "",
            "weeklyHigh": format_tick(weekly_high, tick) if weekly_high is not None else "",
            "weeklyLow": format_tick(weekly_low, tick) if weekly_low is not None else "",
            "weeklyAchievement": weekly_achievement,
            "weeklyTarget": weekly_target,
        })

    return history_rows


def main():
    out = Path("feeds")
    history_dir = out / "history"
    cache_dir = out / "cache"

    out.mkdir(exist_ok=True)
    history_dir.mkdir(exist_ok=True)
    cache_dir.mkdir(exist_ok=True)

    daily_feed = []
    weekly_feed = []
    previous_ranges_feed = []
    errors = []
    history_index = []

    now_ct = datetime.now(ZoneInfo("America/Chicago")).isoformat()

    for contract in CONTRACTS:
        try:
            rows = fetch_yahoo_history(contract["symbol"])
            parsed = parse_rows(rows, contract["commodity"])
            clean = contract["base_symbol"]

            daily_feed.append({
                "symbol": clean,
                "dailyHigh": parsed["dailyHigh"],
                "dailyLow": parsed["dailyLow"],
                "asOf": now_ct,
                "isStale": False,
            })

            weekly_feed.append({
                "symbol": clean,
                "weeklyHigh": parsed["weeklyHigh"],
                "weeklyLow": parsed["weeklyLow"],
                "asOf": now_ct,
                "isStale": False,
            })

            previous_ranges_feed.append({
                "symbol": clean,
                "previousDailyRanges": parsed["previousDailyRanges"],
                "previousWeeklyRanges": parsed["previousWeeklyRanges"],
            })

            history_rows = build_history(rows, contract)
            history_payload = {
                "symbol": clean,
                "commodity": contract["commodity"],
                "month": contract["month"],
                "updatedAt": now_ct,
                "rows": history_rows,
            }

            (history_dir / f"{clean}.json").write_text(
                json.dumps(history_payload, indent=2),
                encoding="utf-8",
            )

            history_index.append(clean)
            print("OK", contract["symbol"])

        except Exception as exc:
            errors.append({"symbol": contract["symbol"], "error": str(exc)})
            print("ERR", contract["symbol"], exc)

        time.sleep(0.5)

    history_index.sort(key=lambda s: HOME_ORDER.index(s) if s in HOME_ORDER else 9999)

    (out / "daily-feed-full.json").write_text(json.dumps(daily_feed, indent=2), encoding="utf-8")
    (out / "weekly-feed-full.json").write_text(json.dumps(weekly_feed, indent=2), encoding="utf-8")
    (out / "previous-ranges-feed-full.json").write_text(json.dumps(previous_ranges_feed, indent=2), encoding="utf-8")
    (out / "errors.json").write_text(json.dumps(errors, indent=2), encoding="utf-8")

    meta = {
        "builtAt": now_ct,
        "status": "ok" if not errors else "partial",
        "successCount": len(CONTRACTS) - len(errors),
        "errorCount": len(errors),
        "version": "v2.1-export",
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    (history_dir / "index.json").write_text(
        json.dumps({"contracts": history_index}, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
