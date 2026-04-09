from pathlib import Path
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from src.config import CONTRACTS, TICK_SIZES
from src.scraper import fetch_yahoo_history, parse_rows, round_to_tick, format_tick


HOME_ORDER = [
    "CCK26",
    "KCK26",
    "HGK26",
    "ZCN26",
    "ZCZ26",
    "CTK26",
    "CLM26",
    "GFK26",
    "GCJ26",
    "KEN26",
    "HEM26",
    "LEJ26",
    "NQM26",
    "NGM26",
    "ZRN26",
    "ESM26",
    "SIM26",
    "ZMN26",
    "ZLN26",
    "ZSN26",
    "ZSX26",
    "DXM26",
    "ZWN26",
]


def chicago_date_from_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")


def range_for_row(row: dict, tick: float) -> str:
    return format_tick(round_to_tick(row["high"] - row["low"], tick), tick)


def average_range(rows: list, tick: float, start: int, count: int) -> str:
    block = rows[start:start + count]
    if len(block) < count:
        return ""
    values = [r["high"] - r["low"] for r in block]
    avg = sum(values) / len(values)
    return format_tick(round_to_tick(avg, tick), tick)


def range_bounds_for_week(rows: list, tick: float, start: int) -> tuple[str, str, str]:
    block = rows[start:start + 5]
    if len(block) < 5:
        return "", "", ""
    high = max(r["high"] for r in block)
    low = min(r["low"] for r in block)
    rng = high - low
    return (
        format_tick(round_to_tick(rng, tick), tick),
        format_tick(round_to_tick(high, tick), tick),
        format_tick(round_to_tick(low, tick), tick),
    )


def pct_str(numerator: float, denominator: float) -> str:
    if not denominator:
        return ""
    return f"{round((numerator / denominator) * 100, 1)}%"


def historic_vol_str(current_range: float, lookback_rows: list) -> str:
    if len(lookback_rows) < 10:
        return ""
    avg_range = sum(r["high"] - r["low"] for r in lookback_rows[:10]) / 10
    if avg_range == 0:
        return ""
    return f"{round((current_range / avg_range) * 100, 1)}%"


def build_history(rows: list, contract: dict) -> list:
    tick = TICK_SIZES[contract["commodity"]]
    history_rows = []

    for i, r in enumerate(rows):
        current_range_raw = r["high"] - r["low"]
        current_range = format_tick(round_to_tick(current_range_raw, tick), tick)

        # placeholder target logic
        prev3 = rows[i + 1:i + 4]
        if len(prev3) == 3:
            daily_target_raw = sum(x["high"] - x["low"] for x in prev3) / 3
        else:
            daily_target_raw = current_range_raw

        daily_target = format_tick(round_to_tick(daily_target_raw, tick), tick)
        daily_achievement = pct_str(current_range_raw, daily_target_raw)

        # placeholder historic vol
        hist_vol = historic_vol_str(current_range_raw, rows[i + 1:i + 21])

        # rolling 5-day week ending on this row
        week_block = rows[i:i + 5]
        if len(week_block) == 5:
            weekly_high_raw = max(x["high"] for x in week_block)
            weekly_low_raw = min(x["low"] for x in week_block)
            weekly_range_raw = weekly_high_raw - weekly_low_raw
            prev_week_blocks = rows[i + 5:i + 20]
            if len(prev_week_blocks) >= 15:
                weekly_target_raw = (
                    (max(x["high"] for x in rows[i + 5:i + 10]) - min(x["low"] for x in rows[i + 5:i + 10])) +
                    (max(x["high"] for x in rows[i + 10:i + 15]) - min(x["low"] for x in rows[i + 10:i + 15])) +
                    (max(x["high"] for x in rows[i + 15:i + 20]) - min(x["low"] for x in rows[i + 15:i + 20]))
                ) / 3
            else:
                weekly_target_raw = weekly_range_raw

            weekly_range = format_tick(round_to_tick(weekly_range_raw, tick), tick)
            weekly_high = format_tick(round_to_tick(weekly_high_raw, tick), tick)
            weekly_low = format_tick(round_to_tick(weekly_low_raw, tick), tick)
            weekly_target = format_tick(round_to_tick(weekly_target_raw, tick), tick)
            weekly_achievement = pct_str(weekly_range_raw, weekly_target_raw)
        else:
            weekly_range = ""
            weekly_high = ""
            weekly_low = ""
            weekly_target = ""
            weekly_achievement = ""

        history_rows.append({
            "date": chicago_date_from_ts(r["timestamp"]),
            "dailyTarget": daily_target,
            "dailyRange": current_range,
            "dailyHigh": format_tick(round_to_tick(r["high"], tick), tick),
            "dailyLow": format_tick(round_to_tick(r["low"], tick), tick),
            "dailyAchievement": daily_achievement,
            "historicVol": hist_vol,
            "impliedVol": "",
            "weeklyRange": weekly_range,
            "weeklyHigh": weekly_high,
            "weeklyLow": weekly_low,
            "weeklyAchievement": weekly_achievement,
            "weeklyTarget": weekly_target
        })

    return history_rows


def build_overview(rows: list, contract: dict) -> dict:
    tick = TICK_SIZES[contract["commodity"]]
    latest = rows[0]
    latest_range_raw = latest["high"] - latest["low"]
    latest_range = format_tick(round_to_tick(latest_range_raw, tick), tick)

    # "current day target" placeholder: average of previous 3 daily ranges
    prev3 = rows[1:4]
    if len(prev3) == 3:
        daily_target_raw = sum(r["high"] - r["low"] for r in prev3) / 3
    else:
        daily_target_raw = latest_range_raw
    daily_target = format_tick(round_to_tick(daily_target_raw, tick), tick)
    daily_achievement = pct_str(latest_range_raw, daily_target_raw)

    # "next day target" placeholder: average of latest + previous 2 daily ranges
    seed3 = rows[0:3]
    if len(seed3) == 3:
        next_daily_target_raw = sum(r["high"] - r["low"] for r in seed3) / 3
    else:
        next_daily_target_raw = latest_range_raw
    next_daily_target = format_tick(round_to_tick(next_daily_target_raw, tick), tick)

    # placeholder next-day bounds centered on latest midpoint
    midpoint = (latest["high"] + latest["low"]) / 2
    half_target = next_daily_target_raw / 2
    next_daily_high = format_tick(round_to_tick(midpoint + half_target, tick), tick)
    next_daily_low = format_tick(round_to_tick(midpoint - half_target, tick), tick)

    hist_vol = historic_vol_str(latest_range_raw, rows[1:21])

    # week = latest rolling 5-session block
    if len(rows) >= 5:
        weekly_high_raw = max(r["high"] for r in rows[0:5])
        weekly_low_raw = min(r["low"] for r in rows[0:5])
        weekly_range_raw = weekly_high_raw - weekly_low_raw
        weekly_high = format_tick(round_to_tick(weekly_high_raw, tick), tick)
        weekly_low = format_tick(round_to_tick(weekly_low_raw, tick), tick)
        weekly_range = format_tick(round_to_tick(weekly_range_raw, tick), tick)
    else:
        weekly_high_raw = 0
        weekly_low_raw = 0
        weekly_range_raw = 0
        weekly_high = ""
        weekly_low = ""
        weekly_range = ""

    # weekly target placeholder: average of prior 3 weekly blocks
    weekly_blocks = []
    for start in (5, 10, 15):
        block = rows[start:start + 5]
        if len(block) == 5:
            weekly_blocks.append(max(r["high"] for r in block) - min(r["low"] for r in block))

    if weekly_blocks:
        weekly_target_raw = sum(weekly_blocks) / len(weekly_blocks)
        weekly_target = format_tick(round_to_tick(weekly_target_raw, tick), tick)
        weekly_achievement = pct_str(weekly_range_raw, weekly_target_raw) if weekly_range_raw else ""
    else:
        weekly_target_raw = 0
        weekly_target = ""
        weekly_achievement = ""

    return {
        "symbol": contract["base_symbol"],
        "commodity": contract["commodity"],
        "month": contract["month"],
        "dailyTarget": daily_target,
        "dailyRange": latest_range,
        "dailyHigh": format_tick(round_to_tick(latest["high"], tick), tick),
        "dailyLow": format_tick(round_to_tick(latest["low"], tick), tick),
        "dailyAchievement": daily_achievement,
        "nextDailyTarget": next_daily_target,
        "nextDailyHigh": next_daily_high,
        "nextDailyLow": next_daily_low,
        "historicVol": hist_vol,
        "impliedVol": "",
        "weeklyRange": weekly_range,
        "weeklyHigh": weekly_high,
        "weeklyLow": weekly_low,
        "weeklyAchievement": weekly_achievement,
        "weeklyTarget": weekly_target
    }


def main():
    out = Path("feeds")
    history_dir = out / "history"

    out.mkdir(exist_ok=True)
    history_dir.mkdir(exist_ok=True)

    daily_feed = []
    weekly_feed = []
    previous_ranges_feed = []
    overview_feed = []
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
                "isStale": False
            })

            weekly_feed.append({
                "symbol": clean,
                "weeklyHigh": parsed["weeklyHigh"],
                "weeklyLow": parsed["weeklyLow"],
                "asOf": now_ct,
                "isStale": False
            })

            previous_ranges_feed.append({
                "symbol": clean,
                "previousDailyRanges": parsed["previousDailyRanges"],
                "previousWeeklyRanges": parsed["previousWeeklyRanges"]
            })

            overview_feed.append(build_overview(rows, contract))

            history_rows = build_history(rows, contract)
            history_payload = {
                "symbol": clean,
                "commodity": contract["commodity"],
                "month": contract["month"],
                "updatedAt": now_ct,
                "rows": history_rows
            }

            (history_dir / f"{clean}.json").write_text(
                json.dumps(history_payload, indent=2),
                encoding="utf-8"
            )

            history_index.append(clean)
            print("OK", contract["symbol"])

        except Exception as exc:
            errors.append({
                "symbol": contract["symbol"],
                "error": str(exc)
            })
            print("ERR", contract["symbol"], exc)

        time.sleep(0.5)

    order_map = {symbol: i for i, symbol in enumerate(HOME_ORDER)}
    overview_feed.sort(key=lambda x: order_map.get(x["symbol"], 9999))

    (out / "daily-feed-full.json").write_text(
        json.dumps(daily_feed, indent=2),
        encoding="utf-8"
    )
    (out / "weekly-feed-full.json").write_text(
        json.dumps(weekly_feed, indent=2),
        encoding="utf-8"
    )
    (out / "previous-ranges-feed-full.json").write_text(
        json.dumps(previous_ranges_feed, indent=2),
        encoding="utf-8"
    )
    (out / "overview-feed.json").write_text(
        json.dumps(overview_feed, indent=2),
        encoding="utf-8"
    )
    (out / "errors.json").write_text(
        json.dumps(errors, indent=2),
        encoding="utf-8"
    )

    meta = {
        "builtAt": now_ct,
        "status": "ok" if not errors else "partial",
        "successCount": len(CONTRACTS) - len(errors),
        "errorCount": len(errors),
        "version": "v2.1"
    }
    (out / "meta.json").write_text(
        json.dumps(meta, indent=2),
        encoding="utf-8"
    )

    (history_dir / "index.json").write_text(
        json.dumps({"contracts": history_index}, indent=2),
        encoding="utf-8"
    )


if __name__ == "__main__":
    main()
