from pathlib import Path
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from src.config import CONTRACTS, TICK_SIZES
from src.scraper import fetch_yahoo_history, round_to_tick, format_tick


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


def compute_daily_range(row: dict, tick: float) -> float:
    high = round_to_tick(row["high"], tick)
    low = round_to_tick(row["low"], tick)
    return round_to_tick(high - low, tick)


def compute_weekly_block(rows: list, start_idx: int, tick: float):
    """
    Rolling 5-trading-day block starting at start_idx.
    rows are expected newest -> oldest.
    """
    block = rows[start_idx:start_idx + 5]
    if len(block) < 5:
        return None

    high = round_to_tick(max(x["high"] for x in block), tick)
    low = round_to_tick(min(x["low"] for x in block), tick)
    rng = round_to_tick(high - low, tick)

    return {
        "high": high,
        "low": low,
        "range": rng,
    }


def compute_daily_full_achievement(rows: list, i: int, tick: float):
    """
    Full achievement = average of current day range + previous 2 trading-day ranges.
    Since rows are newest -> oldest, that is rows[i], rows[i+1], rows[i+2].
    """
    window = rows[i:i + 3]
    if len(window) < 3:
        return None

    ranges = [compute_daily_range(x, tick) for x in window]
    avg = sum(ranges) / 3
    return round_to_tick(avg, tick)


def compute_next_daily_target(rows: list, i: int, tick: float):
    """
    Next target = 80% of full achievement.
    """
    full_achievement = compute_daily_full_achievement(rows, i, tick)
    if full_achievement is None:
        return None
    return round_to_tick(full_achievement * 0.8, tick)


def compute_weekly_full_achievement(rows: list, i: int, tick: float):
    """
    Weekly full achievement = average of current rolling weekly range
    plus the next 2 older rolling weekly ranges.
    """
    ranges = []
    for start in (i, i + 5, i + 10):
        block = compute_weekly_block(rows, start, tick)
        if block is None:
            return None
        ranges.append(block["range"])

    avg = sum(ranges) / 3
    return round_to_tick(avg, tick)


def compute_next_weekly_target(rows: list, i: int, tick: float):
    full_achievement = compute_weekly_full_achievement(rows, i, tick)
    if full_achievement is None:
        return None
    return round_to_tick(full_achievement * 0.8, tick)


def build_history(rows: list, contract: dict) -> list:
    tick = TICK_SIZES[contract["commodity"]]
    history_rows = []

    # First pass: calculate row-local values and forward-looking next targets
    for i, r in enumerate(rows):
        daily_high = round_to_tick(r["high"], tick)
        daily_low = round_to_tick(r["low"], tick)
        daily_range = round_to_tick(daily_high - daily_low, tick)

        full_achievement_value = compute_daily_full_achievement(rows, i, tick)
        next_daily_target_value = compute_next_daily_target(rows, i, tick)

        hist_vol = historic_vol_str(daily_range, rows[i + 1:i + 21], tick)

        weekly_block = compute_weekly_block(rows, i, tick)
        if weekly_block is not None:
            weekly_high = weekly_block["high"]
            weekly_low = weekly_block["low"]
            weekly_range = weekly_block["range"]

            weekly_full_achievement_value = compute_weekly_full_achievement(rows, i, tick)
            next_weekly_target_value = compute_next_weekly_target(rows, i, tick)
        else:
            weekly_high = None
            weekly_low = None
            weekly_range = None
            weekly_full_achievement_value = None
            next_weekly_target_value = None

        history_rows.append({
            "date": chicago_date_from_ts(r["timestamp"]),
            "dailyHigh": format_tick(daily_high, tick),
            "dailyLow": format_tick(daily_low, tick),
            "dailyRange": format_tick(daily_range, tick),
            "fullAchievement": format_tick(full_achievement_value, tick) if full_achievement_value is not None else "",
            "fullAchievementValue": full_achievement_value,
            "nextDailyTarget": format_tick(next_daily_target_value, tick) if next_daily_target_value is not None else "",
            "nextDailyTargetValue": next_daily_target_value,
            "historicVol": hist_vol,
            "impliedVol": "",
            "weeklyHigh": format_tick(weekly_high, tick) if weekly_high is not None else "",
            "weeklyLow": format_tick(weekly_low, tick) if weekly_low is not None else "",
            "weeklyRange": format_tick(weekly_range, tick) if weekly_range is not None else "",
            "weeklyFullAchievement": format_tick(weekly_full_achievement_value, tick) if weekly_full_achievement_value is not None else "",
            "weeklyFullAchievementValue": weekly_full_achievement_value,
            "nextWeeklyTarget": format_tick(next_weekly_target_value, tick) if next_weekly_target_value is not None else "",
            "nextWeeklyTargetValue": next_weekly_target_value,
        })

    # Second pass: current targets come from prior row's next targets
    for i, row in enumerate(history_rows):
        prev_row = history_rows[i + 1] if i + 1 < len(history_rows) else None

        daily_target_value = prev_row["nextDailyTargetValue"] if prev_row else None
        weekly_target_value = prev_row["nextWeeklyTargetValue"] if prev_row else None

        daily_range_value = row["dailyRange"]
        weekly_range_value = row["weeklyRange"]

        daily_range_num = float(daily_range_value) if daily_range_value else None
        weekly_range_num = float(weekly_range_value) if weekly_range_value else None

        row["dailyTarget"] = format_tick(daily_target_value, tick) if daily_target_value is not None else ""
        row["dailyAchievement"] = pct_str(daily_range_num, daily_target_value) if daily_range_num is not None and daily_target_value else ""

        row["weeklyTarget"] = format_tick(weekly_target_value, tick) if weekly_target_value is not None else ""
        row["weeklyAchievement"] = pct_str(weekly_range_num, weekly_target_value) if weekly_range_num is not None and weekly_target_value else ""

        # remove internal numeric helpers from final JSON
        del row["fullAchievementValue"]
        del row["nextDailyTargetValue"]
        del row["weeklyFullAchievementValue"]
        del row["nextWeeklyTargetValue"]

    return history_rows


def build_overview_rows_from_history(history_payload: dict) -> list:
    rows = history_payload["rows"]
    overview_rows = []

    for row in rows:
        overview_rows.append({
            "date": row["date"],
            "symbol": history_payload["symbol"],
            "commodity": history_payload["commodity"],
            "month": history_payload["month"],
            "dailyTarget": row["dailyTarget"],
            "dailyRange": row["dailyRange"],
            "dailyHigh": row["dailyHigh"],
            "dailyLow": row["dailyLow"],
            "dailyAchievement": row["dailyAchievement"],
            "fullAchievement": row["fullAchievement"],
            "nextDailyTarget": row["nextDailyTarget"],
            "historicVol": row["historicVol"],
            "impliedVol": row["impliedVol"],
            "weeklyRange": row["weeklyRange"],
            "weeklyHigh": row["weeklyHigh"],
            "weeklyLow": row["weeklyLow"],
            "weeklyAchievement": row["weeklyAchievement"],
            "weeklyTarget": row["weeklyTarget"],
        })

    return overview_rows


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
    all_overview_rows = []

    now_ct = datetime.now(ZoneInfo("America/Chicago")).isoformat()

    for contract in CONTRACTS:
        try:
            rows = fetch_yahoo_history(contract["symbol"])
            clean = contract["base_symbol"]
            tick = TICK_SIZES[contract["commodity"]]

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

            overview_rows = build_overview_rows_from_history(history_payload)
            all_overview_rows.extend(overview_rows)

            latest_row = history_rows[0] if history_rows else {}

            daily_feed.append({
                "symbol": clean,
                "dailyHigh": latest_row.get("dailyHigh", ""),
                "dailyLow": latest_row.get("dailyLow", ""),
                "asOf": now_ct,
                "isStale": False,
            })

            weekly_feed.append({
                "symbol": clean,
                "weeklyHigh": latest_row.get("weeklyHigh", ""),
                "weeklyLow": latest_row.get("weeklyLow", ""),
                "asOf": now_ct,
                "isStale": False,
            })

            previous_daily_ranges = []
            for x in history_rows[1:4]:
                if x.get("dailyRange"):
                    previous_daily_ranges.append(x["dailyRange"])

            previous_weekly_ranges = []
            for start in (5, 10, 15):
                block = compute_weekly_block(rows, start, tick)
                if block is not None:
                    previous_weekly_ranges.append(format_tick(block["range"], tick))

            previous_ranges_feed.append({
                "symbol": clean,
                "previousDailyRanges": previous_daily_ranges,
                "previousWeeklyRanges": previous_weekly_ranges,
            })

            history_index.append(clean)
            print("OK", contract["symbol"])

        except Exception as exc:
            errors.append({"symbol": contract["symbol"], "error": str(exc)})
            print("ERR", contract["symbol"], exc)

        time.sleep(0.5)

    history_index.sort(key=lambda s: HOME_ORDER.index(s) if s in HOME_ORDER else 9999)

    overview_by_date = {}
    for row in all_overview_rows:
        overview_by_date.setdefault(row["date"], []).append(row)

    for date_key in overview_by_date:
        overview_by_date[date_key].sort(
            key=lambda r: HOME_ORDER.index(r["symbol"]) if r["symbol"] in HOME_ORDER else 9999
        )

    (out / "daily-feed-full.json").write_text(json.dumps(daily_feed, indent=2), encoding="utf-8")
    (out / "weekly-feed-full.json").write_text(json.dumps(weekly_feed, indent=2), encoding="utf-8")
    (out / "previous-ranges-feed-full.json").write_text(json.dumps(previous_ranges_feed, indent=2), encoding="utf-8")
    (out / "errors.json").write_text(json.dumps(errors, indent=2), encoding="utf-8")
    (out / "overview-by-date.json").write_text(json.dumps(overview_by_date, indent=2), encoding="utf-8")

    meta = {
        "builtAt": now_ct,
        "status": "ok" if not errors else "partial",
        "successCount": len(CONTRACTS) - len(errors),
        "errorCount": len(errors),
        "version": "v2.3-overview-feed",
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    (history_dir / "index.json").write_text(
        json.dumps({"contracts": history_index}, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
