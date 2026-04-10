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


def compute_daily_target(rows: list, i: int, tick: float):
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
    prior_ranges = []
    for start in (i + 5, i + 10, i + 15):
        block = compute_weekly_block(rows, start, tick)
        if block is None:
            return None
        prior_ranges.append(block["range"])

    target_raw = (sum(prior_ranges) / 3) * 0.8
    return round_to_tick(target_raw, tick)


def build_history(rows: list, contract: dict) -> list:
    tick = TICK_SIZES[contract["commodity"]]
    history_rows = []

    for i, r in enumerate(rows):
        daily_high = round_to_tick(r["high"], tick)
        daily_low = round_to_tick(r["low"], tick)
        daily_range = round_to_tick(daily_high - daily_low, tick)

        daily_target_value = compute_daily_target(rows, i, tick)
        daily_target = format_tick(daily_target_value, tick) if daily_target_value is not None else ""
        daily_achievement = pct_str(daily_range, daily_target_value) if daily_target_value else ""

        hist_vol = historic_vol_str(daily_range, rows[i + 1:i + 21], tick)

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


def compute_next_daily_target_from_history_rows(history_rows: list, idx: int, tick: float) -> str:
    if idx + 2 >= len(history_rows):
        return ""

    r0 = history_rows[idx]
    r1 = history_rows[idx + 1]
    r2 = history_rows[idx + 2]

    d0 = round_to_tick(float(r0["dailyRange"]), tick)
    d1 = round_to_tick(float(r1["dailyRange"]), tick)
    d2 = round_to_tick(float(r2["dailyRange"]), tick)

    avg = (d0 + d1 + d2) / 3
    next_target = round_to_tick(avg * 0.8, tick)
    return format_tick(next_target, tick)


def build_overview_rows_from_history(history_payload: dict) -> list:
    symbol = history_payload["symbol"]
    commodity = history_payload["commodity"]
    month = history_payload["month"]
    rows = history_payload["rows"]
    tick = TICK_SIZES[commodity]

    overview_rows = []

    for idx, row in enumerate(rows):
        next_target = compute_next_daily_target_from_history_rows(rows, idx, tick)
        next_target_subtext = ""
        if idx + 2 < len(rows):
            next_target_subtext = ", ".join([
                rows[idx]["dailyRange"],
                rows[idx + 1]["dailyRange"],
                rows[idx + 2]["dailyRange"],
            ])

        overview_rows.append({
            "date": row["date"],
            "symbol": symbol,
            "commodity": commodity,
            "month": month,
            "dailyTarget": row["dailyTarget"],
            "dailyRange": row["dailyRange"],
            "dailyHigh": row["dailyHigh"],
            "dailyLow": row["dailyLow"],
            "dailyAchievement": row["dailyAchievement"],
            "nextDailyTarget": next_target,
            "nextDailyTargetSubtext": next_target_subtext,
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
            previous_weekly_ranges = []

            for x in history_rows[1:4]:
                if x.get("dailyRange"):
                    previous_daily_ranges.append(x["dailyRange"])

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
        "version": "v2.2-overview-feed",
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    (history_dir / "index.json").write_text(
        json.dumps({"contracts": history_index}, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
