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

        prev3 = rows[i + 1:i + 4]
        if len(prev3) == 3:
            daily_target_raw = sum(x["high"] - x["low"] for x in prev3) / 3
        else:
            daily_target_raw = current_range_raw

        daily_target = format_tick(round_to_tick(daily_target_raw, tick), tick)
        daily_achievement = pct_str(current_range_raw, daily_target_raw)
        hist_vol = historic_vol_str(current_range_raw, rows[i + 1:i + 21])

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


def main():
    out = Path("feeds")
    history_dir = out / "history"

    out.mkdir(exist_ok=True)
    history_dir.mkdir(exist_ok=True)

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

    history_index.sort(key=lambda s: HOME_ORDER.index(s) if s in HOME_ORDER else 9999)

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
