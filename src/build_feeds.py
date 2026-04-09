from pathlib import Path
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from src.config import CONTRACTS
from src.scraper import fetch_yahoo_history, parse_rows


def build_history(rows, contract):
    history_rows = []

    for r in rows:
        date = datetime.fromtimestamp(r["timestamp"], tz=ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")

        daily_range = r["high"] - r["low"]

        # TEMP placeholder logic (you will refine later)
        daily_target = daily_range
        daily_achievement = (daily_range / daily_target * 100) if daily_target else 0

        history_rows.append({
            "date": date,
            "dailyTarget": f"{round(daily_target, 2)}",
            "dailyRange": f"{round(daily_range, 2)}",
            "dailyHigh": f"{round(r['high'], 2)}",
            "dailyLow": f"{round(r['low'], 2)}",
            "dailyAchievement": f"{round(daily_achievement, 1)}%",
            "historicVol": "",
            "impliedVol": "",
            "weeklyRange": "",
            "weeklyHigh": "",
            "weeklyLow": "",
            "weeklyAchievement": "",
            "weeklyTarget": ""
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

    # ✅ Central Time timestamp
    now_ct = datetime.now(ZoneInfo("America/Chicago")).isoformat()

    for contract in CONTRACTS:
        try:
            rows = fetch_yahoo_history(contract["symbol"])
            parsed = parse_rows(rows, contract["commodity"])

            clean = contract["base_symbol"]

            # ---------- DAILY ----------
            daily_feed.append({
                "symbol": clean,
                "dailyHigh": parsed["dailyHigh"],
                "dailyLow": parsed["dailyLow"],
                "asOf": now_ct,
                "isStale": False
            })

            # ---------- WEEKLY ----------
            weekly_feed.append({
                "symbol": clean,
                "weeklyHigh": parsed["weeklyHigh"],
                "weeklyLow": parsed["weeklyLow"],
                "asOf": now_ct,
                "isStale": False
            })

            # ---------- PREVIOUS ----------
            previous_ranges_feed.append({
                "symbol": clean,
                "previousDailyRanges": parsed["previousDailyRanges"],
                "previousWeeklyRanges": parsed["previousWeeklyRanges"]
            })

            # ---------- HISTORY ----------
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

    # ---------- WRITE MAIN FEEDS ----------
    (out / "daily-feed-full.json").write_text(json.dumps(daily_feed, indent=2))
    (out / "weekly-feed-full.json").write_text(json.dumps(weekly_feed, indent=2))
    (out / "previous-ranges-feed-full.json").write_text(json.dumps(previous_ranges_feed, indent=2))
    (out / "errors.json").write_text(json.dumps(errors, indent=2))

    # ---------- META ----------
    meta = {
        "builtAt": now_ct,
        "status": "ok" if not errors else "partial",
        "successCount": len(CONTRACTS) - len(errors),
        "errorCount": len(errors),
        "version": "v2.1"
    }

    (out / "meta.json").write_text(json.dumps(meta, indent=2))

    # ---------- HISTORY INDEX ----------
    (history_dir / "index.json").write_text(json.dumps({
        "contracts": history_index
    }, indent=2))


if __name__ == "__main__":
    main()
