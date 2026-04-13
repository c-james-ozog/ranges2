from pathlib import Path
import json
import time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

from src.config import CONTRACTS, TICK_SIZES, PRICE_DIVISORS
from src.scraper import fetch_yahoo_history, round_to_tick, format_tick


HOME_ORDER = [
    "CCK26", "KCK26", "HGK26", "ZCN26", "ZCZ26", "CTK26", "CLM26", "GFK26",
    "GCJ26", "KEN26", "HEM26", "LEJ26", "NQM26", "NGM26", "ZRN26", "ESM26",
    "SIM26", "ZMN26", "ZLN26", "ZSN26", "ZSX26", "DXM26", "ZWN26",
]

CME_HOLIDAYS_2026 = {
    "2026-01-01",
    "2026-01-19",
    "2026-02-16",
    "2026-04-03",
    "2026-05-25",
    "2026-06-19",
    "2026-07-03",
    "2026-09-07",
    "2026-11-26",
    "2026-12-25",
}


def chicago_date_from_ts(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=ZoneInfo("America/Chicago"))
    return (dt + timedelta(days=1)).strftime("%Y-%m-%d")


def pct_str(numerator: float, denominator: float) -> str:
    if not denominator:
        return ""
    return f"{round((numerator / denominator) * 100, 1)}%"


def compute_daily_range(row: dict, tick: float) -> float:
    high = round_to_tick(row["high"], tick)
    low = round_to_tick(row["low"], tick)
    return round_to_tick(high - low, tick)


def compute_daily_full_achievement(rows: list, i: int, tick: float):
    window = rows[i:i + 3]
    if len(window) < 3:
        return None
    ranges = [compute_daily_range(x, tick) for x in window]
    avg = sum(ranges) / 3
    return round_to_tick(avg, tick)


def compute_next_daily_target(rows: list, i: int, tick: float):
    full_achievement = compute_daily_full_achievement(rows, i, tick)
    if full_achievement is None:
        return None
    return round_to_tick(full_achievement * 0.8, tick)


def compute_historic_vol(rows: list, i: int, tick: float, price_divisor: float = 100.0) -> str:
    window = rows[i:i + 3]
    if len(window) < 3:
        return ""
    close_price = window[0].get("close")
    if not close_price:
        return ""
    close_one_pct = close_price / price_divisor
    ranges = [compute_daily_range(x, tick) for x in window]
    avg_range = sum(ranges) / 3
    target_range = avg_range * 0.80
    hv = (target_range / close_one_pct) * 16
    return f"{round(hv, 1)}%"


def get_week_monday(date_str: str) -> str:
    d = date.fromisoformat(date_str)
    monday = d - timedelta(days=d.weekday())
    return monday.isoformat()


def is_last_trading_day_of_week(date_str: str) -> bool:
    d = date.fromisoformat(date_str)
    friday = d + timedelta(days=(4 - d.weekday()))
    candidate = friday
    for _ in range(5):
        iso = candidate.isoformat()
        if candidate.weekday() < 5 and iso not in CME_HOLIDAYS_2026:
            return date_str == iso
        candidate -= timedelta(days=1)
    return False


def compute_weekly_ranges(dated_rows: list, tick: float) -> dict:
    """Cumulative weekly high/low/range for every date."""
    weeks: dict = {}
    for r in dated_rows:
        monday = get_week_monday(r["date"])
        weeks.setdefault(monday, []).append(r)

    result = {}
    for monday, week_rows in weeks.items():
        week_rows_sorted = sorted(week_rows, key=lambda x: x["date"])
        cum_high = None
        cum_low = None
        for r in week_rows_sorted:
            daily_high = round_to_tick(r["high"], tick)
            daily_low = round_to_tick(r["low"], tick)
            cum_high = daily_high if cum_high is None else max(cum_high, daily_high)
            cum_low = daily_low if cum_low is None else min(cum_low, daily_low)
            weekly_range = round_to_tick(cum_high - cum_low, tick)
            result[r["date"]] = {
                "weeklyHigh": cum_high,
                "weeklyLow": cum_low,
                "weeklyRange": weekly_range,
            }
    return result


def compute_completed_weekly_ranges(dated_rows: list, tick: float) -> list:
    """Returns list of completed weekly ranges ordered newest first."""
    weeks: dict = {}
    for r in dated_rows:
        monday = get_week_monday(r["date"])
        weeks.setdefault(monday, []).append(r)

    completed = []
    for monday, week_rows in sorted(weeks.items(), reverse=True):
        week_rows_sorted = sorted(week_rows, key=lambda x: x["date"])
        last_date = week_rows_sorted[-1]["date"]
        if is_last_trading_day_of_week(last_date):
            high = round_to_tick(max(r["high"] for r in week_rows_sorted), tick)
            low  = round_to_tick(min(r["low"]  for r in week_rows_sorted), tick)
            rng  = round_to_tick(high - low, tick)
            completed.append({
                "monday": monday,
                "lastDay": last_date,
                "high": high,
                "low": low,
                "range": rng,
            })
    return completed


def compute_weekly_targets(dated_rows: list, tick: float) -> dict:
    """
    For every date, compute:
      weeklyTarget    = avg of 3 prior completed weeks' ranges (current week's target)
      nextWeeklyTarget = avg of current + 2 prior completed weeks' ranges (next week's target)
                         only populated on the last trading day of the week

    Returns dict keyed by date string -> {"weeklyTarget": ..., "nextWeeklyTarget": ...}
    """
    completed = compute_completed_weekly_ranges(dated_rows, tick)

    # For each completed week i:
    #   nextWeeklyTarget = avg of completed[i], [i+1], [i+2]  (used as target for week i+1... wait)
    #   weeklyTarget for week N = avg of completed weeks N-1, N-2, N-3
    #   nextWeeklyTarget for week N's last day = avg of completed weeks N, N-1, N-2

    # Map monday -> weeklyTarget (avg of prior 3 completed weeks)
    weekly_target_by_monday = {}
    for i, week in enumerate(completed):
        if i + 2 < len(completed):
            # completed is newest-first, so i+1 and i+2 are older weeks
            # weeklyTarget for the week AFTER completed[i] = avg of completed[i], [i+1], [i+2]
            ranges = [completed[i]["range"], completed[i+1]["range"], completed[i+2]["range"]]
            avg = sum(ranges) / 3
            target = round_to_tick(avg, tick)
            # This target applies to the week whose monday comes AFTER completed[i]["monday"]
            next_monday_d = date.fromisoformat(completed[i]["monday"]) + timedelta(weeks=1)
            next_monday = next_monday_d.isoformat()
            weekly_target_by_monday[next_monday] = target

    # nextWeeklyTarget for last trading day of week N
    # = avg of completed[N], [N-1], [N-2] = same as weeklyTarget for week N+1
    # So nextWeeklyTarget on week N's last day = weeklyTarget_by_monday of week N+1
    next_weekly_target_by_last_day = {}
    for week in completed:
        next_monday_d = date.fromisoformat(week["monday"]) + timedelta(weeks=1)
        next_monday = next_monday_d.isoformat()
        if next_monday in weekly_target_by_monday:
            next_weekly_target_by_last_day[week["lastDay"]] = weekly_target_by_monday[next_monday]

    # Map every date to its week's target
    result = {}
    for r in dated_rows:
        monday = get_week_monday(r["date"])
        wt = weekly_target_by_monday.get(monday)
        nwt = next_weekly_target_by_last_day.get(r["date"])  # only on last trading day
        result[r["date"]] = {
            "weeklyTarget": wt,
            "nextWeeklyTarget": nwt,
        }
    return result


def has_market_closed_gap(prev_date_str: str, curr_date_str: str) -> bool:
    try:
        prev_d = date.fromisoformat(prev_date_str)
        curr_d = date.fromisoformat(curr_date_str)
    except ValueError:
        return False
    d = curr_d + timedelta(days=1)
    while d < prev_d:
        iso = d.isoformat()
        if d.weekday() >= 5 or iso in CME_HOLIDAYS_2026:
            return True
        d += timedelta(days=1)
    return False


def build_history(rows: list, contract: dict) -> list:
    tick = TICK_SIZES[contract["commodity"]]
    price_divisor = PRICE_DIVISORS.get(contract["commodity"], 100)

    # Pre-build dated rows for weekly functions
    dated_rows = []
    for r in rows:
        dated_rows.append({
            "date": chicago_date_from_ts(r["timestamp"]),
            "high": r["high"],
            "low": r["low"],
            "close": r.get("close"),
        })

    weekly_data    = compute_weekly_ranges(dated_rows, tick)
    weekly_targets = compute_weekly_targets(dated_rows, tick)

    history_rows = []

    for i, r in enumerate(rows):
        date_str   = dated_rows[i]["date"]
        daily_high = round_to_tick(r["high"], tick)
        daily_low  = round_to_tick(r["low"], tick)
        daily_range = round_to_tick(daily_high - daily_low, tick)

        full_achievement_value  = compute_daily_full_achievement(rows, i, tick)
        next_daily_target_value = compute_next_daily_target(rows, i, tick)
        historic_vol            = compute_historic_vol(rows, i, tick, price_divisor)

        wd = weekly_data.get(date_str, {})
        weekly_high  = wd.get("weeklyHigh")
        weekly_low   = wd.get("weeklyLow")
        weekly_range = wd.get("weeklyRange")

        wt = weekly_targets.get(date_str, {})
        weekly_target_value      = wt.get("weeklyTarget")
        next_weekly_target_value = wt.get("nextWeeklyTarget")

        history_rows.append({
            "date": date_str,
            "dailyHigh": format_tick(daily_high, tick),
            "dailyLow": format_tick(daily_low, tick),
            "dailyRange": format_tick(daily_range, tick),
            "fullAchievement": format_tick(full_achievement_value, tick) if full_achievement_value is not None else "",
            "fullAchievementValue": full_achievement_value,
            "nextDailyTarget": format_tick(next_daily_target_value, tick) if next_daily_target_value is not None else "",
            "nextDailyTargetValue": next_daily_target_value,
            "historicVol": historic_vol,
            "impliedVol": "",
            "weeklyHigh": format_tick(weekly_high, tick) if weekly_high is not None else "",
            "weeklyLow": format_tick(weekly_low, tick) if weekly_low is not None else "",
            "weeklyRange": format_tick(weekly_range, tick) if weekly_range is not None else "",
            "weeklyTargetValue": weekly_target_value,
            "nextWeeklyTargetValue": next_weekly_target_value,
            "sectionBreak": False,
        })

    # Second pass: daily target from prior row's nextDailyTarget
    for i, row in enumerate(history_rows):
        prev_row = history_rows[i + 1] if i + 1 < len(history_rows) else None

        daily_target_value  = prev_row["nextDailyTargetValue"] if prev_row else None
        daily_range_num     = float(row["dailyRange"]) if row["dailyRange"] else None
        weekly_range_num    = float(row["weeklyRange"]) if row["weeklyRange"] else None
        weekly_target_value = row["weeklyTargetValue"]
        next_weekly_target_value = row["nextWeeklyTargetValue"]

        row["dailyTarget"]    = format_tick(daily_target_value, tick) if daily_target_value is not None else ""
        row["dailyAchievement"] = pct_str(daily_range_num, daily_target_value) if daily_range_num is not None and daily_target_value else ""
        row["weeklyTarget"]   = format_tick(weekly_target_value, tick) if weekly_target_value is not None else ""
        row["weeklyAchievement"] = pct_str(weekly_range_num, weekly_target_value) if weekly_range_num is not None and weekly_target_value else ""
        row["nextWeeklyTarget"] = format_tick(next_weekly_target_value, tick) if next_weekly_target_value is not None else ""

        if i > 0:
            newer = history_rows[i - 1]["date"]
            older = row["date"]
            row["sectionBreak"] = has_market_closed_gap(newer, older)

        del row["fullAchievementValue"]
        del row["nextDailyTargetValue"]
        del row["weeklyTargetValue"]
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
            "nextWeeklyTarget": row["nextWeeklyTarget"],
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

            dated_rows = [{"date": r["date"], "high": rows[j]["high"], "low": rows[j]["low"]}
                         for j, r in enumerate(history_rows)]
            completed = compute_completed_weekly_ranges(dated_rows, tick)
            previous_weekly_ranges = []
            for w in completed[1:4]:
                previous_weekly_ranges.append(format_tick(w["range"], tick))

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
        "version": "v3.1-next-weekly-target",
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    (history_dir / "index.json").write_text(
        json.dumps({"contracts": history_index}, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
