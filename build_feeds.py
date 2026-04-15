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
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
}

# Load implied vol overrides
IMPLIED_VOL_FILE = Path("implied_vol.json")
IMPLIED_VOL_DATA: dict = {}
if IMPLIED_VOL_FILE.exists():
    try:
        IMPLIED_VOL_DATA = json.loads(IMPLIED_VOL_FILE.read_text(encoding="utf-8"))
        print(f"Loaded implied vol data for {len(IMPLIED_VOL_DATA)} dates")
    except Exception as e:
        print(f"Warning: could not load implied_vol.json: {e}")

# Load price overrides (for correcting bad Yahoo data)
# Format: { "YYYY-MM-DD": { "SYMBOL": { "high": float, "low": float }, ... }, ... }
PRICE_OVERRIDE_FILE = Path("price_overrides.json")
PRICE_OVERRIDE_DATA: dict = {}
if PRICE_OVERRIDE_FILE.exists():
    try:
        PRICE_OVERRIDE_DATA = json.loads(PRICE_OVERRIDE_FILE.read_text(encoding="utf-8"))
        print(f"Loaded price overrides for {len(PRICE_OVERRIDE_DATA)} dates")
    except Exception as e:
        print(f"Warning: could not load price_overrides.json: {e}")

# Load Rice manual override from Excel
RICE_OVERRIDE_DATA: dict = {}
EXCEL_FILE = Path("implied_vol_input.xlsx")
if EXCEL_FILE.exists():
    try:
        from openpyxl import load_workbook
        wb = load_workbook(EXCEL_FILE, data_only=True)
        if 'Rice Override (ZRN26)' in wb.sheetnames:
            ws = wb['Rice Override (ZRN26)']
            for row in ws.iter_rows(min_row=3, values_only=True):
                date_val, high, low, close, *_ = list(row) + [None, None, None, None]
                if date_val is None or high is None or low is None:
                    continue
                if hasattr(date_val, 'strftime'):
                    date_str = date_val.strftime('%Y-%m-%d')
                else:
                    try:
                        from datetime import datetime as dt
                        d = dt.strptime(str(date_val), '%m/%d/%y')
                        date_str = d.strftime('%Y-%m-%d')
                    except Exception:
                        continue
                RICE_OVERRIDE_DATA[date_str] = {
                    'high': float(high),
                    'low': float(low),
                    'close': float(close) if close else None,
                }
        print(f"Loaded Rice override data for {len(RICE_OVERRIDE_DATA)} dates")
    except Exception as e:
        print(f"Warning: could not load Rice override from Excel: {e}")


def get_implied_vol(date_str: str, symbol: str) -> str:
    day_data = IMPLIED_VOL_DATA.get(date_str, {})
    val = day_data.get(symbol)
    if val is None:
        return ""
    return f"{round(float(val), 1)}%"


def chicago_date_from_ts(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=ZoneInfo("America/Chicago"))
    return dt.strftime("%Y-%m-%d")


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


def compute_implied_vol_trends(history_rows: list) -> list:
    reversed_rows = list(reversed(history_rows))
    trends = [""] * len(reversed_rows)
    for i, row in enumerate(reversed_rows):
        iv_str = row.get("impliedVol", "")
        if not iv_str:
            trends[i] = ""
            continue
        try:
            iv = float(iv_str.replace("%", ""))
        except ValueError:
            trends[i] = ""
            continue
        if i == 0:
            trends[i] = ""
            continue
        prev_iv = None
        for j in range(i - 1, -1, -1):
            prev_str = reversed_rows[j].get("impliedVol", "")
            if prev_str:
                try:
                    prev_iv = float(prev_str.replace("%", ""))
                    break
                except ValueError:
                    continue
        if prev_iv is None:
            trends[i] = ""
            continue
        if iv > prev_iv:
            direction = "up"
        elif iv < prev_iv:
            direction = "down"
        else:
            trends[i] = ""
            continue
        count = 1
        for j in range(i - 1, -1, -1):
            prev_trend = trends[j]
            if not prev_trend:
                break
            prev_dir = prev_trend.split("|")[0]
            if prev_dir == direction:
                count += 1
            else:
                break
        trends[i] = direction + "|" + str(count)
    return list(reversed(trends))


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


def is_first_trading_day_of_week(date_str: str) -> bool:
    d = date.fromisoformat(date_str)
    monday = d - timedelta(days=d.weekday())
    candidate = monday
    for _ in range(5):
        iso = candidate.isoformat()
        if candidate.weekday() < 5 and iso not in CME_HOLIDAYS_2026:
            return date_str == iso
        candidate += timedelta(days=1)
    return False


def compute_weekly_ranges(dated_rows: list, tick: float) -> dict:
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
    completed = compute_completed_weekly_ranges(dated_rows, tick)
    weekly_target_by_monday = {}
    for i, week in enumerate(completed):
        if i + 2 < len(completed):
            ranges = [completed[i]["range"], completed[i+1]["range"], completed[i+2]["range"]]
            avg = sum(ranges) / 3
            target = round_to_tick(avg, tick)
            next_monday_d = date.fromisoformat(completed[i]["monday"]) + timedelta(weeks=1)
            next_monday = next_monday_d.isoformat()
            weekly_target_by_monday[next_monday] = target
    next_weekly_target_by_last_day = {}
    for week in completed:
        next_monday_d = date.fromisoformat(week["monday"]) + timedelta(weeks=1)
        next_monday = next_monday_d.isoformat()
        if next_monday in weekly_target_by_monday:
            next_weekly_target_by_last_day[week["lastDay"]] = weekly_target_by_monday[next_monday]
    result = {}
    for r in dated_rows:
        monday = get_week_monday(r["date"])
        wt = weekly_target_by_monday.get(monday)
        nwt = next_weekly_target_by_last_day.get(r["date"])
        result[r["date"]] = {
            "weeklyTarget": wt,
            "nextWeeklyTarget": nwt,
        }
    return result


def apply_price_overrides(rows: list, symbol: str) -> list:
    """Apply manual price overrides for any contract/date in price_overrides.json."""
    if not PRICE_OVERRIDE_DATA:
        return rows
    updated = []
    for r in rows:
        date_str = chicago_date_from_ts(r["timestamp"])
        day_overrides = PRICE_OVERRIDE_DATA.get(date_str, {})
        sym_override = day_overrides.get(symbol)
        if sym_override:
            r = dict(r)
            if "high" in sym_override:
                r["high"] = float(sym_override["high"])
            if "low" in sym_override:
                r["low"] = float(sym_override["low"])
            if "close" in sym_override and sym_override["close"] is not None:
                r["close"] = float(sym_override["close"])
        updated.append(r)
    return updated


def apply_rice_overrides(rows: list, symbol: str) -> list:
    """For ZRN26, replace Yahoo high/low/close with manual override data where available."""
    if symbol != "ZRN26" or not RICE_OVERRIDE_DATA:
        return rows
    updated = []
    for r in rows:
        date_str = chicago_date_from_ts(r["timestamp"])
        if date_str in RICE_OVERRIDE_DATA:
            override = RICE_OVERRIDE_DATA[date_str]
            r = dict(r)
            r["high"] = override["high"]
            r["low"] = override["low"]
            if override.get("close"):
                r["close"] = override["close"]
        updated.append(r)
    return updated


def build_history(rows: list, contract: dict) -> list:
    tick = TICK_SIZES[contract["commodity"]]
    price_divisor = PRICE_DIVISORS.get(contract["commodity"], 100)
    symbol = contract["base_symbol"]

    # Apply overrides before any calculations
    rows = apply_price_overrides(rows, symbol)
    rows = apply_rice_overrides(rows, symbol)

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
        date_str    = dated_rows[i]["date"]
        daily_high  = round_to_tick(r["high"], tick)
        daily_low   = round_to_tick(r["low"], tick)
        daily_range = round_to_tick(daily_high - daily_low, tick)

        full_achievement_value  = compute_daily_full_achievement(rows, i, tick)
        next_daily_target_value = compute_next_daily_target(rows, i, tick)
        historic_vol            = compute_historic_vol(rows, i, tick, price_divisor)
        implied_vol             = get_implied_vol(date_str, symbol)

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
            "impliedVol": implied_vol,
            "weeklyHigh": format_tick(weekly_high, tick) if weekly_high is not None else "",
            "weeklyLow": format_tick(weekly_low, tick) if weekly_low is not None else "",
            "weeklyRange": format_tick(weekly_range, tick) if weekly_range is not None else "",
            "weeklyTargetValue": weekly_target_value,
            "nextWeeklyTargetValue": next_weekly_target_value,
            "sectionBreak": False,
        })

    # Second pass
    for i, row in enumerate(history_rows):
        prev_row = history_rows[i + 1] if i + 1 < len(history_rows) else None

        daily_target_value       = prev_row["nextDailyTargetValue"] if prev_row else None
        daily_range_num          = float(row["dailyRange"]) if row["dailyRange"] else None
        weekly_range_num         = float(row["weeklyRange"]) if row["weeklyRange"] else None
        weekly_target_value      = row["weeklyTargetValue"]
        next_weekly_target_value = row["nextWeeklyTargetValue"]

        row["dailyTarget"]       = format_tick(daily_target_value, tick) if daily_target_value is not None else ""
        row["dailyAchievement"]  = pct_str(daily_range_num, daily_target_value) if daily_range_num is not None and daily_target_value else ""
        row["weeklyTarget"]      = format_tick(weekly_target_value, tick) if weekly_target_value is not None else ""
        row["weeklyAchievement"] = pct_str(weekly_range_num, weekly_target_value) if weekly_range_num is not None and weekly_target_value else ""
        row["nextWeeklyTarget"]  = format_tick(next_weekly_target_value, tick) if next_weekly_target_value is not None else ""

        row["sectionBreak"] = is_first_trading_day_of_week(row["date"])

        del row["fullAchievementValue"]
        del row["nextDailyTargetValue"]
        del row["weeklyTargetValue"]
        del row["nextWeeklyTargetValue"]

    # Third pass: implied vol trend
    trends = compute_implied_vol_trends(history_rows)
    for i, row in enumerate(history_rows):
        row["impliedVolTrend"] = trends[i]

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
            "impliedVolTrend": row["impliedVolTrend"],
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

            dated_rows_for_prev = [{"date": r["date"], "high": rows[j]["high"], "low": rows[j]["low"]}
                                   for j, r in enumerate(history_rows)]
            completed = compute_completed_weekly_ranges(dated_rows_for_prev, tick)
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
        "version": "v3.5-price-overrides",
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    (history_dir / "index.json").write_text(
        json.dumps({"contracts": history_index}, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
