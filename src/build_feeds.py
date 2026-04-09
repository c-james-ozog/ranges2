from __future__ import annotations

import json
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from src.config import CONTRACTS, SETTINGS
from src.scraper import build_history_rows, fetch_yahoo_history, latest_summary


def read_json(path: Path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_contract_lookup():
    return {c["base_symbol"]: c for c in CONTRACTS}


def apply_overrides(base_symbol: str, payload: dict, overrides: dict):
    contract_overrides = deepcopy(overrides.get(base_symbol, {}))
    if not contract_overrides:
        payload["overrideApplied"] = False
        return payload
    for key, value in contract_overrides.items():
        if key in {"note", "expiresAt"}:
            continue
        payload[key] = value
    payload["overrideApplied"] = True
    if "note" in contract_overrides:
        payload["overrideNote"] = contract_overrides["note"]
    if "expiresAt" in contract_overrides:
        payload["overrideExpiresAt"] = contract_overrides["expiresAt"]
    return payload


def main():
    out = Path("feeds")
    out.mkdir(exist_ok=True)
    SETTINGS["cache_dir"].mkdir(parents=True, exist_ok=True)
    SETTINGS["history_dir"].mkdir(parents=True, exist_ok=True)

    built_at = now_iso()
    overrides = read_json(SETTINGS["overrides_file"], {})
    contract_lookup = build_contract_lookup()

    daily_feed = []
    weekly_feed = []
    previous_ranges_feed = []
    overview_feed = []
    errors = []
    history_index = []

    for contract in CONTRACTS:
        base_symbol = contract["base_symbol"]
        cache_path = SETTINGS["cache_dir"] / f"{base_symbol}.json"
        history_path = SETTINGS["history_dir"] / f"{base_symbol}.json"

        try:
            rows = fetch_yahoo_history(contract["symbol"])
            history_rows = build_history_rows(rows, contract["commodity"])
            summary = latest_summary(history_rows)
            stale = False
            source = "yahoo"
            cache_payload = {
                "contract": contract,
                "summary": summary,
                "history": history_rows,
                "asOf": built_at,
                "isStale": False,
                "source": source,
            }
            write_json(cache_path, cache_payload)
            print("OK", contract["symbol"])
        except Exception as exc:
            cached = read_json(cache_path, None)
            if not cached:
                errors.append({"symbol": contract["symbol"], "error": str(exc), "usedCache": False})
                print("ERR", contract["symbol"], exc)
                time.sleep(0.5)
                continue
            contract = cached["contract"]
            summary = cached["summary"]
            history_rows = cached["history"]
            stale = True
            source = "cache"
            errors.append({"symbol": contract["symbol"], "error": str(exc), "usedCache": True})
            print("CACHE", contract["symbol"], exc)

        daily_payload = {
            "symbol": base_symbol,
            "commodity": contract["commodity"],
            "month": contract["month"],
            "dailyHigh": summary["dailyHigh"],
            "dailyLow": summary["dailyLow"],
            "dailyRange": summary["dailyRange"],
            "dailyTarget": summary["dailyTarget"],
            "dailyAchievement": summary["dailyAchievement"],
            "asOf": built_at,
            "isStale": stale,
            "source": source,
        }
        weekly_payload = {
            "symbol": base_symbol,
            "commodity": contract["commodity"],
            "month": contract["month"],
            "weeklyHigh": summary["weeklyHigh"],
            "weeklyLow": summary["weeklyLow"],
            "weeklyRange": summary["weeklyRange"],
            "weeklyTarget": summary["weeklyTarget"],
            "weeklyAchievement": summary["weeklyAchievement"],
            "asOf": built_at,
            "isStale": stale,
            "source": source,
        }
        previous_payload = {
            "symbol": base_symbol,
            "commodity": contract["commodity"],
            "month": contract["month"],
            "previousDailyRanges": summary["previousDailyRanges"],
            "previousWeeklyRanges": summary["previousWeeklyRanges"],
            "historicVol": summary["historicVol"],
            "impliedVol": summary["impliedVol"],
            "impliedTarget": summary["impliedTarget"],
            "asOf": built_at,
            "isStale": stale,
            "source": source,
        }
        overview_payload = {
            "symbol": base_symbol,
            "commodity": contract["commodity"],
            "month": contract["month"],
            "date": summary["date"],
            "daily": {
                "high": summary["dailyHigh"],
                "low": summary["dailyLow"],
                "range": summary["dailyRange"],
                "target": summary["dailyTarget"],
                "achievement": summary["dailyAchievement"],
            },
            "weekly": {
                "high": summary["weeklyHigh"],
                "low": summary["weeklyLow"],
                "range": summary["weeklyRange"],
                "target": summary["weeklyTarget"],
                "achievement": summary["weeklyAchievement"],
            },
            "historicVol": summary["historicVol"],
            "impliedVol": summary["impliedVol"],
            "impliedTarget": summary["impliedTarget"],
            "asOf": built_at,
            "isStale": stale,
            "source": source,
        }

        daily_payload = apply_overrides(base_symbol, daily_payload, overrides)
        weekly_payload = apply_overrides(base_symbol, weekly_payload, overrides)
        previous_payload = apply_overrides(base_symbol, previous_payload, overrides)
        overview_payload = apply_overrides(base_symbol, overview_payload, overrides)

        daily_feed.append(daily_payload)
        weekly_feed.append(weekly_payload)
        previous_ranges_feed.append(previous_payload)
        overview_feed.append(overview_payload)

        history_doc = {
            "symbol": base_symbol,
            "commodity": contract["commodity"],
            "month": contract["month"],
            "asOf": built_at,
            "isStale": stale,
            "source": source,
            "rows": history_rows,
        }
        history_doc = apply_overrides(base_symbol, history_doc, overrides)
        write_json(history_path, history_doc)
        history_index.append(
            {
                "symbol": base_symbol,
                "commodity": contract["commodity"],
                "month": contract["month"],
                "historyUrl": f"history/{base_symbol}.json",
            }
        )
        time.sleep(0.5)

    meta = {
        "builtAt": built_at,
        "contractCount": len(CONTRACTS),
        "successCount": len(overview_feed),
        "errorCount": len(errors),
        "status": "ok" if not errors else ("partial" if overview_feed else "error"),
        "historyRange": SETTINGS["history_range"],
        "historicLookbackDays": SETTINGS["historic_lookback_days"],
        "hasOverrides": bool(overrides),
    }

    write_json(out / "daily-feed-full.json", daily_feed)
    write_json(out / "weekly-feed-full.json", weekly_feed)
    write_json(out / "previous-ranges-feed-full.json", previous_ranges_feed)
    write_json(out / "overview-feed-full.json", overview_feed)
    write_json(out / "history-index.json", history_index)
    write_json(out / "errors.json", errors)
    write_json(out / "meta.json", meta)


if __name__ == "__main__":
    main()
