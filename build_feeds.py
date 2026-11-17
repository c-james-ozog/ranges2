"""
ranges2 feed builder — SELF-CONTAINED (inline config + inline scraper + build logic).
Rebuilt after the options refactor gutted the builder; no src/ imports so the
options config.py cannot break it. Run: python build_feeds.py
"""
from __future__ import annotations

import json
import logging
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import TypedDict


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

FEEDS_DIR    = Path("feeds")
HISTORY_DIR  = FEEDS_DIR / "history"

IMPLIED_VOL_FILE    = Path("implied_vol.json")
PRICE_OVERRIDE_FILE = Path("price_overrides.json")
EXCEL_OVERRIDE_FILE = Path("implied_vol_input.xlsx")
MANUAL_OHLC_FILE    = Path("manual_ohlc.json")

# ---------------------------------------------------------------------------
# Chicago timezone & date helpers
# ---------------------------------------------------------------------------

CT = ZoneInfo("America/Chicago")



class Contract(TypedDict, total=False):
    commodity:   str           # Human-readable name (e.g. "Cocoa")
    symbol:      str           # Yahoo Finance ticker (e.g. "CCN26.NYB")
    base_symbol: str           # Clean CME symbol (e.g. "CCN26")
    month:       str           # Contract month abbreviation (e.g. "Jul")
    roll_date:   str | None    # YYYY-MM-DD first date shown on home page (None = always)
    always_show: bool          # If True, always included on home page regardless of roll logic
    drop_date:   str | None    # YYYY-MM-DD on/after which this contract is excluded entirely
    history_start: str | None  # YYYY-MM-DD earliest date to include in history feed (None = all history)
    source:      str           # Data source; "manual" reads OHLC from manual_ohlc.json instead of Yahoo


# ---------------------------------------------------------------------------
# All contracts  (active + expired, ordered for home-page display by commodity)
# Contracts with the same commodity are ordered newest-first so the most
# recent always appears when a date falls within its roll window.
# ---------------------------------------------------------------------------

CONTRACTS: list[Contract] = [
    # Bitcoin — switched to continuous front-month (BTC=F) on 6/27;
    # per-contract-month tickers (BTCQ26 etc.) are unreliable on Yahoo —
    # many show "$0.00" or "data unavailable" even when CME lists them.
    {"commodity": "Bitcoin",          "symbol": "BTC=F",      "base_symbol": "BTC=F", "month": "Cont.", "roll_date": "2026-06-27", "history_start": "2026-06-05"},
    {"commodity": "Bitcoin",          "symbol": "BTCQ26.CME", "base_symbol": "BTCQ26", "month": "Aug", "roll_date": "2026-06-05"},

    # Canola Oil — ICE Canada (RS), continuous front month via TradingView RS1!.
    # No Yahoo feed; OHLC is entered by hand in manual_ohlc.json (source: manual).
    {"commodity": "Canola Oil",       "symbol": "RS1",        "base_symbol": "RS1", "month": "Cont.", "roll_date": "2026-07-14", "history_start": "2026-07-14", "source": "manual"},

    # Cocoa — Mar27 active from 11/13, Dec from 6/12, Jul from 4/20, May from 2/25, Mar fallback
    {"commodity": "Cocoa",            "symbol": "CCH27.NYB",  "base_symbol": "CCH27", "month": "Mar27", "roll_date": "2026-11-13"},
    {"commodity": "Cocoa",            "symbol": "CCZ26.NYB",  "base_symbol": "CCZ26", "month": "Dec", "roll_date": "2026-06-12"},
    {"commodity": "Cocoa",            "symbol": "CCN26.NYB",  "base_symbol": "CCN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Cocoa",            "symbol": "CCK26.NYB",  "base_symbol": "CCK26", "month": "May", "roll_date": "2026-02-25"},
    {"commodity": "Cocoa",            "symbol": "CCH26.NYB",  "base_symbol": "CCH26", "month": "Mar", "roll_date": None},

    # Coffee — Mar27 active from 11/13, Dec from 6/12, Jul from 4/20, May from 2/18, Mar fallback
    {"commodity": "Coffee",           "symbol": "KCH27.NYB",  "base_symbol": "KCH27", "month": "Mar27", "roll_date": "2026-11-13"},
    {"commodity": "Coffee",           "symbol": "KCZ26.NYB",  "base_symbol": "KCZ26", "month": "Dec", "roll_date": "2026-06-12"},
    {"commodity": "Coffee",           "symbol": "KCN26.NYB",  "base_symbol": "KCN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Coffee",           "symbol": "KCK26.NYB",  "base_symbol": "KCK26", "month": "May", "roll_date": "2026-02-18"},
    {"commodity": "Coffee",           "symbol": "KCH26.NYB",  "base_symbol": "KCH26", "month": "Mar", "roll_date": None},

    # Copper — Dec active from 9/27, Oct from 7/29, Aug from 6/5, Jul from 4/20, May from 2/26, Mar fallback
    {"commodity": "Copper",           "symbol": "HGZ26.CMX",  "base_symbol": "HGZ26", "month": "Dec", "roll_date": "2026-09-27"},
    {"commodity": "Copper",           "symbol": "HGV26.CMX",  "base_symbol": "HGV26", "month": "Oct", "roll_date": "2026-07-29"},
    {"commodity": "Copper",           "symbol": "HGQ26.CMX",  "base_symbol": "HGQ26", "month": "Aug", "roll_date": "2026-06-05"},
    {"commodity": "Copper",           "symbol": "HGN26.CMX",  "base_symbol": "HGN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Copper",           "symbol": "HGK26.CMX",  "base_symbol": "HGK26", "month": "May", "roll_date": "2026-02-26"},
    {"commodity": "Copper",           "symbol": "HGH26.CMX",  "base_symbol": "HGH26", "month": "Mar", "roll_date": None},

    # Corn — chronological: Dec26, Mar27, Dec27. Sep/Jul dropped 8/23.
    {"commodity": "Corn",             "symbol": "ZCZ26.CBT",  "base_symbol": "ZCZ26", "month": "Dec", "roll_date": None, "always_show": True},
    {"commodity": "Corn",             "symbol": "ZCH27.CBT",  "base_symbol": "ZCH27", "month": "Mar27", "roll_date": "2026-08-24"},
    {"commodity": "Corn",             "symbol": "ZCZ27.CBT",  "base_symbol": "ZCZ27", "month": "Dec27", "roll_date": "2026-06-17", "always_show": True},
    {"commodity": "Corn",             "symbol": "ZCU26.CBT",  "base_symbol": "ZCU26", "month": "Sep", "roll_date": "2026-06-27", "drop_date": "2026-08-23"},
    {"commodity": "Corn",             "symbol": "ZCN26.CBT",  "base_symbol": "ZCN26", "month": "Jul", "roll_date": None, "drop_date": "2026-08-23"},
    {"commodity": "Corn",             "symbol": "ZCH26.CBT",  "base_symbol": "ZCH26", "month": "Mar", "roll_date": None, "drop_date": "2026-08-23"},

    # Cotton — Mar27 active from 11/13, Dec from 6/12, Jul from 4/20
    {"commodity": "Cotton",           "symbol": "CTH27.NYB",  "base_symbol": "CTH27", "month": "Mar27", "roll_date": "2026-11-13"},
    {"commodity": "Cotton",           "symbol": "CTZ26.NYB",  "base_symbol": "CTZ26", "month": "Dec", "roll_date": "2026-06-12"},
    {"commodity": "Cotton",           "symbol": "CTN26.NYB",  "base_symbol": "CTN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Cotton",           "symbol": "CTK26.NYB",  "base_symbol": "CTK26", "month": "May", "roll_date": None},

    # Crude Oil — Dec active from 6/17, Jul active from 5/16, Jun expired, Apr expired 3/21
    {"commodity": "Crude Oil WTI",    "symbol": "CLZ26.NYM",  "base_symbol": "CLZ26", "month": "Dec", "roll_date": "2026-06-17"},
    {"commodity": "Crude Oil WTI",    "symbol": "CLN26.NYM",  "base_symbol": "CLN26", "month": "Jul", "roll_date": "2026-05-16"},
    {"commodity": "Crude Oil WTI",    "symbol": "CLM26.NYM",  "base_symbol": "CLM26", "month": "Jun", "roll_date": "2026-03-21"},
    {"commodity": "Crude Oil WTI",    "symbol": "CLJ26.NYM",  "base_symbol": "CLJ26", "month": "Apr", "roll_date": None},

    # Feeder Cattle — Jan27 active from 10/22, Oct from 8/14, Aug from 4/20
    {"commodity": "Feeder Cattle",    "symbol": "GFF27.CME",  "base_symbol": "GFF27", "month": "Jan27", "roll_date": "2026-10-22"},
    {"commodity": "Feeder Cattle",    "symbol": "GFV26.CME",  "base_symbol": "GFV26", "month": "Oct", "roll_date": "2026-08-14"},
    {"commodity": "Feeder Cattle",    "symbol": "GFQ26.CME",  "base_symbol": "GFQ26", "month": "Aug", "roll_date": "2026-04-20"},
    {"commodity": "Feeder Cattle",    "symbol": "GFK26.CME",  "base_symbol": "GFK26", "month": "May", "roll_date": None},

    # Gold — Dec active from 9/8, Oct active from 7/29, Aug from 5/27, Jun from 4/20, Apr from 2/26, Mar fallback
    {"commodity": "Gold",             "symbol": "GCZ26.CMX",  "base_symbol": "GCZ26", "month": "Dec", "roll_date": "2026-09-08"},
    {"commodity": "Gold",             "symbol": "GCV26.CMX",  "base_symbol": "GCV26", "month": "Oct", "roll_date": "2026-07-29"},
    {"commodity": "Gold",             "symbol": "GCQ26.CMX",  "base_symbol": "GCQ26", "month": "Aug", "roll_date": "2026-05-27"},
    {"commodity": "Gold",             "symbol": "GCM26.CMX",  "base_symbol": "GCM26", "month": "Jun", "roll_date": "2026-04-20"},
    {"commodity": "Gold",             "symbol": "GCJ26.CMX",  "base_symbol": "GCJ26", "month": "Apr", "roll_date": "2026-02-26"},
    {"commodity": "Gold",             "symbol": "GCH26.CMX",  "base_symbol": "GCH26", "month": "Mar", "roll_date": None},

    # Hard Red Wheat — Dec added 8/20; Sep drops 8/23; Jul dropped 6/27
    {"commodity": "Hard Red Wheat",   "symbol": "KEZ26.CBT",  "base_symbol": "KEZ26", "month": "Dec", "roll_date": "2026-08-20", "always_show": True},
    {"commodity": "Hard Red Wheat",   "symbol": "KEN26.CBT",  "base_symbol": "KEN26", "month": "Jul", "roll_date": None, "drop_date": "2026-06-27"},
    {"commodity": "Hard Red Wheat",   "symbol": "KEU26.CBT",  "base_symbol": "KEU26", "month": "Sep", "roll_date": "2026-06-18", "always_show": True, "drop_date": "2026-08-23"},

    # Heating Oil — Dec 2026 (HOZ26) added 8/24
    {"commodity": "Heating Oil",      "symbol": "HOZ26.NYM",   "base_symbol": "HOZ26", "month": "Dec", "roll_date": "2026-08-24", "history_start": "2026-08-24"},

    # Lean Hogs — rolled Oct->Dec on 10/22 (Oct retired; Dec was the always_show slot, now the front); Aug/Jun/Feb expired
    {"commodity": "Lean Hogs",        "symbol": "HEZ26.CME",  "base_symbol": "HEZ26", "month": "Dec", "roll_date": "2026-10-22"},
    {"commodity": "Lean Hogs",        "symbol": "HEV26.CME",  "base_symbol": "HEV26", "month": "Oct", "roll_date": "2026-07-29", "drop_date": "2026-10-22"},
    {"commodity": "Lean Hogs",        "symbol": "HEQ26.CME",  "base_symbol": "HEQ26", "month": "Aug", "roll_date": "2026-06-05"},
    {"commodity": "Lean Hogs",        "symbol": "HEM26.CME",  "base_symbol": "HEM26", "month": "Jun", "roll_date": "2026-02-14"},
    {"commodity": "Lean Hogs",        "symbol": "HEG26.CME",  "base_symbol": "HEG26", "month": "Feb", "roll_date": None},

    # Live Cattle — Dec active starting 10/2, Oct active from 7/29, Aug from 6/5, Jun from 4/20, Apr from 2/14, Feb fallback
    {"commodity": "Live Cattle",      "symbol": "LEZ26.CME",  "base_symbol": "LEZ26", "month": "Dec", "roll_date": "2026-10-01"},
    {"commodity": "Live Cattle",      "symbol": "LEV26.CME",  "base_symbol": "LEV26", "month": "Oct", "roll_date": "2026-07-29"},
    {"commodity": "Live Cattle",      "symbol": "LEQ26.CME",  "base_symbol": "LEQ26", "month": "Aug", "roll_date": "2026-06-05"},
    {"commodity": "Live Cattle",      "symbol": "LEM26.CME",  "base_symbol": "LEM26", "month": "Jun", "roll_date": "2026-04-20"},
    {"commodity": "Live Cattle",      "symbol": "LEJ26.CME",  "base_symbol": "LEJ26", "month": "Apr", "roll_date": "2026-02-14"},
    {"commodity": "Live Cattle",      "symbol": "LEG26.CME",  "base_symbol": "LEG26", "month": "Feb", "roll_date": None},

    # Nasdaq 100 E-Mini — rolled to Dec on 9/18 (Sep expired), Sep from 6/22 (quarterly cycle: Mar/Jun/Sep/Dec)
    {"commodity": "Nasdaq 100 E-Mini","symbol": "NQZ26.CME",  "base_symbol": "NQZ26", "month": "Dec", "roll_date": "2026-09-18"},
    {"commodity": "Nasdaq 100 E-Mini","symbol": "NQU26.CME",  "base_symbol": "NQU26", "month": "Sep", "roll_date": "2026-06-22"},
    {"commodity": "Nasdaq 100 E-Mini","symbol": "NQM26.CME",  "base_symbol": "NQM26", "month": "Jun", "roll_date": None},

    # Natural Gas — Dec active from 9/27, Oct from 7/29, Mar27 always_show (added 7/29), Aug from 6/29, Jul from 5/27, Jun from 2/26, Mar fallback
    {"commodity": "Natural Gas",      "symbol": "NGZ26.NYM",  "base_symbol": "NGZ26", "month": "Dec", "roll_date": "2026-09-27"},
    {"commodity": "Natural Gas",      "symbol": "NGV26.NYM",  "base_symbol": "NGV26", "month": "Oct", "roll_date": "2026-07-29"},
    {"commodity": "Natural Gas",      "symbol": "NGH27.NYM",  "base_symbol": "NGH27", "month": "Mar27", "roll_date": "2026-07-29", "always_show": True},
    {"commodity": "Natural Gas",      "symbol": "NGQ26.NYM",  "base_symbol": "NGQ26", "month": "Aug", "roll_date": "2026-06-29"},
    {"commodity": "Natural Gas",      "symbol": "NGN26.NYM",  "base_symbol": "NGN26", "month": "Jul", "roll_date": "2026-05-27"},
    {"commodity": "Natural Gas",      "symbol": "NGM26.NYM",  "base_symbol": "NGM26", "month": "Jun", "roll_date": "2026-02-26"},
    {"commodity": "Natural Gas",      "symbol": "NGH26.NYM",  "base_symbol": "NGH26", "month": "Mar", "roll_date": None},

    # Rice — switched to continuous front-month (ZR=F) on 7/14;
    # per-contract tickers unreliable on Yahoo (flat data, scale issues).
    {"commodity": "Rice",             "symbol": "ZR=F",        "base_symbol": "ZR=F", "month": "Cont.", "roll_date": "2026-07-14", "history_start": "2026-07-14"},
    {"commodity": "Rice",             "symbol": "ZRN26.CBT",   "base_symbol": "ZRN26", "month": "Jul", "roll_date": None},

    # S&P 500 E-Mini — rolled to Dec on 9/18 (Sep expired), Sep from 6/18 (quarterly cycle: Mar/Jun/Sep/Dec)
    {"commodity": "S&P 500 E-Mini",   "symbol": "ESZ26.CME",  "base_symbol": "ESZ26", "month": "Dec", "roll_date": "2026-09-18"},
    {"commodity": "S&P 500 E-Mini",   "symbol": "ESU26.CME",  "base_symbol": "ESU26", "month": "Sep", "roll_date": "2026-06-18"},
    {"commodity": "S&P 500 E-Mini",   "symbol": "ESM26.CME",  "base_symbol": "ESM26", "month": "Jun", "roll_date": None},

    # Silver — Dec active from 9/27, Oct from 7/29, Aug from 6/27, Jul from 5/27, Jun from 2/26, Mar fallback
    {"commodity": "Silver",           "symbol": "SIZ26.CMX",  "base_symbol": "SIZ26", "month": "Dec", "roll_date": "2026-09-27"},
    {"commodity": "Silver",           "symbol": "SIV26.CMX",  "base_symbol": "SIV26", "month": "Oct", "roll_date": "2026-07-29"},
    {"commodity": "Silver",           "symbol": "SIQ26.CMX",  "base_symbol": "SIQ26", "month": "Aug", "roll_date": "2026-06-27"},
    {"commodity": "Silver",           "symbol": "SIN26.CMX",  "base_symbol": "SIN26", "month": "Jul", "roll_date": "2026-05-27"},
    {"commodity": "Silver",           "symbol": "SIM26.CMX",  "base_symbol": "SIM26", "month": "Jun", "roll_date": "2026-02-26"},
    {"commodity": "Silver",           "symbol": "SIH26.CMX",  "base_symbol": "SIH26", "month": "Mar", "roll_date": None},

    # Soybean Meal — Dec added 8/20 as always_show; Sep drops 8/23
    {"commodity": "Soybean Meal",     "symbol": "ZMZ26.CBT",  "base_symbol": "ZMZ26", "month": "Dec", "roll_date": "2026-08-20", "always_show": True},
    {"commodity": "Soybean Meal",     "symbol": "ZMU26.CBT",  "base_symbol": "ZMU26", "month": "Sep", "roll_date": "2026-06-27", "drop_date": "2026-08-23"},
    {"commodity": "Soybean Meal",     "symbol": "ZMN26.CBT",  "base_symbol": "ZMN26", "month": "Jul", "roll_date": "2026-02-26", "drop_date": "2026-08-23"},
    {"commodity": "Soybean Meal",     "symbol": "ZMH26.CBT",  "base_symbol": "ZMH26", "month": "Mar", "roll_date": None, "drop_date": "2026-08-23"},

    # Soybean Oil — Dec added 8/20 as always_show; Sep drops 8/23
    {"commodity": "Soybean Oil",      "symbol": "ZLZ26.CBT",  "base_symbol": "ZLZ26", "month": "Dec", "roll_date": "2026-08-20", "always_show": True},
    {"commodity": "Soybean Oil",      "symbol": "ZLU26.CBT",  "base_symbol": "ZLU26", "month": "Sep", "roll_date": "2026-06-27", "drop_date": "2026-08-23"},
    {"commodity": "Soybean Oil",      "symbol": "ZLN26.CBT",  "base_symbol": "ZLN26", "month": "Jul", "roll_date": "2026-02-26", "drop_date": "2026-08-23"},
    {"commodity": "Soybean Oil",      "symbol": "ZLH26.CBT",  "base_symbol": "ZLH26", "month": "Mar", "roll_date": None, "drop_date": "2026-08-23"},

    # Soybeans — chronological: Nov26, Mar27, Nov27. Nov26 retired 10/25; Sep/Jul dropped 8/23.
    {"commodity": "Soybeans",         "symbol": "ZSX26.CBT",  "base_symbol": "ZSX26", "month": "Nov", "roll_date": None, "always_show": True, "drop_date": "2026-10-25"},
    {"commodity": "Soybeans",         "symbol": "ZSH27.CBT",  "base_symbol": "ZSH27", "month": "Mar27", "roll_date": "2026-08-24"},
    {"commodity": "Soybeans",         "symbol": "ZSX27.CBT",  "base_symbol": "ZSX27", "month": "Nov27", "roll_date": "2026-06-17", "always_show": True},
    {"commodity": "Soybeans",         "symbol": "ZSU26.CBT",  "base_symbol": "ZSU26", "month": "Sep", "roll_date": "2026-06-27", "drop_date": "2026-08-23"},
    {"commodity": "Soybeans",         "symbol": "ZSN26.CBT",  "base_symbol": "ZSN26", "month": "Jul", "roll_date": None, "drop_date": "2026-08-23"},
    {"commodity": "Soybeans",         "symbol": "ZSH26.CBT",  "base_symbol": "ZSH26", "month": "Mar", "roll_date": None, "drop_date": "2026-08-23"},

    # Sugar — Mar27 active from 9/15 (no Dec contract for Sugar #11), Oct from 6/5
    {"commodity": "Sugar",            "symbol": "SBH27.NYB",  "base_symbol": "SBH27", "month": "Mar27", "roll_date": "2026-09-15"},
    {"commodity": "Sugar",            "symbol": "SBV26.NYB",  "base_symbol": "SBV26", "month": "Oct", "roll_date": "2026-06-05"},

    # US Dollar — rolled to Dec on 9/15 (Sep expired), Sep from 6/15 (quarterly cycle: Mar/Jun/Sep/Dec)
    {"commodity": "US Dollar",        "symbol": "DXZ26.NYB",  "base_symbol": "DXZ26", "month": "Dec", "roll_date": "2026-09-15"},
    {"commodity": "US Dollar",        "symbol": "DXU26.NYB",  "base_symbol": "DXU26", "month": "Sep", "roll_date": "2026-06-15"},
    {"commodity": "US Dollar",        "symbol": "DXM26.NYB",  "base_symbol": "DXM26", "month": "Jun", "roll_date": None},

    # Unleaded Gasoline (RBOB) — new from 6/30, using continuous front-month (RB=F)
    {"commodity": "Unleaded Gasoline","symbol": "RB=F",        "base_symbol": "RB=F", "month": "Cont.", "roll_date": "2026-06-30", "history_start": "2026-06-30"},

    # Wheat — Dec added 8/20; Sep drops 8/23; Jul dropped 6/27
    {"commodity": "Wheat",            "symbol": "ZWZ26.CBT",  "base_symbol": "ZWZ26", "month": "Dec", "roll_date": "2026-08-20", "always_show": True},
    {"commodity": "Wheat",            "symbol": "ZWN26.CBT",  "base_symbol": "ZWN26", "month": "Jul", "roll_date": None, "drop_date": "2026-06-27"},
    {"commodity": "Wheat",            "symbol": "ZWU26.CBT",  "base_symbol": "ZWU26", "month": "Sep", "roll_date": "2026-06-18", "always_show": True, "drop_date": "2026-08-23"},

    # 10-Year T-Note — Dec 2026 (ZNZ26) added 8/24, at bottom of home page
    {"commodity": "10-Year T-Note",   "symbol": "ZNZ26.CBT",   "base_symbol": "ZNZ26", "month": "Dec", "roll_date": "2026-08-24", "history_start": "2026-08-24"},
]

# Lookup of base_symbol -> Contract
CONTRACT_BY_SYMBOL: dict[str, Contract] = {c["base_symbol"]: c for c in CONTRACTS}

# Home page display order — unique commodities in display order
# (used to sort rows; we pick one contract per commodity per date)
COMMODITY_ORDER: list[str] = [
    "Canola Oil",
    "Cocoa", "Coffee", "Copper", "Corn", "Corn",
    "Cotton", "Crude Oil WTI", "Feeder Cattle", "Gold",
    "Hard Red Wheat", "Lean Hogs", "Live Cattle",
    "Nasdaq 100 E-Mini", "Natural Gas", "Rice",
    "S&P 500 E-Mini", "Silver", "Soybean Meal", "Soybean Oil",
    "Soybeans", "Soybeans", "US Dollar", "Wheat",
]

# Stable per-symbol order for sorting (all symbols, newest contract first per commodity)
HOME_ORDER: list[str] = [c["base_symbol"] for c in CONTRACTS]


def active_symbol_for_date(commodity: str, date_str: str) -> str:
    """
    Return the base_symbol of the contract that should appear on the home page
    for the given commodity and date.

    Picks the newest contract whose roll_date <= date_str,
    falling back to the contract with roll_date=None.
    """
    # Collect all contracts for this commodity, excluding always_show contracts
    # (those are separate fixed slots, handled directly in active_symbols_for_date)
    # and excluding contracts whose drop_date has passed (fully retired).
    candidates = [
        c for c in CONTRACTS
        if c["commodity"] == commodity
        and not c.get("always_show")
        and not (c.get("drop_date") is not None and c["drop_date"] <= date_str)
    ]
    if not candidates:
        return ""

    # Find the best match: latest roll_date that is <= date_str
    best = None
    for c in candidates:
        rd = c["roll_date"]
        if rd is None:
            if best is None:
                best = c          # fallback
        elif rd <= date_str:
            if best is None or (best["roll_date"] or "") < rd:
                best = c

    if best is not None:
        return best["base_symbol"]
    # No candidate's roll_date has been reached yet, and no roll_date=None
    # fallback exists for this commodity — nothing should show yet.
    return ""


def active_symbols_for_date(date_str: str) -> list[str]:
    """Return the ordered list of base_symbols active on the home page for date_str.
    
    Contracts with always_show=True are included regardless of the commodity's
    main (single-pick) roll logic, but still respect their own roll_date —
    e.g. ZCZ27 only appears once date_str >= its roll_date.
    For other contracts, one is selected per commodity based on roll_date.
    """
    seen_commodities: list[str] = []
    result: list[str] = []
    for c in CONTRACTS:
        commodity = c["commodity"]
        if c.get("always_show"):
            rd = c.get("roll_date")
            if rd is not None and rd > date_str:
                continue  # not yet active
            dd = c.get("drop_date")
            if dd is not None and dd <= date_str:
                continue  # expired/retired
            result.append(c["base_symbol"])
            continue
        expected = active_symbol_for_date(commodity, date_str)
        if expected == c["base_symbol"] and commodity not in seen_commodities:
            result.append(c["base_symbol"])
            seen_commodities.append(commodity)
    return result


# ---------------------------------------------------------------------------
# Tick sizes
# ---------------------------------------------------------------------------

TICK_SIZES: dict[str, float] = {
    "Bitcoin":           5.0,
    "Canola Oil":        0.1,
    "Cocoa":             1.0,
    "Coffee":            0.05,
    "Copper":            0.0005,
    "Corn":              0.25,
    "Cotton":            0.01,
    "Crude Oil WTI":     0.01,
    "Feeder Cattle":     0.025,
    "Gold":              0.1,
    "Hard Red Wheat":    0.25,
    "Lean Hogs":         0.025,
    "Live Cattle":       0.025,
    "Nasdaq 100 E-Mini": 0.25,
    "Natural Gas":       0.001,
    "Rice":              0.5,
    "Unleaded Gasoline": 0.0001,
    "S&P 500 E-Mini":    0.25,
    "Silver":            0.005,
    "Soybean Meal":      0.1,
    "Soybean Oil":       0.01,
    "Soybeans":          0.25,
    "US Dollar":         0.005,
    "Wheat":             0.25,
    "Heating Oil":       0.0001,
    "10-Year T-Note":    0.015625,
    "Sugar":             0.01,
}


# ---------------------------------------------------------------------------
# Formula constants
# ---------------------------------------------------------------------------

PRICE_DIVISOR: int = 100
HV_TARGET_MULTIPLIER: float = 0.80
HV_ANNUALIZATION_FACTOR: int = 16
WEEKLY_TARGET_LOOKBACK: int = 3
DAILY_TARGET_LOOKBACK: int = 3


# ---------------------------------------------------------------------------
# CME holidays 2026
# ---------------------------------------------------------------------------

CME_HOLIDAYS: frozenset[str] = frozenset({
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
})


# ---------------------------------------------------------------------------
# Feed settings
# ---------------------------------------------------------------------------

YAHOO_RANGE: str = "6mo"
YAHOO_INTERVAL: str = "1d"
FETCH_WORKERS: int = 4
FETCH_DELAY: float = 0.25
FETCH_MAX_RETRIES: int = 3


# ---------------------------------------------------------------------------
# Options feed configuration
# ---------------------------------------------------------------------------

# Commodities whose yfinance spot price is in cents but CME strikes are in dollars.
CENTS_COMMODITIES: frozenset[str] = frozenset({
    "Corn", "Soybeans", "Wheat", "Hard Red Wheat",
    "Soybean Meal", "Soybean Oil",
    "Copper", "Live Cattle", "Lean Hogs", "Feeder Cattle",
})

# ETF proxy for IV estimation via yfinance options chains.
ETF_IV_PROXY: dict[str, str | None] = {
    "Gold":          "GLD",
    "Silver":        "SLV",
    "Corn":          "CORN",
    "Wheat":         "WEAT",
    "Hard Red Wheat":"WEAT",
    "Soybeans":      "SOYB",
    "Copper":        None,
    "Soybean Meal":  None,
    "Soybean Oil":   None,
    "Live Cattle":   None,
    "Lean Hogs":     None,
    "Feeder Cattle": None,
}

# yfinance continuous ticker for spot price lookup per commodity.
OPTIONS_TICKER_MAP: dict[str, str | None] = {
    "Corn":              "ZC=F",
    "Soybeans":          "ZS=F",
    "Wheat":             "ZW=F",
    "Hard Red Wheat":    "KE=F",
    "Soybean Meal":      "ZM=F",
    "Soybean Oil":       "ZL=F",
    "Gold":              "GC=F",
    "Silver":            "SI=F",
    "Copper":            "HG=F",
    "Live Cattle":       "LE=F",
    "Lean Hogs":         "HE=F",
    "Feeder Cattle":     "GF=F",
}

# Month offset: options expire the month BEFORE delivery for grains (-1), same month for others (0).
EXPIRY_OFFSET: dict[str, int] = {
    "Corn":           -1,
    "Soybeans":       -1,
    "Wheat":          -1,
    "Hard Red Wheat": -1,
    "Soybean Meal":   -1,
    "Soybean Oil":    -1,
    "Gold":            0,
    "Silver":          0,
    "Copper":          0,
    "Live Cattle":     0,
    "Lean Hogs":       0,
    "Feeder Cattle":   0,
}

# CME month letter → calendar month number
MONTH_LETTER_TO_NUM: dict[str, int] = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

# CME product IDs for options OI fetching
CME_PRODUCT_MAP: dict[str, int] = {
    "Corn":           300,
    "Soybean Meal":   310,
    "Soybean Oil":    312,
    "Soybeans":       320,
    "Wheat":          323,
    "Hard Red Wheat": 348,
    "Gold":           437,
    "Copper":         438,
    "Silver":         458,
    "Lean Hogs":       19,
    "Live Cattle":     22,
    "Feeder Cattle":   34,
}

RISK_FREE_RATE: float = 0.05
OI_HISTORY_WINDOW: int = 15
OI_LOOKBACK: int = 10



# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

RawRow = dict  # {timestamp: int, high: float, low: float, close: float}


# ---------------------------------------------------------------------------
# Tick helpers
# ---------------------------------------------------------------------------

def round_to_tick(value: float, tick: float) -> float:
    """Round value to the nearest tick increment."""
    return round(round(value / tick) * tick, 10)


def format_tick(value: float, tick: float) -> str:
    """
    Format a price value using the correct number of decimal places for its tick.
    Strips trailing zeros while preserving meaningful precision.

    Examples:
        format_tick(1175.00, 0.25) -> "1175"
        format_tick(6.0955, 0.0005) -> "6.0955"
        format_tick(25.10, 0.1) -> "25.1"
    """
    if "." in str(tick):
        decimals = len(str(tick).rstrip("0").split(".")[1])
        formatted = f"{round(value, decimals):.{decimals}f}"
        return formatted.rstrip("0").rstrip(".")
    return str(int(round(value)))


# ---------------------------------------------------------------------------
# Yahoo Finance fetcher
# ---------------------------------------------------------------------------

def fetch_yahoo_history(symbol: str) -> list[RawRow]:
    """
    Fetch daily OHLC history from Yahoo Finance for the given symbol.

    For contracts where daily data returns flat H==L (bad data), automatically
    falls back to hourly data aggregated into daily bars.

    Returns rows sorted newest-first, with None values filtered out.
    Retries on transient network errors with exponential backoff.

    Raises:
        RuntimeError: if all retry attempts fail.
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol, safe='')}"
        f"?range={YAHOO_RANGE}&interval={YAHOO_INTERVAL}"
        f"&includePrePost=false&events=div%2Csplits"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    rows = _fetch_url(url, symbol, headers)

    # Detect flat data: if >50% of rows have high == low, data is bad
    # Fall back to hourly data aggregated to daily
    if rows and sum(1 for r in rows if r["high"] == r["low"]) > len(rows) * 0.5:
        log.warning("%s: flat OHLC detected — falling back to hourly aggregation", symbol)
        hourly_url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol, safe='')}"
            f"?range=2mo&interval=1h&includePrePost=false"
        )
        try:
            hourly_rows = _fetch_url(hourly_url, symbol, headers)
            aggregated = _aggregate_hourly_to_daily(hourly_rows)
            if aggregated:
                return aggregated
        except Exception as e:
            log.warning("%s: hourly fallback failed: %s", symbol, e)

    return rows


def _fetch_url(url: str, symbol: str, headers: dict) -> list[RawRow]:
    """Internal fetch with retry logic."""
    last_error: Exception | None = None
    for attempt in range(FETCH_MAX_RETRIES):
        if attempt > 0:
            backoff = FETCH_DELAY * (2 ** attempt)
            log.warning(
                "Retrying %s (attempt %d/%d) after %.1fs backoff",
                symbol, attempt + 1, FETCH_MAX_RETRIES, backoff,
            )
            time.sleep(backoff)
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=30) as resp:
                data = json.load(resp)
            return _parse_yahoo_response(data)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
            last_error = e
            log.debug("Fetch attempt %d failed for %s: %s", attempt + 1, symbol, e)
        except (KeyError, IndexError) as e:
            # Structural parse error — not worth retrying
            raise RuntimeError(f"Unexpected Yahoo response structure for {symbol}: {e}") from e

    raise RuntimeError(
        f"Failed to fetch {symbol} after {FETCH_MAX_RETRIES} attempts: {last_error}"
    )


def _aggregate_hourly_to_daily(rows: list[RawRow]) -> list[RawRow]:
    """
    Aggregate hourly bars into daily OHLC bars.

    Stores timestamps as midnight UTC of the prior day — matching Yahoo's
    daily bar convention — so ts_to_ct_date() computes the correct trade date.
    """
    from collections import defaultdict
    from datetime import datetime, timezone, timedelta
    from zoneinfo import ZoneInfo

    CT = ZoneInfo("America/Chicago")
    daily: dict[str, dict] = defaultdict(lambda: {"high": None, "low": None, "close": None})

    for row in rows:
        # Use Chicago time to assign hourly bar to its trade date
        dt_ct = datetime.fromtimestamp(row["timestamp"], tz=CT)
        date_str = dt_ct.strftime("%Y-%m-%d")
        d = daily[date_str]
        d["high"]  = row["high"] if d["high"] is None else max(d["high"], row["high"])
        d["low"]   = row["low"]  if d["low"]  is None else min(d["low"],  row["low"])
        d["close"] = row["close"]

    result = []
    for date_str, d in daily.items():
        if d["high"] is None or d["high"] == d["low"]:
            continue
        # Build a midnight-UTC timestamp for the prior day so ts_to_ct_date
        # adds 1 day and recovers the correct trade date
        prior_midnight = datetime.fromisoformat(date_str) - timedelta(days=1)
        fake_ts = int(prior_midnight.replace(tzinfo=timezone.utc).timestamp())
        result.append({
            "timestamp": fake_ts,
            "high":      d["high"],
            "low":       d["low"],
            "close":     d["close"],
        })

    result.sort(key=lambda r: r["timestamp"], reverse=True)
    return result


def _parse_yahoo_response(data: dict) -> list[RawRow]:
    """Extract and validate OHLC rows from a Yahoo Finance chart response."""
    result = data["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]

    rows: list[RawRow] = []
    for ts, high, low, close in zip(
        result.get("timestamp", []),
        quote.get("high", []),
        quote.get("low", []),
        quote.get("close", []),
    ):
        # Yahoo sometimes returns None for missing bars
        if None in (ts, high, low, close):
            continue
        rows.append({
            "timestamp": int(ts),
            "high": float(high),
            "low": float(low),
            "close": float(close),
        })

    rows.sort(key=lambda r: r["timestamp"], reverse=True)
    return rows

def ts_to_ct_date(ts: int) -> str:
    """
    Convert a Yahoo Finance Unix timestamp to a Chicago-time trade date.
    Yahoo stores timestamps at midnight UTC for the *previous* calendar day,
    so adding one day corrects to the actual trade date.
    """
    return (datetime.fromtimestamp(ts, tz=CT) + timedelta(days=1)).strftime("%Y-%m-%d")


def is_trading_day(date_str: str) -> bool:
    """Return True if date_str is a weekday that is not a CME holiday."""
    try:
        d = date.fromisoformat(date_str)
    except ValueError:
        return False
    return d.weekday() < 5 and date_str not in CME_HOLIDAYS


def week_monday(date_str: str) -> str:
    """Return the ISO date of the Monday that starts the week containing date_str."""
    d = date.fromisoformat(date_str)
    return (d - timedelta(days=d.weekday())).isoformat()


def is_last_trading_day_of_week(date_str: str) -> bool:
    """True if date_str is the last trading day (Friday or earlier if holiday) of its week."""
    d = date.fromisoformat(date_str)
    friday = d + timedelta(days=4 - d.weekday())
    candidate = friday
    for _ in range(5):
        iso = candidate.isoformat()
        if candidate.weekday() < 5 and iso not in CME_HOLIDAYS:
            return date_str == iso
        candidate -= timedelta(days=1)
    return False


def is_first_trading_day_of_week(date_str: str) -> bool:
    """True if date_str is the first trading day (Monday or later if holiday) of its week."""
    d = date.fromisoformat(date_str)
    monday = d - timedelta(days=d.weekday())
    candidate = monday
    for _ in range(5):
        iso = candidate.isoformat()
        if candidate.weekday() < 5 and iso not in CME_HOLIDAYS:
            return date_str == iso
        candidate += timedelta(days=1)
    return False


# ---------------------------------------------------------------------------
# Override loaders
# ---------------------------------------------------------------------------

def load_json_file(path: Path) -> dict:
    """Load a JSON file, returning an empty dict on missing file or parse error."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Could not load %s: %s", path, e)
        return {}


def load_implied_vol() -> dict[str, dict[str, float]]:
    """
    Load implied volatility data from implied_vol.json.
    Format: { "YYYY-MM-DD": { "SYMBOL": float_percent } }
    """
    data = load_json_file(IMPLIED_VOL_FILE)
    if data:
        log.info("Loaded implied vol for %d dates", len(data))
    return data


def load_price_overrides() -> dict[str, dict[str, dict[str, float]]]:
    """
    Load manual price corrections from price_overrides.json.
    Format: { "YYYY-MM-DD": { "SYMBOL": { "high": float, "low": float } } }
    """
    data = load_json_file(PRICE_OVERRIDE_FILE)
    if data:
        log.info("Loaded price overrides for %d dates", len(data))
    return data


def load_rice_overrides() -> dict[str, dict[str, float]]:
    """
    Load Rice manual OHLC data from the Excel workbook.
    Yahoo Finance data for Rice is unreliable; this provides correct values.
    Looks for sheets named "Rice Override (ZRN26)", "Rice Override (ZRU26)", etc.
    All matching sheets are merged; later dates win on conflict.
    Format: { "YYYY-MM-DD": { "high": float, "low": float, "close": float } }
    """
    if not EXCEL_OVERRIDE_FILE.exists():
        return {}
    try:
        from openpyxl import load_workbook  # lazy import — only needed if file exists
        wb = load_workbook(EXCEL_OVERRIDE_FILE, data_only=True)
        # Find all Rice Override sheets (any symbol)
        rice_sheets = [s for s in wb.sheetnames if s.startswith("Rice Override")]
        if not rice_sheets:
            return {}
        result: dict[str, dict[str, float]] = {}
        for sheet_name in rice_sheets:
            ws = wb[sheet_name]
            for date_val, high, low, close, *_ in ws.iter_rows(min_row=3, values_only=True):
                if date_val is None or high is None or low is None:
                    continue
                s = str(date_val)
                for fmt in ("%Y-%m-%d", "%m/%d/%y"):
                    try:
                        date_str = (
                            date_val.strftime("%Y-%m-%d")
                            if hasattr(date_val, "strftime")
                            else __import__("datetime").datetime.strptime(s, fmt).strftime("%Y-%m-%d")
                        )
                        break
                    except (ValueError, AttributeError):
                        continue
                else:
                    continue
                result[date_str] = {
                    "high": float(high),
                    "low": float(low),
                    "close": float(close) if close is not None else 0.0,
                }
        if result:
            log.info("Loaded Rice overrides for %d dates", len(result))
        return result
    except Exception as e:
        log.warning("Could not load Rice overrides from Excel: %s", e)
        return {}


def load_manual_ohlc() -> dict[str, dict[str, dict[str, float]]]:
    """
    Load manually-entered OHLC for contracts that have no Yahoo source
    (e.g. Canola / RS1, sourced from TradingView and pasted by hand).

    Format: { "SYMBOL": { "YYYY-MM-DD": { "high": float, "low": float,
                                          "close": float } } }
    Keyed by base_symbol, then by trade date. `open` is not required —
    the range/target formulas only use high, low and close.
    """
    data = load_json_file(MANUAL_OHLC_FILE)
    if data:
        total = sum(len(v) for v in data.values())
        log.info("Loaded manual OHLC for %d symbol(s), %d rows", len(data), total)
    return data


def build_manual_rows(symbol: str, manual_data: dict) -> list[RawRow]:
    """
    Turn a manual per-date OHLC map into RawRows compatible with the rest of
    the pipeline. Synthesises a timestamp for each trade date such that
    ts_to_ct_date() recovers exactly that date (noon CT on the prior day, so
    the +1-day convention lands on the intended trade date and DST edges are
    avoided). Returns rows newest-first, matching fetch_yahoo_history().
    """
    per_symbol = manual_data.get(symbol, {})
    rows: list[RawRow] = []
    for date_str, ohlc in per_symbol.items():
        try:
            d = date.fromisoformat(date_str)
        except ValueError:
            log.warning("Manual OHLC %s: bad date %r — skipping", symbol, date_str)
            continue
        prior_noon_ct = datetime(d.year, d.month, d.day, 12, tzinfo=CT) - timedelta(days=1)
        rows.append({
            "timestamp": int(prior_noon_ct.timestamp()),
            "high":      float(ohlc["high"]),
            "low":       float(ohlc["low"]),
            "close":     float(ohlc["close"]),
        })
    rows.sort(key=lambda r: r["timestamp"], reverse=True)
    return rows


# ---------------------------------------------------------------------------
# Row pre-processing
# ---------------------------------------------------------------------------

def preprocess_rows(
    raw_rows: list[RawRow],
    symbol: str,
    today: str,
    price_overrides: dict,
    rice_overrides: dict,
) -> list[RawRow]:
    """
    Apply all corrections to raw Yahoo rows and filter to valid trading days <= today.

    Processing order:
        1. Apply price_overrides (corrects bad Yahoo highs/lows for any contract)
        2. Apply rice_overrides (replaces all Yahoo data for ZRN26)
        3. Filter: keep only valid trading days on or before today
    """
    result: list[RawRow] = []
    for row in raw_rows:
        trade_date = ts_to_ct_date(row["timestamp"])

        # Skip future dates and non-trading days
        if trade_date > today or not is_trading_day(trade_date):
            continue

        # Apply generic price overrides
        if day := price_overrides.get(trade_date, {}).get(symbol):
            row = dict(row)  # avoid mutating original
            row["high"]  = float(day.get("high",  row["high"]))
            row["low"]   = float(day.get("low",   row["low"]))
            row["close"] = float(day.get("close", row["close"]))

        # Apply Rice-specific overrides (works for any ZR* symbol: ZRN26, ZRU26, etc.)
        if symbol.startswith("ZR") and (override := rice_overrides.get(trade_date)):
            row = dict(row)
            row["high"]  = override["high"]
            row["low"]   = override["low"]
            row["close"] = override["close"] or row["close"]

        # Yahoo changed Rice pricing from cents/cwt to dollars/cwt
        # If price is < 100, multiply by 100 to restore correct scale
        if symbol.startswith("ZR") and row.get("high", 0) < 100:
            row = dict(row)
            row["high"]  = round(row["high"]  * 100, 4)
            row["low"]   = round(row["low"]   * 100, 4)
            row["close"] = round(row["close"] * 100, 4)

        result.append(row)

    return result


# ---------------------------------------------------------------------------
# Tick-level computations
# ---------------------------------------------------------------------------

def daily_range(row: RawRow, tick: float, prev_close: float | None = None) -> float:
    """
    True Range = max(High - Low, |High - Prev Close|, |Low - Prev Close|)
    Falls back to High - Low when no previous close is available.
    """
    high  = round_to_tick(row["high"], tick)
    low   = round_to_tick(row["low"],  tick)
    hl    = round_to_tick(high - low, tick)
    if prev_close is None:
        return hl
    pc = round_to_tick(prev_close, tick)
    return round_to_tick(max(hl, abs(high - pc), abs(low - pc)), tick)


def pct(numerator: float | None, denominator: float | None) -> str:
    """Format numerator/denominator as a percentage string, e.g. '95.5%'."""
    if not numerator or not denominator:
        return ""
    return f"{round((numerator / denominator) * 100, 1)}%"


def historic_vol(rows: list[RawRow], i: int, tick: float) -> str:
    """
    Historic Vol = (avg_3day_range * HV_TARGET_MULT) / (close / PRICE_DIVISOR) * HV_ANNUALIZATION

    Uses a rolling window of DAILY_TARGET_LOOKBACK days starting at index i.
    Returns '' if insufficient data.
    """
    window = rows[i : i + DAILY_TARGET_LOOKBACK]
    if len(window) < DAILY_TARGET_LOOKBACK or not window[0].get("close"):
        return ""
    # Pass previous close for True Range (rows are newest-first, so next index = prior day)
    avg_range = sum(
        daily_range(window[j], tick, rows[i + j + 1].get("close") if i + j + 1 < len(rows) else None)
        for j in range(DAILY_TARGET_LOOKBACK)
    ) / DAILY_TARGET_LOOKBACK
    close_one_pct = window[0]["close"] / PRICE_DIVISOR
    hv = (avg_range * HV_TARGET_MULTIPLIER / close_one_pct) * HV_ANNUALIZATION_FACTOR
    return f"{round(hv, 1)}%"


def full_achievement_and_target(
    rows: list[RawRow], i: int, tick: float
) -> tuple[float | None, float | None]:
    """
    Return (avg_range, daily_target) for the window starting at index i.
    avg_range is the average of DAILY_TARGET_LOOKBACK daily ranges.
    daily_target is avg_range * HV_TARGET_MULTIPLIER.
    Returns (None, None) if insufficient data.
    """
    window = rows[i : i + DAILY_TARGET_LOOKBACK]
    if len(window) < DAILY_TARGET_LOOKBACK:
        return None, None
    # Pass previous close for True Range
    avg = sum(
        daily_range(window[j], tick, rows[i + j + 1].get("close") if i + j + 1 < len(rows) else None)
        for j in range(DAILY_TARGET_LOOKBACK)
    ) / DAILY_TARGET_LOOKBACK
    avg_r  = round_to_tick(avg, tick)
    target = round_to_tick(avg_r * HV_TARGET_MULTIPLIER, tick)
    return avg_r, target


# ---------------------------------------------------------------------------
# Weekly computations
# ---------------------------------------------------------------------------

WeeklyData = dict  # date_str -> {weeklyHigh, weeklyLow, weeklyRange}
WeeklyTargets = dict  # date_str -> {weeklyTarget, nextWeeklyTarget}


def compute_weekly_ranges(dated_rows: list[dict], tick: float) -> WeeklyData:
    """
    Compute cumulative weekly high/low/range for each trading day.
    Ranges expand Mon→Fri as new highs/lows are made during the week.
    Accounts for gap opens: if Monday opens with a gap vs prior Friday's close,
    the gap is included in the weekly range.
    """
    # Build a date -> close lookup for gap detection
    close_by_date: dict[str, float] = {
        r["date"]: r["close"] for r in dated_rows if r.get("close")
    }

    # Sort all rows by date to find prior Friday close easily
    sorted_dates = sorted(close_by_date.keys())

    def prior_close(date_str: str) -> float | None:
        """Return the close of the most recent prior trading day."""
        idx = sorted_dates.index(date_str) if date_str in sorted_dates else -1
        if idx <= 0:
            return None
        return close_by_date.get(sorted_dates[idx - 1])

    # Group rows by their week's Monday
    weeks: dict[str, list[dict]] = {}
    for row in dated_rows:
        weeks.setdefault(week_monday(row["date"]), []).append(row)

    result: WeeklyData = {}
    for week_rows in weeks.values():
        week_rows.sort(key=lambda r: r["date"])
        cum_high = cum_low = None
        for row in week_rows:
            h = round_to_tick(row["high"], tick)
            l = round_to_tick(row["low"], tick)
            # On first day of week, check for gap vs prior close
            if cum_high is None:
                pc = prior_close(row["date"])
                if pc is not None:
                    pc = round_to_tick(pc, tick)
                    # Gap up: prior close below today's low
                    # Gap down: prior close above today's high
                    h = max(h, pc)
                    l = min(l, pc)
            cum_high = h if cum_high is None else max(cum_high, h)
            cum_low  = l if cum_low  is None else min(cum_low,  l)
            result[row["date"]] = {
                "weeklyHigh":  cum_high,
                "weeklyLow":   cum_low,
                "weeklyRange": round_to_tick(cum_high - cum_low, tick),
            }
    return result


def compute_completed_weeks(dated_rows: list[dict], tick: float) -> list[dict]:
    """
    Return completed weeks (those ending on a valid last trading day), newest first.
    Each entry: {monday, lastDay, high, low, range}
    Accounts for gap opens on Monday vs prior Friday close.
    """
    # Build close lookup for gap detection
    close_by_date: dict[str, float] = {
        r["date"]: r["close"] for r in dated_rows if r.get("close")
    }
    sorted_dates = sorted(close_by_date.keys())

    def prior_close(date_str: str) -> float | None:
        idx = sorted_dates.index(date_str) if date_str in sorted_dates else -1
        if idx <= 0:
            return None
        return close_by_date.get(sorted_dates[idx - 1])

    weeks: dict[str, list[dict]] = {}
    for row in dated_rows:
        weeks.setdefault(week_monday(row["date"]), []).append(row)

    completed = []
    for monday, week_rows in sorted(weeks.items(), reverse=True):
        week_rows.sort(key=lambda r: r["date"])
        last_day = week_rows[-1]["date"]
        if not is_last_trading_day_of_week(last_day):
            continue
        high = round_to_tick(max(r["high"] for r in week_rows), tick)
        low  = round_to_tick(min(r["low"]  for r in week_rows), tick)
        # Include gap on Monday
        first_day = week_rows[0]["date"]
        pc = prior_close(first_day)
        if pc is not None:
            pc = round_to_tick(pc, tick)
            high = max(high, pc)
            low  = min(low,  pc)
        completed.append({
            "monday":  monday,
            "lastDay": last_day,
            "high":    high,
            "low":     low,
            "range":   round_to_tick(high - low, tick),
        })
    return completed


def compute_weekly_targets(dated_rows: list[dict], tick: float) -> WeeklyTargets:
    """
    For each trading day, determine the applicable weekly target and next weekly target.

    Weekly target for week W = avg range of the 3 prior completed weeks.
    nextWeeklyTarget is shown on the last trading day of the week (for Friday display).
    """
    completed = compute_completed_weeks(dated_rows, tick)

    # Map each week's Monday to its target (derived from the 3 prior completed weeks)
    target_by_monday: dict[str, float] = {}
    for i in range(len(completed) - WEEKLY_TARGET_LOOKBACK + 1):
        window = completed[i : i + WEEKLY_TARGET_LOOKBACK]
        avg = sum(w["range"] for w in window) / WEEKLY_TARGET_LOOKBACK
        next_mon = (date.fromisoformat(completed[i]["monday"]) + timedelta(weeks=1)).isoformat()
        target_by_monday[next_mon] = round_to_tick(avg, tick)

    # nextWeeklyTarget: available on the last trading day of a completed week
    next_target_by_last_day: dict[str, float] = {}
    for week in completed:
        next_mon = (date.fromisoformat(week["monday"]) + timedelta(weeks=1)).isoformat()
        if target := target_by_monday.get(next_mon):
            next_target_by_last_day[week["lastDay"]] = target

    return {
        row["date"]: {
            "weeklyTarget":     target_by_monday.get(week_monday(row["date"])),
            "nextWeeklyTarget": next_target_by_last_day.get(row["date"]),
        }
        for row in dated_rows
    }


# ---------------------------------------------------------------------------
# Implied vol trend
# ---------------------------------------------------------------------------

def compute_iv_trends(history_rows: list[dict]) -> list[str]:
    """
    For each row (newest-first) compute the implied vol trend string.

    Format: "up|N" or "down|N" where N is the consecutive-day streak count.
    Returns "" for the oldest data point or when no prior IV exists.

    Processes oldest-to-newest internally for streak counting, then reverses.
    """
    reversed_rows = list(reversed(history_rows))
    trends = [""] * len(reversed_rows)

    for i, row in enumerate(reversed_rows):
        raw = row.get("impliedVol", "")
        if not raw:
            continue
        try:
            iv = float(raw.replace("%", ""))
        except ValueError:
            continue

        # Find the most recent prior row with an IV value
        prev_iv: float | None = None
        for j in range(i - 1, -1, -1):
            prev_raw = reversed_rows[j].get("impliedVol", "")
            if prev_raw:
                try:
                    prev_iv = float(prev_raw.replace("%", ""))
                    break
                except ValueError:
                    continue

        if prev_iv is None:
            continue

        if iv > prev_iv:
            direction = "up"
        elif iv < prev_iv:
            direction = "down"
        else:
            continue  # unchanged — no trend marker

        # Count consecutive days in this direction
        count = 1
        for j in range(i - 1, -1, -1):
            prev_trend = trends[j]
            if not prev_trend or prev_trend.split("|")[0] != direction:
                break
            count += 1

        trends[i] = f"{direction}|{count}"

    return list(reversed(trends))


# ---------------------------------------------------------------------------
# History builder
# ---------------------------------------------------------------------------

def build_history(rows: list[RawRow], contract: Contract, iv_data: dict) -> list[dict]:
    """
    Build the full history row list for a single contract.

    Args:
        rows:     Pre-processed (filtered, overridden) Yahoo rows, newest-first.
        contract: Contract config dict.
        iv_data:  Full implied vol data keyed by date → symbol → float.

    Returns:
        List of history row dicts, newest-first, ready for JSON serialisation.
    """
    tick = TICK_SIZES[contract["commodity"]]
    symbol = contract["base_symbol"]

    # Build lightweight dated rows for weekly computations
    dated = [
        {"date": ts_to_ct_date(r["timestamp"]), "high": r["high"], "low": r["low"], "close": r.get("close")}
        for r in rows
    ]

    weekly_ranges  = compute_weekly_ranges(dated, tick)
    weekly_targets = compute_weekly_targets(dated, tick)

    # ---- First pass: build core row data ----
    history: list[dict] = []
    for i, row in enumerate(rows):
        trade_date = ts_to_ct_date(row["timestamp"])
        d_high = round_to_tick(row["high"], tick)
        d_low  = round_to_tick(row["low"],  tick)
        # True Range: include overnight gap vs previous close (rows newest-first, so i+1 = prior day)
        prev_close = rows[i + 1].get("close") if i + 1 < len(rows) else None
        d_range = daily_range(row, tick, prev_close)

        full_ach, next_target = full_achievement_and_target(rows, i, tick)
        hv = historic_vol(rows, i, tick)

        iv_val = iv_data.get(trade_date, {}).get(symbol)
        iv_str = f"{round(float(iv_val), 1)}%" if iv_val is not None and float(iv_val) > 0 else ""

        wr = weekly_ranges.get(trade_date, {})
        wt = weekly_targets.get(trade_date, {})

        history.append({
            "date":            trade_date,
            "dailyHigh":       format_tick(d_high, tick),
            "dailyLow":        format_tick(d_low,  tick),
            "dailyRange":      format_tick(d_range, tick),
            "fullAchievement": format_tick(full_ach, tick) if full_ach is not None else "",
            "_fullAch":        full_ach,    # temp — used in second pass
            "_nextTarget":     next_target, # temp — used in second pass
            "historicVol":     hv,
            "impliedVol":      iv_str,
            "weeklyHigh":      format_tick(wr["weeklyHigh"],  tick) if wr else "",
            "weeklyLow":       format_tick(wr["weeklyLow"],   tick) if wr else "",
            "weeklyRange":     format_tick(wr["weeklyRange"],  tick) if wr else "",
            "_weeklyTarget":   wt.get("weeklyTarget"),   # temp
            "_nextWeeklyTarget": wt.get("nextWeeklyTarget"), # temp
            "sectionBreak":    False,
        })

    # ---- Second pass: targets, achievements, section breaks ----
    for i, row in enumerate(history):
        prev = history[i + 1] if i + 1 < len(history) else None

        daily_target = prev["_nextTarget"] if prev else None
        d_range_num  = float(row["dailyRange"]) if row["dailyRange"] else None
        w_range_num  = float(row["weeklyRange"]) if row["weeklyRange"] else None
        weekly_target = row["_weeklyTarget"]
        next_weekly   = row["_nextWeeklyTarget"]

        row["dailyTarget"]      = format_tick(daily_target, tick) if daily_target else ""
        row["nextDailyTarget"]  = format_tick(row["_nextTarget"], tick) if row["_nextTarget"] else ""
        row["dailyAchievement"] = pct(d_range_num, daily_target)
        row["weeklyTarget"]     = format_tick(weekly_target, tick) if weekly_target else ""
        row["nextWeeklyTarget"] = format_tick(next_weekly, tick) if next_weekly else ""
        row["weeklyAchievement"]= pct(w_range_num, weekly_target)
        row["sectionBreak"]     = is_first_trading_day_of_week(row["date"])

        # Remove temp keys
        del row["_fullAch"], row["_nextTarget"], row["_weeklyTarget"], row["_nextWeeklyTarget"]

    # ---- Third pass: implied vol trends ----
    trends = compute_iv_trends(history)
    for row, trend in zip(history, trends):
        row["impliedVolTrend"] = trend

    # ---- Fourth pass: under-target streak (oldest to newest) ----
    # history is newest-first so iterate in reverse to build streak correctly
    streak = 0
    for row in reversed(history):
        d_range  = float(row["dailyRange"])  if row["dailyRange"]  else None
        d_target = float(row["dailyTarget"]) if row["dailyTarget"] else None
        if d_range is not None and d_target and d_range < d_target:
            streak += 1
        else:
            streak = 0
        row["underTargetStreak"] = streak

    # ---- Fifth pass: weekly trend arrows (per completed week, oldest to newest) ----
    # Green arrow (up) when weekly range >= target, red arrow (down) when range < target
    # Counter tracks consecutive weeks in same direction
    from collections import defaultdict
    weeks: dict = defaultdict(list)
    for row in history:
        weeks[week_monday(row["date"])].append(row)

    weekly_streak = 0
    weekly_direction = ""
    for monday in sorted(weeks.keys()):
        week_rows = weeks[monday]
        last_day = max(r["date"] for r in week_rows)
        trend_str = ""
        if is_last_trading_day_of_week(last_day):
            w_range  = float(week_rows[0]["weeklyRange"])  if week_rows[0].get("weeklyRange")  else None
            w_target = float(week_rows[0]["weeklyTarget"]) if week_rows[0].get("weeklyTarget") else None
            if w_range is not None and w_target:
                direction = "up" if w_range >= w_target else "down"
                if direction == weekly_direction:
                    weekly_streak += 1
                else:
                    weekly_streak = 1
                    weekly_direction = direction
                trend_str = f"{direction}|{weekly_streak}"
        # Apply trend to all rows in this week
        # For incomplete weeks, carry forward last completed week's trend
        applied_trend = trend_str if trend_str else weekly_direction and f"{weekly_direction}|{weekly_streak}" or ""
        for row in week_rows:
            row["weeklyUnderTargetStreak"] = 0  # kept for compatibility
            row["weeklyTrend"] = applied_trend

    # Filter history to history_start date if specified (e.g. continuous front-month
    # tickers like BTC=F, RB=F, ZR=F should not show pre-roll history)
    history_start = contract.get("history_start")
    if history_start:
        history = [row for row in history if row["date"] >= history_start]

    return history


# ---------------------------------------------------------------------------
# Overview row builder
# ---------------------------------------------------------------------------

def to_overview_row(history_row: dict, contract: Contract) -> dict:
    """Flatten a history row + contract metadata into an overview row."""
    return {
        "date":            history_row["date"],
        "symbol":          contract["base_symbol"],
        "commodity":       contract["commodity"],
        "month":           contract["month"],
        "dailyTarget":     history_row["dailyTarget"],
        "dailyRange":      history_row["dailyRange"],
        "dailyHigh":       history_row["dailyHigh"],
        "dailyLow":        history_row["dailyLow"],
        "dailyAchievement":history_row["dailyAchievement"],
        "fullAchievement": history_row["fullAchievement"],
        "nextDailyTarget": history_row["nextDailyTarget"],
        "historicVol":     history_row["historicVol"],
        "impliedVol":      history_row["impliedVol"],
        "impliedVolTrend": history_row["impliedVolTrend"],
        "weeklyRange":     history_row["weeklyRange"],
        "weeklyHigh":      history_row["weeklyHigh"],
        "weeklyLow":       history_row["weeklyLow"],
        "weeklyAchievement":history_row["weeklyAchievement"],
        "weeklyTarget":    history_row["weeklyTarget"],
        "nextWeeklyTarget":history_row["nextWeeklyTarget"],
        "underTargetStreak":history_row.get("underTargetStreak", 0),
        "weeklyUnderTargetStreak":history_row.get("weeklyUnderTargetStreak", 0),
        "weeklyTrend":history_row.get("weeklyTrend", ""),
    }


# ---------------------------------------------------------------------------
# Contract processor  (runs in thread pool)
# ---------------------------------------------------------------------------

def process_contract(
    contract: Contract,
    today: str,
    price_overrides: dict,
    rice_overrides: dict,
    iv_data: dict,
    manual_data: dict,
) -> tuple[str, list[dict] | Exception]:
    """
    Fetch and process a single contract. Returns (base_symbol, history_rows | Exception).
    Designed to run in a thread pool — all inputs are read-only.

    Contracts marked "source": "manual" are built from manual_ohlc.json instead
    of Yahoo (used for tickers with no Yahoo feed, e.g. Canola / RS1).
    """
    symbol = contract["base_symbol"]
    try:
        if contract.get("source") == "manual":
            raw_rows = build_manual_rows(symbol, manual_data)
        else:
            time.sleep(FETCH_DELAY)
            raw_rows = fetch_yahoo_history(contract["symbol"])
        clean_rows = preprocess_rows(raw_rows, symbol, today, price_overrides, rice_overrides)
        history = build_history(clean_rows, contract, iv_data)
        log.info("OK  %s  (%d rows)", symbol, len(history))
        return symbol, history
    except Exception as exc:
        log.error("ERR %s: %s", symbol, exc)
        return symbol, exc


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    FEEDS_DIR.mkdir(exist_ok=True)
    HISTORY_DIR.mkdir(exist_ok=True)

    today    = datetime.now(CT).strftime("%Y-%m-%d")
    now_ct   = datetime.now(CT).isoformat()

    log.info("Building feeds for %s (today = %s)", now_ct[:10], today)

    # Load all override/supplement data upfront
    iv_data         = load_implied_vol()
    price_overrides = load_price_overrides()
    rice_overrides  = load_rice_overrides()
    manual_data     = load_manual_ohlc()

    # ---- Parallel fetch & process ----
    results: dict[str, list[dict]] = {}
    errors:  list[dict] = []

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        future_map: dict[Future, Contract] = {
            pool.submit(
                process_contract,
                contract, today, price_overrides, rice_overrides, iv_data, manual_data,
            ): contract
            for contract in CONTRACTS
        }
        # Collect results preserving HOME_ORDER by iterating in submission order
        for future in future_map:
            symbol, result = future.result()
            if isinstance(result, Exception):
                errors.append({"symbol": symbol, "error": str(result)})
            else:
                results[symbol] = result

    # ---- Write per-contract history feeds ----
    history_index: list[str] = []
    all_overview_rows: list[dict] = []

    for contract in CONTRACTS:
        symbol = contract["base_symbol"]
        if symbol not in results:
            continue

        history_rows = results[symbol]
        payload = {
            "symbol":    symbol,
            "commodity": contract["commodity"],
            "month":     contract["month"],
            "updatedAt": now_ct,
            "rows":      history_rows,
        }
        (HISTORY_DIR / f"{symbol}.json").write_text(
            json.dumps(payload, indent=2), encoding="utf-8"
        )

        all_overview_rows.extend(
            to_overview_row(row, contract) for row in history_rows
        )
        history_index.append(symbol)

    # ---- Write overview-by-date feed (date-aware contract selection) ----
    # For each date, include the preferred contract per commodity.
    # Contracts with always_show=True (e.g. ZCZ26, ZSX26) are always included.
    # If the preferred contract has no data (expired), fall back to any contract
    # for that commodity that does have data.

    # Symbols that always appear regardless of roll logic
    always_show_syms = {c["base_symbol"] for c in CONTRACTS if c.get("always_show")}

    # Index all rows by (date, symbol)
    rows_by_date_sym: dict[str, dict[str, dict]] = {}
    for row in all_overview_rows:
        rows_by_date_sym.setdefault(row["date"], {})[row["symbol"]] = row

    overview_by_date: dict[str, list[dict]] = {}
    for date_str, sym_map in rows_by_date_sym.items():
        preferred = active_symbols_for_date(date_str)
        date_rows = []
        added_syms: set[str] = set()
        seen_commodities: list[str] = []

        for sym in preferred:
            contract = CONTRACT_BY_SYMBOL.get(sym)
            if not contract:
                continue
            commodity = contract["commodity"]
            is_always = sym in always_show_syms

            # always_show contracts bypass the one-per-commodity rule
            if not is_always and commodity in seen_commodities:
                continue

            if sym in sym_map:
                date_rows.append(sym_map[sym])
                added_syms.add(sym)
                if not is_always:
                    seen_commodities.append(commodity)
            elif not is_always:
                # Preferred contract has no data — fall back
                for c in CONTRACTS:
                    if c["commodity"] == commodity and c["base_symbol"] in sym_map:
                        date_rows.append(sym_map[c["base_symbol"]])
                        added_syms.add(c["base_symbol"])
                        seen_commodities.append(commodity)
                        break

        # Ensure always_show contracts are included — but only those actually
        # active for this date. `preferred` already honors roll_date + drop_date,
        # so a dropped/expired always_show contract (e.g. KEU26 after its
        # drop_date) stays out instead of reappearing just because Yahoo still
        # returns data for it.
        preferred_set = set(preferred)
        for sym in always_show_syms:
            if sym in preferred_set and sym not in added_syms and sym in sym_map:
                date_rows.append(sym_map[sym])

        # Sort by HOME_ORDER
        order_map = {sym: i for i, sym in enumerate(HOME_ORDER)}
        date_rows.sort(key=lambda r: order_map.get(r["symbol"], 9999))
        if date_rows:
            overview_by_date[date_str] = date_rows

    (FEEDS_DIR / "overview-by-date.json").write_text(
        json.dumps(overview_by_date, indent=2), encoding="utf-8"
    )

    # ---- Write ancillary feeds ----
    (FEEDS_DIR / "errors.json").write_text(
        json.dumps(errors, indent=2), encoding="utf-8"
    )
    (HISTORY_DIR / "index.json").write_text(
        json.dumps({"contracts": history_index}, indent=2), encoding="utf-8"
    )
    (FEEDS_DIR / "meta.json").write_text(
        json.dumps({
            "builtAt":      now_ct,
            "status":       "ok" if not errors else "partial",
            "successCount": len(results),
            "errorCount":   len(errors),
            "version":      "v4.0",
        }, indent=2),
        encoding="utf-8",
    )

    # ---- Options feed (non-fatal; lives in src/options_feed.py + src/config.py) ----
    try:
        from src.options_feed import build_options_feed
        options_data = build_options_feed(today, feeds_dir=FEEDS_DIR)
        (FEEDS_DIR / "options-feed.json").write_text(
            json.dumps(options_data, indent=2), encoding="utf-8"
        )
        log.info("Options feed written → feeds/options-feed.json")
    except Exception as _opt_exc:
        log.warning("Options feed build failed (non-fatal): %s", _opt_exc)

    log.info(
        "Done — %d/%d contracts (%d errors)",
        len(results), len(CONTRACTS), len(errors),
    )
    if errors:
        for e in errors:
            log.warning("  Failed: %s — %s", e["symbol"], e["error"])


if __name__ == "__main__":
    main()
