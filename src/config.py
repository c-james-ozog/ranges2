"""
Contract configuration for ranges2 feed builder.

roll_date: the first date this contract appears on the home page.
           For dates before roll_date the prior contract for that commodity is shown.
           None means always active from the beginning of history.
"""

from typing import TypedDict


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class Contract(TypedDict, total=False):
    commodity:   str           # Human-readable name (e.g. "Cocoa")
    symbol:      str           # Yahoo Finance ticker (e.g. "CCN26.NYB")
    base_symbol: str           # Clean CME symbol (e.g. "CCN26")
    month:       str           # Contract month abbreviation (e.g. "Jul")
    roll_date:   str | None    # YYYY-MM-DD first date shown on home page (None = always)
    always_show: bool          # If True, always included on home page regardless of roll logic
    drop_date:   str | None    # YYYY-MM-DD on/after which this contract is excluded entirely
    history_start: str | None  # YYYY-MM-DD earliest date to include in history feed (None = all history)


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

    # Cocoa — rolled to Dec on 6/12, was Jul from 4/20, May from 2/25, Mar fallback
    {"commodity": "Cocoa",            "symbol": "CCZ26.NYB",  "base_symbol": "CCZ26", "month": "Dec", "roll_date": "2026-06-12"},
    {"commodity": "Cocoa",            "symbol": "CCN26.NYB",  "base_symbol": "CCN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Cocoa",            "symbol": "CCK26.NYB",  "base_symbol": "CCK26", "month": "May", "roll_date": "2026-02-25"},
    {"commodity": "Cocoa",            "symbol": "CCH26.NYB",  "base_symbol": "CCH26", "month": "Mar", "roll_date": None},

    # Coffee — rolled to Dec on 6/12, was Jul from 4/20, May from 2/18, Mar fallback
    {"commodity": "Coffee",           "symbol": "KCZ26.NYB",  "base_symbol": "KCZ26", "month": "Dec", "roll_date": "2026-06-12"},
    {"commodity": "Coffee",           "symbol": "KCN26.NYB",  "base_symbol": "KCN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Coffee",           "symbol": "KCK26.NYB",  "base_symbol": "KCK26", "month": "May", "roll_date": "2026-02-18"},
    {"commodity": "Coffee",           "symbol": "KCH26.NYB",  "base_symbol": "KCH26", "month": "Mar", "roll_date": None},

    # Copper — Oct active from 7/29, Aug from 6/5, Jul from 4/20, May from 2/26, Mar fallback
    {"commodity": "Copper",           "symbol": "HGV26.CMX",  "base_symbol": "HGV26", "month": "Oct", "roll_date": "2026-07-29"},
    {"commodity": "Copper",           "symbol": "HGQ26.CMX",  "base_symbol": "HGQ26", "month": "Aug", "roll_date": "2026-06-05"},
    {"commodity": "Copper",           "symbol": "HGN26.CMX",  "base_symbol": "HGN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Copper",           "symbol": "HGK26.CMX",  "base_symbol": "HGK26", "month": "May", "roll_date": "2026-02-26"},
    {"commodity": "Copper",           "symbol": "HGH26.CMX",  "base_symbol": "HGH26", "month": "Mar", "roll_date": None},

    # Corn — Sep active from 6/27, drops 8/23; Dec/Dec27 always_show slots remain
    {"commodity": "Corn",             "symbol": "ZCH27.CBT",  "base_symbol": "ZCH27", "month": "Mar27", "roll_date": "2026-08-24"},
    {"commodity": "Corn",             "symbol": "ZCU26.CBT",  "base_symbol": "ZCU26", "month": "Sep", "roll_date": "2026-06-27", "drop_date": "2026-08-23"},
    {"commodity": "Corn",             "symbol": "ZCN26.CBT",  "base_symbol": "ZCN26", "month": "Jul", "roll_date": None, "drop_date": "2026-08-23"},
    {"commodity": "Corn",             "symbol": "ZCZ26.CBT",  "base_symbol": "ZCZ26", "month": "Dec", "roll_date": None, "always_show": True},
    {"commodity": "Corn",             "symbol": "ZCZ27.CBT",  "base_symbol": "ZCZ27", "month": "Dec27", "roll_date": "2026-06-17", "always_show": True},
    {"commodity": "Corn",             "symbol": "ZCH26.CBT",  "base_symbol": "ZCH26", "month": "Mar", "roll_date": None, "drop_date": "2026-08-23"},

    # Cotton — rolled to Dec on 6/12, was Jul from 4/20
    {"commodity": "Cotton",           "symbol": "CTZ26.NYB",  "base_symbol": "CTZ26", "month": "Dec", "roll_date": "2026-06-12"},
    {"commodity": "Cotton",           "symbol": "CTN26.NYB",  "base_symbol": "CTN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Cotton",           "symbol": "CTK26.NYB",  "base_symbol": "CTK26", "month": "May", "roll_date": None},

    # Crude Oil — Dec active from 6/17, Jul active from 5/16, Jun expired, Apr expired 3/21
    {"commodity": "Crude Oil WTI",    "symbol": "CLZ26.NYM",  "base_symbol": "CLZ26", "month": "Dec", "roll_date": "2026-06-17"},
    {"commodity": "Crude Oil WTI",    "symbol": "CLN26.NYM",  "base_symbol": "CLN26", "month": "Jul", "roll_date": "2026-05-16"},
    {"commodity": "Crude Oil WTI",    "symbol": "CLM26.NYM",  "base_symbol": "CLM26", "month": "Jun", "roll_date": "2026-03-21"},
    {"commodity": "Crude Oil WTI",    "symbol": "CLJ26.NYM",  "base_symbol": "CLJ26", "month": "Apr", "roll_date": None},

    # Feeder Cattle — Oct active from 8/14, Aug from 4/20
    {"commodity": "Feeder Cattle",    "symbol": "GFV26.CME",  "base_symbol": "GFV26", "month": "Oct", "roll_date": "2026-08-14"},
    {"commodity": "Feeder Cattle",    "symbol": "GFQ26.CME",  "base_symbol": "GFQ26", "month": "Aug", "roll_date": "2026-04-20"},
    {"commodity": "Feeder Cattle",    "symbol": "GFK26.CME",  "base_symbol": "GFK26", "month": "May", "roll_date": None},

    # Gold — Oct active from 7/29, Aug from 5/27, Jun from 4/20, Apr from 2/26, Mar fallback
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

    # Lean Hogs — Oct active from 7/29, Dec added 7/29 (always_show second slot), Aug from 6/5, Jun from 2/14, Feb fallback
    {"commodity": "Lean Hogs",        "symbol": "HEV26.CME",  "base_symbol": "HEV26", "month": "Oct", "roll_date": "2026-07-29"},
    {"commodity": "Lean Hogs",        "symbol": "HEZ26.CME",  "base_symbol": "HEZ26", "month": "Dec", "roll_date": "2026-07-29", "always_show": True},
    {"commodity": "Lean Hogs",        "symbol": "HEQ26.CME",  "base_symbol": "HEQ26", "month": "Aug", "roll_date": "2026-06-05"},
    {"commodity": "Lean Hogs",        "symbol": "HEM26.CME",  "base_symbol": "HEM26", "month": "Jun", "roll_date": "2026-02-14"},
    {"commodity": "Lean Hogs",        "symbol": "HEG26.CME",  "base_symbol": "HEG26", "month": "Feb", "roll_date": None},

    # Live Cattle — Oct active from 7/29, Aug from 6/5, Jun from 4/20, Apr from 2/14, Feb fallback
    {"commodity": "Live Cattle",      "symbol": "LEV26.CME",  "base_symbol": "LEV26", "month": "Oct", "roll_date": "2026-07-29"},
    {"commodity": "Live Cattle",      "symbol": "LEQ26.CME",  "base_symbol": "LEQ26", "month": "Aug", "roll_date": "2026-06-05"},
    {"commodity": "Live Cattle",      "symbol": "LEM26.CME",  "base_symbol": "LEM26", "month": "Jun", "roll_date": "2026-04-20"},
    {"commodity": "Live Cattle",      "symbol": "LEJ26.CME",  "base_symbol": "LEJ26", "month": "Apr", "roll_date": "2026-02-14"},
    {"commodity": "Live Cattle",      "symbol": "LEG26.CME",  "base_symbol": "LEG26", "month": "Feb", "roll_date": None},

    # Nasdaq 100 E-Mini — rolled to Sep on 6/22 (quarterly cycle: Mar/Jun/Sep/Dec)
    {"commodity": "Nasdaq 100 E-Mini","symbol": "NQU26.CME",  "base_symbol": "NQU26", "month": "Sep", "roll_date": "2026-06-22"},
    {"commodity": "Nasdaq 100 E-Mini","symbol": "NQM26.CME",  "base_symbol": "NQM26", "month": "Jun", "roll_date": None},

    # Natural Gas — Oct active from 7/29, Mar27 added 7/29 (always_show second slot), Aug from 6/29, Jul from 5/27, Jun from 2/26, Mar fallback
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

    # S&P 500 E-Mini — rolled to Sep on 6/18 (quarterly cycle: Mar/Jun/Sep/Dec)
    {"commodity": "S&P 500 E-Mini",   "symbol": "ESU26.CME",  "base_symbol": "ESU26", "month": "Sep", "roll_date": "2026-06-18"},
    {"commodity": "S&P 500 E-Mini",   "symbol": "ESM26.CME",  "base_symbol": "ESM26", "month": "Jun", "roll_date": None},

    # Silver — Oct active from 7/29, Aug from 6/27, Jul from 5/27, Jun from 2/26, Mar fallback
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

    # Soybeans — Sep active from 6/27, drops 8/23; Nov/Nov27 always_show slots remain
    {"commodity": "Soybeans",         "symbol": "ZSH27.CBT",  "base_symbol": "ZSH27", "month": "Mar27", "roll_date": "2026-08-24"},
    {"commodity": "Soybeans",         "symbol": "ZSU26.CBT",  "base_symbol": "ZSU26", "month": "Sep", "roll_date": "2026-06-27", "drop_date": "2026-08-23"},
    {"commodity": "Soybeans",         "symbol": "ZSN26.CBT",  "base_symbol": "ZSN26", "month": "Jul", "roll_date": None, "drop_date": "2026-08-23"},
    {"commodity": "Soybeans",         "symbol": "ZSX26.CBT",  "base_symbol": "ZSX26", "month": "Nov", "roll_date": None, "always_show": True},
    {"commodity": "Soybeans",         "symbol": "ZSX27.CBT",  "base_symbol": "ZSX27", "month": "Nov27", "roll_date": "2026-06-17", "always_show": True},
    {"commodity": "Soybeans",         "symbol": "ZSH26.CBT",  "base_symbol": "ZSH26", "month": "Mar", "roll_date": None, "drop_date": "2026-08-23"},

    # Sugar — new from 6/5
    {"commodity": "Sugar",            "symbol": "SBV26.NYB",  "base_symbol": "SBV26", "month": "Oct", "roll_date": "2026-06-05"},

    # US Dollar — rolled to Sep on 6/15 (quarterly cycle: Mar/Jun/Sep/Dec)
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
