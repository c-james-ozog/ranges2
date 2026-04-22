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

class Contract(TypedDict):
    commodity:  str            # Human-readable name (e.g. "Cocoa")
    symbol:     str            # Yahoo Finance ticker (e.g. "CCN26.NYB")
    base_symbol: str           # Clean CME symbol (e.g. "CCN26")
    month:      str            # Contract month abbreviation (e.g. "Jul")
    roll_date:  str | None     # YYYY-MM-DD first date shown on home page (None = always)


# ---------------------------------------------------------------------------
# All contracts  (active + expired, ordered for home-page display by commodity)
# Contracts with the same commodity are ordered newest-first so the most
# recent always appears when a date falls within its roll window.
# ---------------------------------------------------------------------------

CONTRACTS: list[Contract] = [
    # Cocoa — rolled to Jul on 4/20
    {"commodity": "Cocoa",            "symbol": "CCN26.NYB",  "base_symbol": "CCN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Cocoa",            "symbol": "CCK26.NYB",  "base_symbol": "CCK26", "month": "May", "roll_date": None},

    # Coffee — rolled to Jul on 4/20
    {"commodity": "Coffee",           "symbol": "KCN26.NYB",  "base_symbol": "KCN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Coffee",           "symbol": "KCK26.NYB",  "base_symbol": "KCK26", "month": "May", "roll_date": None},

    # Copper — rolled to Jul on 4/20
    {"commodity": "Copper",           "symbol": "HGN26.CMX",  "base_symbol": "HGN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Copper",           "symbol": "HGK26.CMX",  "base_symbol": "HGK26", "month": "May", "roll_date": None},

    # Corn — no roll
    {"commodity": "Corn",             "symbol": "ZCN26.CBT",  "base_symbol": "ZCN26", "month": "Jul", "roll_date": None},
    {"commodity": "Corn",             "symbol": "ZCZ26.CBT",  "base_symbol": "ZCZ26", "month": "Dec", "roll_date": None},

    # Cotton — rolled to Jul on 4/20
    {"commodity": "Cotton",           "symbol": "CTN26.NYB",  "base_symbol": "CTN26", "month": "Jul", "roll_date": "2026-04-20"},
    {"commodity": "Cotton",           "symbol": "CTK26.NYB",  "base_symbol": "CTK26", "month": "May", "roll_date": None},

    # Crude Oil — no roll
    {"commodity": "Crude Oil WTI",    "symbol": "CLM26.NYM",  "base_symbol": "CLM26", "month": "Jun", "roll_date": None},

    # Feeder Cattle — rolled to Aug on 4/20
    {"commodity": "Feeder Cattle",    "symbol": "GFQ26.CME",  "base_symbol": "GFQ26", "month": "Aug", "roll_date": "2026-04-20"},
    {"commodity": "Feeder Cattle",    "symbol": "GFK26.CME",  "base_symbol": "GFK26", "month": "May", "roll_date": None},

    # Gold — rolled to Jun on 4/20
    {"commodity": "Gold",             "symbol": "GCM26.CMX",  "base_symbol": "GCM26", "month": "Jun", "roll_date": "2026-04-20"},
    {"commodity": "Gold",             "symbol": "GCJ26.CMX",  "base_symbol": "GCJ26", "month": "Apr", "roll_date": None},

    # Hard Red Wheat — no roll
    {"commodity": "Hard Red Wheat",   "symbol": "KEN26.CBT",  "base_symbol": "KEN26", "month": "Jul", "roll_date": None},

    # Lean Hogs — no roll
    {"commodity": "Lean Hogs",        "symbol": "HEM26.CME",  "base_symbol": "HEM26", "month": "Jun", "roll_date": None},

    # Live Cattle — rolled to Jun on 4/20
    {"commodity": "Live Cattle",      "symbol": "LEM26.CME",  "base_symbol": "LEM26", "month": "Jun", "roll_date": "2026-04-20"},
    {"commodity": "Live Cattle",      "symbol": "LEJ26.CME",  "base_symbol": "LEJ26", "month": "Apr", "roll_date": None},

    # Nasdaq — no roll
    {"commodity": "Nasdaq 100 E-Mini","symbol": "NQM26.CME",  "base_symbol": "NQM26", "month": "Jun", "roll_date": None},

    # Natural Gas — no roll
    {"commodity": "Natural Gas",      "symbol": "NGM26.NYM",  "base_symbol": "NGM26", "month": "Jun", "roll_date": None},

    # Rice — no roll
    {"commodity": "Rice",             "symbol": "ZRN26.CBT",  "base_symbol": "ZRN26", "month": "Jul", "roll_date": None},

    # S&P 500 — no roll
    {"commodity": "S&P 500 E-Mini",   "symbol": "ESM26.CME",  "base_symbol": "ESM26", "month": "Jun", "roll_date": None},

    # Silver — no roll
    {"commodity": "Silver",           "symbol": "SIM26.CMX",  "base_symbol": "SIM26", "month": "Jun", "roll_date": None},

    # Soybean Meal — no roll
    {"commodity": "Soybean Meal",     "symbol": "ZMN26.CBT",  "base_symbol": "ZMN26", "month": "Jul", "roll_date": None},

    # Soybean Oil — no roll
    {"commodity": "Soybean Oil",      "symbol": "ZLN26.CBT",  "base_symbol": "ZLN26", "month": "Jul", "roll_date": None},

    # Soybeans — no roll
    {"commodity": "Soybeans",         "symbol": "ZSN26.CBT",  "base_symbol": "ZSN26", "month": "Jul", "roll_date": None},
    {"commodity": "Soybeans",         "symbol": "ZSX26.CBT",  "base_symbol": "ZSX26", "month": "Nov", "roll_date": None},

    # US Dollar — no roll
    {"commodity": "US Dollar",        "symbol": "DXM26.NYB",  "base_symbol": "DXM26", "month": "Jun", "roll_date": None},

    # Wheat — no roll
    {"commodity": "Wheat",            "symbol": "ZWN26.CBT",  "base_symbol": "ZWN26", "month": "Jul", "roll_date": None},
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
    # Collect all contracts for this commodity
    candidates = [c for c in CONTRACTS if c["commodity"] == commodity]
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

    return best["base_symbol"] if best else candidates[-1]["base_symbol"]


def active_symbols_for_date(date_str: str) -> list[str]:
    """Return the ordered list of base_symbols active on the home page for date_str."""
    seen_commodities: list[str] = []
    result: list[str] = []
    for c in CONTRACTS:
        commodity = c["commodity"]
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
    "S&P 500 E-Mini":    0.25,
    "Silver":            0.005,
    "Soybean Meal":      0.1,
    "Soybean Oil":       0.01,
    "Soybeans":          0.25,
    "US Dollar":         0.005,
    "Wheat":             0.25,
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
