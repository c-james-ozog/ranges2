"""
Contract configuration for ranges2 feed builder.

All commodity definitions, tick sizes, and formula constants live here.
"""

from typing import TypedDict


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class Contract(TypedDict):
    commodity: str    # Human-readable name (e.g. "Cocoa")
    symbol: str       # Yahoo Finance ticker (e.g. "CCN26.NYB")
    base_symbol: str  # Clean CME symbol (e.g. "CCN26")
    month: str        # Contract month abbreviation (e.g. "Jul")


# ---------------------------------------------------------------------------
# Active contracts  (shown on home page, ordered for display)
# ---------------------------------------------------------------------------

ACTIVE_CONTRACTS: list[Contract] = [
    {"commodity": "Cocoa",            "symbol": "CCN26.NYB",  "base_symbol": "CCN26", "month": "Jul"},
    {"commodity": "Coffee",           "symbol": "KCN26.NYB",  "base_symbol": "KCN26", "month": "Jul"},
    {"commodity": "Copper",           "symbol": "HGN26.CMX",  "base_symbol": "HGN26", "month": "Jul"},
    {"commodity": "Corn",             "symbol": "ZCN26.CBT",  "base_symbol": "ZCN26", "month": "Jul"},
    {"commodity": "Corn",             "symbol": "ZCZ26.CBT",  "base_symbol": "ZCZ26", "month": "Dec"},
    {"commodity": "Cotton",           "symbol": "CTN26.NYB",  "base_symbol": "CTN26", "month": "Jul"},
    {"commodity": "Crude Oil WTI",    "symbol": "CLM26.NYM",  "base_symbol": "CLM26", "month": "Jun"},
    {"commodity": "Feeder Cattle",    "symbol": "GFQ26.CME",  "base_symbol": "GFQ26", "month": "Aug"},
    {"commodity": "Gold",             "symbol": "GCM26.CMX",  "base_symbol": "GCM26", "month": "Jun"},
    {"commodity": "Hard Red Wheat",   "symbol": "KEN26.CBT",  "base_symbol": "KEN26", "month": "Jul"},
    {"commodity": "Lean Hogs",        "symbol": "HEM26.CME",  "base_symbol": "HEM26", "month": "Jun"},
    {"commodity": "Live Cattle",      "symbol": "LEM26.CME",  "base_symbol": "LEM26", "month": "Jun"},
    {"commodity": "Nasdaq 100 E-Mini","symbol": "NQM26.CME",  "base_symbol": "NQM26", "month": "Jun"},
    {"commodity": "Natural Gas",      "symbol": "NGM26.NYM",  "base_symbol": "NGM26", "month": "Jun"},
    {"commodity": "Rice",             "symbol": "ZRN26.CBT",  "base_symbol": "ZRN26", "month": "Jul"},
    {"commodity": "S&P 500 E-Mini",   "symbol": "ESM26.CME",  "base_symbol": "ESM26", "month": "Jun"},
    {"commodity": "Silver",           "symbol": "SIM26.CMX",  "base_symbol": "SIM26", "month": "Jun"},
    {"commodity": "Soybean Meal",     "symbol": "ZMN26.CBT",  "base_symbol": "ZMN26", "month": "Jul"},
    {"commodity": "Soybean Oil",      "symbol": "ZLN26.CBT",  "base_symbol": "ZLN26", "month": "Jul"},
    {"commodity": "Soybeans",         "symbol": "ZSN26.CBT",  "base_symbol": "ZSN26", "month": "Jul"},
    {"commodity": "Soybeans",         "symbol": "ZSX26.CBT",  "base_symbol": "ZSX26", "month": "Nov"},
    {"commodity": "US Dollar",        "symbol": "DXM26.NYB",  "base_symbol": "DXM26", "month": "Jun"},
    {"commodity": "Wheat",            "symbol": "ZWN26.CBT",  "base_symbol": "ZWN26", "month": "Jul"},
]

# ---------------------------------------------------------------------------
# Expired/rolled contracts  (history feeds still built, not shown on home page)
# ---------------------------------------------------------------------------

EXPIRED_CONTRACTS: list[Contract] = [
    {"commodity": "Cocoa",         "symbol": "CCK26.NYB",  "base_symbol": "CCK26", "month": "May"},
    {"commodity": "Coffee",        "symbol": "KCK26.NYB",  "base_symbol": "KCK26", "month": "May"},
    {"commodity": "Copper",        "symbol": "HGK26.CMX",  "base_symbol": "HGK26", "month": "May"},
    {"commodity": "Cotton",        "symbol": "CTK26.NYB",  "base_symbol": "CTK26", "month": "May"},
    {"commodity": "Gold",          "symbol": "GCJ26.CMX",  "base_symbol": "GCJ26", "month": "Apr"},
    {"commodity": "Live Cattle",   "symbol": "LEJ26.CME",  "base_symbol": "LEJ26", "month": "Apr"},
    {"commodity": "Feeder Cattle", "symbol": "GFK26.CME",  "base_symbol": "GFK26", "month": "May"},
]

# All contracts (active + expired) — full set for feed building
CONTRACTS: list[Contract] = ACTIVE_CONTRACTS + EXPIRED_CONTRACTS

# Lookup of base_symbol -> Contract for fast access
CONTRACT_BY_SYMBOL: dict[str, Contract] = {c["base_symbol"]: c for c in CONTRACTS}

# Home page display order (active contracts only)
HOME_ORDER: list[str] = [c["base_symbol"] for c in ACTIVE_CONTRACTS]


# ---------------------------------------------------------------------------
# Tick sizes (minimum price increment per commodity)
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
    "2026-01-01",  # New Year's Day
    "2026-01-19",  # MLK Day
    "2026-02-16",  # Presidents' Day
    "2026-04-03",  # Good Friday
    "2026-05-25",  # Memorial Day
    "2026-06-19",  # Juneteenth
    "2026-07-03",  # Independence Day (observed)
    "2026-09-07",  # Labor Day
    "2026-11-26",  # Thanksgiving
    "2026-12-25",  # Christmas
})


# ---------------------------------------------------------------------------
# Feed settings
# ---------------------------------------------------------------------------

YAHOO_RANGE: str = "6mo"
YAHOO_INTERVAL: str = "1d"
FETCH_WORKERS: int = 4
FETCH_DELAY: float = 0.25
FETCH_MAX_RETRIES: int = 3
