"""
Configuration for the commodity options dashboard.
Contracts list carried over from v1 ranges2, plus options/COT mappings.
"""
from typing import TypedDict


class Contract(TypedDict, total=False):
    commodity:    str
    symbol:       str
    base_symbol:  str
    month:        str
    roll_date:    str | None
    always_show:  bool
    drop_date:    str | None
    history_start: str | None


CONTRACTS: list[Contract] = [
    # Metals (COMEX) — all on Dec 2026 as of Sep/Oct rolls
    {"commodity": "Copper",         "symbol": "HGZ26.CMX", "base_symbol": "HGZ26", "month": "Dec",   "roll_date": "2026-09-27"},
    {"commodity": "Copper",         "symbol": "HGV26.CMX", "base_symbol": "HGV26", "month": "Oct",   "roll_date": "2026-07-29"},
    {"commodity": "Gold",           "symbol": "GCZ26.CMX", "base_symbol": "GCZ26", "month": "Dec",   "roll_date": "2026-09-08"},
    {"commodity": "Gold",           "symbol": "GCV26.CMX", "base_symbol": "GCV26", "month": "Oct",   "roll_date": "2026-07-29"},
    {"commodity": "Silver",         "symbol": "SIZ26.CMX", "base_symbol": "SIZ26", "month": "Dec",   "roll_date": "2026-09-27"},
    {"commodity": "Silver",         "symbol": "SIV26.CMX", "base_symbol": "SIV26", "month": "Oct",   "roll_date": "2026-07-29"},

    # Grains (CBOT)
    {"commodity": "Corn",           "symbol": "ZCZ26.CBT", "base_symbol": "ZCZ26", "month": "Dec",   "roll_date": None,         "always_show": True},
    {"commodity": "Corn",           "symbol": "ZCH27.CBT", "base_symbol": "ZCH27", "month": "Mar27", "roll_date": "2026-08-24"},
    {"commodity": "Corn",           "symbol": "ZCZ27.CBT", "base_symbol": "ZCZ27", "month": "Dec27", "roll_date": "2026-06-17", "always_show": True},
    {"commodity": "Hard Red Wheat", "symbol": "KEZ26.CBT", "base_symbol": "KEZ26", "month": "Dec",   "roll_date": "2026-08-20", "always_show": True},
    {"commodity": "Soybeans",       "symbol": "ZSH27.CBT", "base_symbol": "ZSH27", "month": "Mar27", "roll_date": "2026-08-24"},
    {"commodity": "Soybeans",       "symbol": "ZSX27.CBT", "base_symbol": "ZSX27", "month": "Nov27", "roll_date": "2026-06-17", "always_show": True},
    {"commodity": "Soybeans",       "symbol": "ZSX26.CBT", "base_symbol": "ZSX26", "month": "Nov",   "roll_date": None,         "always_show": True, "drop_date": "2026-10-25"},
    {"commodity": "Soybean Meal",   "symbol": "ZMZ26.CBT", "base_symbol": "ZMZ26", "month": "Dec",   "roll_date": "2026-08-20", "always_show": True},
    {"commodity": "Soybean Oil",    "symbol": "ZLZ26.CBT", "base_symbol": "ZLZ26", "month": "Dec",   "roll_date": "2026-08-20", "always_show": True},
    {"commodity": "Wheat",          "symbol": "ZWZ26.CBT", "base_symbol": "ZWZ26", "month": "Dec",   "roll_date": "2026-08-20", "always_show": True},

    # Livestock (CME)
    {"commodity": "Feeder Cattle",  "symbol": "GFF27.CME", "base_symbol": "GFF27", "month": "Jan27", "roll_date": "2026-10-22"},
    {"commodity": "Feeder Cattle",  "symbol": "GFV26.CME", "base_symbol": "GFV26", "month": "Oct",   "roll_date": "2026-08-14"},
    {"commodity": "Lean Hogs",      "symbol": "HEZ26.CME", "base_symbol": "HEZ26", "month": "Dec",   "roll_date": "2026-10-22"},
    {"commodity": "Lean Hogs",      "symbol": "HEV26.CME", "base_symbol": "HEV26", "month": "Oct",   "roll_date": "2026-07-29", "drop_date": "2026-10-22"},
    {"commodity": "Live Cattle",    "symbol": "LEZ26.CME", "base_symbol": "LEZ26", "month": "Dec",   "roll_date": "2026-10-01"},
    {"commodity": "Live Cattle",    "symbol": "LEV26.CME", "base_symbol": "LEV26", "month": "Oct",   "roll_date": "2026-07-29"},
]

# ETF proxy for IV estimation via yfinance options.
# Used when actual futures options aren't available through yfinance.
# IV from these ETFs closely approximates commodity futures ATM IV.
ETF_IV_PROXY: dict[str, str | None] = {
    "Gold":              "GLD",    # SPDR Gold Shares ≈ GC/10 scale
    "Silver":            "SLV",    # iShares Silver Trust ≈ SI scale
    "Crude Oil WTI":     "USO",    # US Oil Fund
    "Natural Gas":       "UNG",    # US Natural Gas Fund
    "Corn":              "CORN",   # Teucrium Corn (low liquidity)
    "Wheat":             "WEAT",   # Teucrium Wheat
    "Soybeans":          "SOYB",   # Teucrium Soybean (very low liquidity)
    # Others: no suitable liquid ETF proxy
}

# yfinance continuous ticker for options chain lookup per commodity.
# None = options not available via yfinance for this commodity.
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
    "Crude Oil WTI":     "CL=F",
    "Natural Gas":       "NG=F",
    "Unleaded Gasoline": "RB=F",
    "Heating Oil":       "HO=F",
    "S&P 500 E-Mini":    "ES=F",
    "Nasdaq 100 E-Mini": "NQ=F",
    "Live Cattle":       "LE=F",
    "Lean Hogs":         "HE=F",
    "Feeder Cattle":     "GF=F",
    "Coffee":            "KC=F",
    "Cocoa":             "CC=F",
    "Cotton":            "CT=F",
    "Sugar":             "SB=F",
    "10-Year T-Note":    "ZN=F",
    "US Dollar":         "UUP",   # Invesco DB US Dollar Index Bullish Fund (DX=F unavailable on Yahoo)
    "Rice":              None,
    "Bitcoin":           "BTC=F",
}

# Month offset applied when matching a yfinance expiration date to a contract month.
# Grains/softs/energy options expire the month BEFORE their delivery month.
# Metals/livestock/financials expire in the same calendar month as delivery.
EXPIRY_OFFSET: dict[str, int] = {
    "Corn":              -1,
    "Soybeans":          -1,
    "Wheat":             -1,
    "Hard Red Wheat":    -1,
    "Soybean Meal":      -1,
    "Soybean Oil":       -1,
    "Gold":               0,
    "Silver":             0,
    "Copper":             0,
    "Crude Oil WTI":     -1,
    "Natural Gas":       -1,
    "Unleaded Gasoline": -1,
    "Heating Oil":       -1,
    "S&P 500 E-Mini":     0,
    "Nasdaq 100 E-Mini":  0,
    "Live Cattle":        0,
    "Lean Hogs":          0,
    "Feeder Cattle":      0,
    "Coffee":            -1,
    "Cocoa":             -1,
    "Cotton":            -1,
    "Sugar":             -1,
    "10-Year T-Note":     0,
    "US Dollar":          0,
    "Rice":               0,
    "Bitcoin":            0,
}

# FMP API symbol for COT report lookup.
# Verify exact strings via GET /api/v4/commitment_of_traders_report_list before first run.
COT_SYMBOL_MAP: dict[str, str | None] = {
    "Corn":              "CORN",
    "Soybeans":          "SOYBEANS",
    "Wheat":             "WHEAT-SRW",
    "Hard Red Wheat":    "WHEAT-HRW",
    "Soybean Meal":      "SOYBEAN MEAL",
    "Soybean Oil":       "SOYBEAN OIL",
    "Gold":              "GOLD",
    "Silver":            "SILVER",
    "Copper":            "COPPER",
    "Crude Oil WTI":     "CRUDE OIL, LIGHT SWEET",
    "Natural Gas":       "NATURAL GAS",
    "Unleaded Gasoline": "GASOLINE BLENDSTOCK (RBOB)",
    "Heating Oil":       "NO. 2 HEATING OIL",
    "S&P 500 E-Mini":    "S&P 500 MINI",
    "Nasdaq 100 E-Mini": "NASDAQ MINI",
    "Live Cattle":       "LIVE CATTLE",
    "Lean Hogs":         "LEAN HOGS",
    "Feeder Cattle":     "FEEDER CATTLE",
    "Coffee":            "COFFEE C",
    "Cocoa":             "COCOA",
    "Cotton":            "COTTON NO. 2",
    "Sugar":             "SUGAR NO. 11",
    "10-Year T-Note":    "10-YEAR U.S. TREASURY NOTES",
    "US Dollar":         "U.S. DOLLAR INDEX",
    "Rice":              "ROUGH RICE",
    "Bitcoin":           "BITCOIN",
}

# Commodities whose yfinance spot price is in cents but CME strikes are in dollars.
# Multiply CME strikes by 100 to bring them into the same unit as spot.
CENTS_COMMODITIES: frozenset[str] = frozenset({
    "Corn", "Soybeans", "Wheat", "Hard Red Wheat",
    "Soybean Meal", "Soybean Oil",
    "Copper", "Live Cattle", "Lean Hogs", "Feeder Cattle",
})

# CME month letter → calendar month number
MONTH_LETTER_TO_NUM: dict[str, int] = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

# Dashboard display order
COMMODITY_DISPLAY_ORDER: list[str] = [
    # Metals (COMEX)
    "Copper", "Gold", "Silver",
    # Grains (CBOT)
    "Corn", "Hard Red Wheat", "Soybeans", "Soybean Meal", "Soybean Oil", "Wheat",
    # Livestock (CME)
    "Feeder Cattle", "Lean Hogs", "Live Cattle",
]

# Constants
RISK_FREE_RATE:    float = 0.05
OI_HISTORY_WINDOW: int   = 15   # trading days of OI snapshots to keep
OI_LOOKBACK:       int   = 10   # trading days back for OI change comparison
OI_MIN_CHANGE:     int   = 100  # minimum OI delta to include in gainers/losers

CME_HOLIDAYS: frozenset[str] = frozenset({
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
})


def is_trading_day(date_str: str) -> bool:
    import datetime
    d = datetime.date.fromisoformat(date_str)
    return d.weekday() < 5 and date_str not in CME_HOLIDAYS


def trading_day_offset(date_str: str, n: int) -> str:
    """Return the date n trading days before date_str."""
    import datetime
    d = datetime.date.fromisoformat(date_str)
    count = 0
    while count < n:
        d -= datetime.timedelta(days=1)
        if is_trading_day(d.isoformat()):
            count += 1
    return d.isoformat()


def get_active_contracts(date_str: str) -> list[Contract]:
    """Return all contracts that are not yet dropped as of date_str."""
    result = []
    seen: set[str] = set()
    for c in CONTRACTS:
        sym = c["base_symbol"]
        if sym in seen:
            continue
        dd = c.get("drop_date")
        if dd and dd <= date_str:
            continue
        seen.add(sym)
        result.append(c)
    return result


def active_symbols_for_date(date_str: str) -> list[str]:
    """Return base_symbols of contracts rolled to but not yet dropped as of date_str."""
    result = []
    seen: set[str] = set()
    for c in CONTRACTS:
        sym = c["base_symbol"]
        if sym in seen:
            continue
        rd = c.get("roll_date")
        if rd and rd > date_str:
            continue
        dd = c.get("drop_date")
        if dd and dd <= date_str:
            continue
        seen.add(sym)
        result.append(sym)
    return result
