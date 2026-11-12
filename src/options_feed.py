"""
Options feed builder for 12 CME physical commodities.

Writes feeds/options-feed.json with per-commodity data:
  - Per active contract: DTE, ATM IV, expected move, top-5 OI strikes, OI gainers/losers
  - Per commodity: CFTC COT report (commercial / non-commercial / non-reportable)

Maintains rolling OI history in feeds/oi_history/<SYMBOL>.json (15 trading days).
Call via build_feeds.py:
    from src.options_feed import build_options_feed
    result = build_options_feed(today_str, feeds_dir=FEEDS_DIR)
"""
from __future__ import annotations

import calendar
import datetime
import json
import logging
import math
import time
import urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

RISK_FREE_RATE: float = 0.05


# ---------------------------------------------------------------------------
# Black-76 IV solver (bisection, no external dependency beyond math)
# ---------------------------------------------------------------------------

def _count_trading_days(start: datetime.date, end: datetime.date) -> int:
    """Count weekday trading days from start to end inclusive."""
    if end < start:
        return 0
    count = 0
    d = start
    while d <= end:
        if d.weekday() < 5:
            count += 1
        d += datetime.timedelta(days=1)
    return count


def _grain_options_expiry(del_month: int, del_year: int) -> datetime.date:
    """
    Grains: last Friday >= 5 business days before first notice day
    (= last business day of month prior to delivery).
    """
    fnd_m, fnd_y = del_month - 1, del_year
    if fnd_m == 0:
        fnd_m, fnd_y = 12, del_year - 1
    last_day = calendar.monthrange(fnd_y, fnd_m)[1]
    fnd = datetime.date(fnd_y, fnd_m, last_day)
    while fnd.weekday() >= 5:
        fnd -= datetime.timedelta(days=1)

    count, d = 0, fnd
    while count < 5:
        d -= datetime.timedelta(days=1)
        if d.weekday() < 5:
            count += 1

    while d.weekday() != 4:
        d -= datetime.timedelta(days=1)
    return d


def _norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2))


def _black76_price(F: float, K: float, r: float, T: float, sigma: float, is_call: bool) -> float:
    if T <= 0 or sigma <= 0:
        intrinsic = max(0.0, (F - K) if is_call else (K - F))
        return intrinsic * math.exp(-r * T)
    sqrtT = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    df = math.exp(-r * T)
    if is_call:
        return df * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
    return df * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))


def _black76_iv(price: float, F: float, K: float, r: float, T: float, is_call: bool) -> float | None:
    if T <= 0 or price <= 0 or F <= 0 or K <= 0:
        return None
    lo, hi = 1e-5, 5.0
    for _ in range(120):
        mid = (lo + hi) / 2.0
        p   = _black76_price(F, K, r, T, mid, is_call)
        if abs(p - price) < 1e-7:
            break
        if p < price:
            lo = mid
        else:
            hi = mid
    result = (lo + hi) / 2.0
    return result if 1e-4 < result < 4.99 else None

# ---------------------------------------------------------------------------
# Commodity config
# ---------------------------------------------------------------------------

CME_COMMODITIES: frozenset[str] = frozenset({
    "Copper", "Gold", "Silver",
    "Corn", "Hard Red Wheat", "Soybeans", "Soybean Meal", "Soybean Oil", "Wheat",
    "Feeder Cattle", "Lean Hogs", "Live Cattle",
})

DISPLAY_ORDER: list[str] = [
    "Copper", "Gold", "Silver",
    "Corn", "Hard Red Wheat", "Soybeans", "Soybean Meal", "Soybean Oil", "Wheat",
    "Feeder Cattle", "Lean Hogs", "Live Cattle",
]

# Commodities whose prices are quoted in cents (not dollars)
CENTS_COMMODITIES: frozenset[str] = frozenset([
    "Corn", "Hard Red Wheat", "Soybeans", "Soybean Oil", "Wheat",
    "Copper", "Lean Hogs", "Live Cattle", "Feeder Cattle",
])

def _fmt_move(val: float, commodity: str) -> str:
    """Format expected move in native commodity units."""
    if commodity in CENTS_COMMODITIES:
        return f"{val:.1f}¢"   # e.g. "25.3¢"
    # dollars: no decimals if large, two if small
    if val >= 10:
        return f"${val:.0f}"
    return f"${val:.2f}"

# ETF proxies for IV estimation via yfinance options chains
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

# Continuous futures ticker for spot price
SPOT_TICKER: dict[str, str] = {
    "Gold":          "GC=F",
    "Silver":        "SI=F",
    "Copper":        "HG=F",
    "Corn":          "ZC=F",
    "Wheat":         "ZW=F",
    "Hard Red Wheat":"KE=F",
    "Soybeans":      "ZS=F",
    "Soybean Meal":  "ZM=F",
    "Soybean Oil":   "ZL=F",
    "Live Cattle":   "LE=F",
    "Lean Hogs":     "HE=F",
    "Feeder Cattle": "GF=F",
}

# Options expiry offset: months before delivery month that options expire
EXPIRY_OFFSET: dict[str, int] = {
    "Corn": -1, "Soybeans": -1, "Wheat": -1, "Hard Red Wheat": -1,
    "Soybean Meal": -1, "Soybean Oil": -1,
    "Gold": 0, "Silver": 0, "Copper": 0,
    "Live Cattle": 0, "Lean Hogs": 0, "Feeder Cattle": 0,
}

# CFTC Socrata legacy COT market_and_exchange_names strings
CFTC_MARKET: dict[str, str] = {
    "Gold":          "GOLD - COMMODITY EXCHANGE INC.",
    "Silver":        "SILVER - COMMODITY EXCHANGE INC.",
    "Copper":        "COPPER- #1 - COMMODITY EXCHANGE INC.",
    "Corn":          "CORN - CHICAGO BOARD OF TRADE",
    "Wheat":         "WHEAT-SRW - CHICAGO BOARD OF TRADE",
    "Hard Red Wheat":"WHEAT-HRW - CHICAGO BOARD OF TRADE",
    "Soybeans":      "SOYBEANS - CHICAGO BOARD OF TRADE",
    "Soybean Meal":  "SOYBEAN MEAL - CHICAGO BOARD OF TRADE",
    "Soybean Oil":   "SOYBEAN OIL - CHICAGO BOARD OF TRADE",
    "Live Cattle":   "LIVE CATTLE - CHICAGO MERCANTILE EXCHANGE",
    "Lean Hogs":     "LEAN HOGS - CHICAGO MERCANTILE EXCHANGE",
    "Feeder Cattle": "FEEDER CATTLE - CHICAGO MERCANTILE EXCHANGE",
}

MONTH_LETTER: dict[str, int] = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

OI_HISTORY_WINDOW: int = 15   # trading days of OI snapshots to retain
OI_MIN_CHANGE: int     = 100  # minimum OI delta to include in gainers/losers

# ---------------------------------------------------------------------------
# CME product IDs
# ---------------------------------------------------------------------------

CME_PRODUCT_MAP: dict[str, int] = {
    "Corn":          300,
    "Soybean Meal":  310,
    "Soybean Oil":   312,
    "Soybeans":      320,
    "Wheat":         323,
    "Hard Red Wheat":348,
    "Gold":          437,
    "Copper":        438,
    "Silver":        458,
    "Lean Hogs":     19,
    "Live Cattle":   22,
    "Feeder Cattle": 34,
}

_CME_BASE   = "https://www.cmegroup.com/CmeWS/mvc/Settlements/Options"
_EXP_URL    = _CME_BASE + "/TradeDateAndExpirations/{fetch_id}"
_SETTLE_URL = (
    _CME_BASE + "/Settlements/{opt_id}/OOF"
    "?strategy=DEFAULT&optionProductId={opt_id}"
    "&monthYear={month_year}&optionExpiration={opt_id}-{exp_code}"
    "&tradeDate={trade_date}&pageSize=500"
)
_WARMUP_URL = "https://www.cmegroup.com/markets/agriculture/grains/corn.html"


# ---------------------------------------------------------------------------
# CME session (uses curl_cffi Chrome TLS impersonation; falls back to requests)
# ---------------------------------------------------------------------------

class _CMESession:
    def __init__(self):
        self._sess = None

    def _ensure(self):
        if self._sess is not None:
            return
        try:
            from curl_cffi import requests as cfr
            self._sess = cfr.Session(impersonate="chrome124")
            try:
                self._sess.get(_WARMUP_URL, timeout=20)
            except Exception:
                pass
        except ImportError:
            import requests as _req
            self._sess = _req.Session()
            log.warning("curl_cffi not installed — CME OI may be blocked by Akamai")

    def get(self, url: str, **kw):
        self._ensure()
        return self._sess.get(url, **kw)


_cme = _CMESession()


# ---------------------------------------------------------------------------
# CME OI fetching
# ---------------------------------------------------------------------------

def _fetch_expirations(fetch_id: int) -> list[dict]:
    url = _EXP_URL.format(fetch_id=fetch_id)
    try:
        r = _cme.get(url, timeout=15)
    except Exception as e:
        log.debug("CME expirations fetch_id=%s: %s", fetch_id, e)
        return []
    if r.status_code != 200:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    if not data or not isinstance(data, list):
        return []

    all_exp: list[dict] = []
    for entry in data:
        for exp in entry.get("expirations", []):
            info   = exp.get("expiration", {})
            code   = info.get("code")
            month  = info.get("month")
            year   = info.get("year")
            opt_id = exp.get("productId")
            if code and month and year and opt_id:
                all_exp.append({"code": code, "month": month, "year": year, "productId": opt_id})
    if not all_exp:
        return []

    # Keep only the primary (most-covered) options series
    id_counts  = Counter(e["productId"] for e in all_exp)
    max_cnt    = max(id_counts.values())
    primary_id = sorted(pid for pid, cnt in id_counts.items() if cnt == max_cnt)[0]
    return [e for e in all_exp if e["productId"] == primary_id]


def _match_exp(expirations: list[dict], target_month: int, target_year: int) -> dict | None:
    def dist(e):
        dm = (e["year"] - target_year) * 12 + (e["month"] - target_month)
        return abs(dm) if abs(dm) <= 2 else 999
    cands = [e for e in expirations if dist(e) < 999]
    return min(cands, key=dist) if cands else None


def _fetch_settlements(opt_id: int, exp_code: str, month_year: str, trade_date: str) -> list[dict]:
    url = _SETTLE_URL.format(opt_id=opt_id, month_year=month_year,
                              exp_code=exp_code, trade_date=trade_date)
    try:
        r = _cme.get(url, timeout=20)
    except Exception as e:
        log.debug("CME settlements: %s", e)
        return []
    if r.status_code != 200:
        return []
    try:
        return r.json().get("settlements", [])
    except Exception:
        return []


def fetch_cme_oi(commodity: str, del_month: int, del_year: int) -> tuple[list[dict], list[dict], str | None]:
    """Return (calls, puts, exp_date_str). Each record: {strike, openInterest}."""
    fetch_id = CME_PRODUCT_MAP.get(commodity)
    if fetch_id is None:
        return [], [], None
    exps = _fetch_expirations(fetch_id)
    if not exps:
        return [], [], None
    matched = _match_exp(exps, del_month, del_year)
    if not matched:
        return [], [], None

    opt_id     = matched["productId"]
    exp_code   = matched["code"]
    month_year = f"{exp_code[0]}{str(matched['year'])[-2:]}"
    today_s    = datetime.date.today().strftime("%m/%d/%Y")
    settlements = _fetch_settlements(opt_id, exp_code, month_year, today_s)
    if not settlements:
        yest = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%m/%d/%Y")
        settlements = _fetch_settlements(opt_id, exp_code, month_year, yest)
    if not settlements:
        return [], [], None

    calls: list[dict] = []
    puts:  list[dict] = []
    for row in settlements:
        try:
            strike = float(row.get("strike", ""))
        except (ValueError, TypeError):
            continue
        try:
            oi = int(str(row.get("openInterest", "0")).replace(",", ""))
        except (ValueError, TypeError):
            oi = 0
        if oi == 0:
            continue
        try:
            settle = float(str(row.get("settle", "0")).replace(",", ""))
        except (ValueError, TypeError):
            settle = 0.0
        t   = row.get("type", "").lower()
        rec = {"strike": strike, "openInterest": oi, "settle": settle}
        if t == "call":
            calls.append(rec)
        elif t == "put":
            puts.append(rec)

    last  = calendar.monthrange(matched["year"], matched["month"])[1]
    exp_d = datetime.date(matched["year"], matched["month"], last)
    while exp_d.weekday() != 4:
        exp_d -= datetime.timedelta(days=1)
    return calls, puts, exp_d.isoformat()


# ---------------------------------------------------------------------------
# IV from yfinance ETF proxy
# ---------------------------------------------------------------------------

def fetch_iv_etf(commodity: str, dte: int) -> dict:
    proxy = ETF_IV_PROXY.get(commodity)
    if not proxy:
        return {}
    try:
        import yfinance as yf
        t    = yf.Ticker(proxy)
        exps = t.options
        if not exps:
            return {}
        today = datetime.date.today()
        best_exp, best_gap = None, 9999
        for exp in exps:
            gap = (datetime.date.fromisoformat(exp) - today).days
            if gap >= 0 and abs(gap - dte) < best_gap:
                best_gap = abs(gap - dte)
                best_exp = exp
        if not best_exp:
            return {}
        chain = t.option_chain(best_exp)
        hist  = t.history(period="2d")
        if hist.empty:
            return {}
        spot  = float(hist["Close"].iloc[-1])
        calls = chain.calls
        if calls.empty:
            return {}
        closest  = float(min(calls["strike"].values, key=lambda k: abs(k - spot)))
        call_row = calls[calls["strike"] == closest]
        put_row  = chain.puts[chain.puts["strike"] == closest]
        call_iv  = float(call_row["impliedVolatility"].values[0]) if not call_row.empty else None
        put_iv   = float(put_row["impliedVolatility"].values[0])  if not put_row.empty  else None
        valid = [v for v in [call_iv, put_iv] if v and v > 0]
        if not valid:
            return {}
        return {"atm_iv": sum(valid) / len(valid), "proxy_ticker": proxy}
    except Exception as e:
        log.debug("ETF IV proxy %s: %s", commodity, e)
        return {}


# ---------------------------------------------------------------------------
# Spot price from yfinance continuous ticker
# ---------------------------------------------------------------------------

def fetch_spot(commodity: str) -> float | None:
    ticker = SPOT_TICKER.get(commodity)
    if not ticker:
        return None
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period="2d")
        return float(hist["Close"].iloc[-1]) if not hist.empty else None
    except Exception as e:
        log.debug("Spot price %s: %s", commodity, e)
        return None


# ---------------------------------------------------------------------------
# CFTC COT (public Socrata API — no key required)
# ---------------------------------------------------------------------------

def fetch_cot(commodity: str) -> dict | None:
    market = CFTC_MARKET.get(commodity)
    if not market:
        return None
    import requests
    url = (
        "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
        f"?market_and_exchange_names={urllib.parse.quote(market)}"
        "&$limit=2&$order=report_date_as_yyyy_mm_dd%20DESC"
    )
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        rows = r.json()
        if not rows:
            return None
    except Exception as e:
        log.warning("COT fetch %s: %s", commodity, e)
        return None

    cur  = rows[0]
    prev = rows[1] if len(rows) > 1 else None

    def intf(row, field):
        try:
            return int(str(row.get(field, 0)).replace(",", ""))
        except (ValueError, TypeError):
            return 0

    def wow(fl, fs):
        if not prev:
            return 0
        return (intf(cur, fl) - intf(cur, fs)) - (intf(prev, fl) - intf(prev, fs))

    c_long  = intf(cur, "comm_positions_long_all")
    c_short = intf(cur, "comm_positions_short_all")
    nc_long = intf(cur, "noncomm_positions_long_all")
    nc_short= intf(cur, "noncomm_positions_short_all")
    nr_long = intf(cur, "nonrept_positions_long_all")
    nr_short= intf(cur, "nonrept_positions_short_all")

    return {
        "reportDate":    cur.get("report_date_as_yyyy_mm_dd", "")[:10],
        "commercial":    {"long": c_long,  "short": c_short,  "net": c_long  - c_short,  "netChangeWow": wow("comm_positions_long_all",   "comm_positions_short_all")},
        "nonCommercial": {"long": nc_long, "short": nc_short, "net": nc_long - nc_short, "netChangeWow": wow("noncomm_positions_long_all", "noncomm_positions_short_all")},
        "nonReportable": {"long": nr_long, "short": nr_short, "net": nr_long - nr_short, "netChangeWow": wow("nonrept_positions_long_all", "nonrept_positions_short_all")},
        "dataAvailable": True,
    }


# ---------------------------------------------------------------------------
# OI history helpers
# ---------------------------------------------------------------------------

def _load_oi_history(symbol: str, oi_dir: Path) -> dict:
    p = oi_dir / f"{symbol}.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_oi_history(symbol: str, history: dict, oi_dir: Path, today: str, cme_hols: frozenset):
    def is_td(d: str) -> bool:
        try:
            dt = datetime.date.fromisoformat(d)
            return dt.weekday() < 5 and d not in cme_hols
        except Exception:
            return False

    tdays = sorted(d for d in history if is_td(d))
    while len(tdays) > OI_HISTORY_WINDOW:
        del history[tdays[0]]
        tdays = tdays[1:]
    oi_dir.mkdir(parents=True, exist_ok=True)
    (oi_dir / f"{symbol}.json").write_text(json.dumps(history, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# OI gainers / losers
# ---------------------------------------------------------------------------

def _oi_deltas(
    today_snap: list[dict],
    oi_history: dict,
    today: str,
) -> tuple[list[dict], list[dict], str | None, bool]:
    if not oi_history or not today_snap:
        return [], [], None, False
    available    = sorted(oi_history.keys())
    compare_date = next((d for d in available if d < today), None)
    if not compare_date:
        return [], [], None, False

    prior = {e["s"]: e for e in oi_history[compare_date]}
    cur   = {e["s"]: e for e in today_snap}
    deltas: list[dict] = []
    for s in set(cur) | set(prior):
        c = cur.get(s,   {"c": 0, "p": 0})
        p = prior.get(s, {"c": 0, "p": 0})
        for side, key in (("call", "c"), ("put", "p")):
            delta = (c.get(key, 0) or 0) - (p.get(key, 0) or 0)
            if abs(delta) >= OI_MIN_CHANGE:
                deltas.append({
                    "strike":      s,
                    "type":        side,
                    "oiChange":    delta,
                    "oiPctChange": round(delta / max(p.get(key, 0) or 1, 1) * 100, 1),
                    "currentOI":   c.get(key, 0) or 0,
                    "priorOI":     p.get(key, 0) or 0,
                    "priorDate":   compare_date,
                })
    deltas.sort(key=lambda x: x["oiChange"], reverse=True)
    gainers = deltas[:5]
    losers  = [x for x in reversed(deltas) if x["oiChange"] < 0][:5]
    return gainers, losers, compare_date, True


# ---------------------------------------------------------------------------
# Per-contract data fetch
# ---------------------------------------------------------------------------

def _build_contract(
    commodity:  str,
    base_symbol: str,
    month_label: str,
    spot:        float | None,
    oi_history:  dict,
    today:       str,
) -> dict:
    clean = base_symbol.split(".")[0]
    if len(clean) < 4 or "=" in clean:
        return {"symbol": base_symbol, "month": month_label, "dataAvailable": False}

    yr_str = clean[-2:]
    ml     = clean[-3]
    if not yr_str.isdigit() or ml not in MONTH_LETTER:
        return {"symbol": base_symbol, "month": month_label, "dataAvailable": False}

    del_month = MONTH_LETTER[ml]
    del_year  = 2000 + int(yr_str)
    offset    = EXPIRY_OFFSET.get(commodity, 0)
    tgt_m, tgt_y = del_month + offset, del_year
    while tgt_m <= 0:  tgt_m += 12; tgt_y -= 1
    while tgt_m > 12:  tgt_m -= 12; tgt_y += 1

    # Options expiration date
    if offset == -1:
        # Grains: last Friday >= 5 business days before first notice day
        exp_d = _grain_options_expiry(del_month, del_year)
    else:
        # Metals/livestock: last Friday of delivery month
        last  = calendar.monthrange(del_year, del_month)[1]
        exp_d = datetime.date(del_year, del_month, last)
        while exp_d.weekday() != 4:
            exp_d -= datetime.timedelta(days=1)
    exp_str = exp_d.isoformat()
    dte     = _count_trading_days(datetime.date.today(), exp_d)

    # CME OI — pass delivery month so API matches correctly; ignore returned exp date
    calls, puts, _cme_exp = fetch_cme_oi(commodity, del_month, del_year)

    # ATM strike
    atm_strike = None
    if spot and calls:
        atm_strike = float(min((r["strike"] for r in calls), key=lambda k: abs(k - spot)))
    elif spot:
        magnitude  = 10 ** math.floor(math.log10(max(spot, 1)))
        tick       = magnitude / 10
        atm_strike = round(spot / tick) * tick

    # IV — try Black-76 from CME settlement prices first, fall back to ETF proxy
    atm_iv   = None
    iv_source = None
    if spot and atm_strike and dte > 0 and (calls or puts):
        T  = dte / 365.0
        F  = spot      # near-month futures price ≈ continuous spot
        K  = atm_strike
        atm_calls = [r for r in calls if r["strike"] == K]
        atm_puts  = [r for r in puts  if r["strike"] == K]
        ivs: list[float] = []
        if atm_calls and atm_calls[0].get("settle", 0) > 0:
            iv = _black76_iv(atm_calls[0]["settle"], F, K, RISK_FREE_RATE, T, is_call=True)
            if iv:
                ivs.append(iv)
        if atm_puts and atm_puts[0].get("settle", 0) > 0:
            iv = _black76_iv(atm_puts[0]["settle"], F, K, RISK_FREE_RATE, T, is_call=False)
            if iv:
                ivs.append(iv)
        if ivs:
            atm_iv    = sum(ivs) / len(ivs)
            iv_source = "CME (Black-76)"

    if not atm_iv:
        iv_data   = fetch_iv_etf(commodity, dte)
        atm_iv    = iv_data.get("atm_iv")
        iv_source = f"ETF ({iv_data.get('proxy_ticker', '?')})" if atm_iv else None

    # Expected move
    move_price = move_pct = None
    if spot and atm_iv and dte > 0:
        T          = dte / 365.0
        move_pct   = round(atm_iv * math.sqrt(T), 6)
        move_price = round(spot * move_pct, 4)

    # Top 5 OI strikes
    oi_map: dict[float, dict] = {}
    for r in calls:
        oi_map.setdefault(r["strike"], {"strike": r["strike"], "callOI": 0, "putOI": 0})["callOI"] = r["openInterest"]
    for r in puts:
        oi_map.setdefault(r["strike"], {"strike": r["strike"], "callOI": 0, "putOI": 0})["putOI"]  = r["openInterest"]
    for e in oi_map.values():
        e["totalOI"] = e["callOI"] + e["putOI"]
    top_oi = sorted(oi_map.values(), key=lambda x: x["totalOI"], reverse=True)[:5]

    # OI snapshot for history
    snapshot = [
        {"s": s, "c": e["callOI"], "p": e["putOI"]}
        for s, e in oi_map.items()
        if e["callOI"] + e["putOI"] > 0
    ]

    # OI gainers / losers
    gainers, losers, compare_date, oi_hist_avail = _oi_deltas(snapshot, oi_history, today)

    return {
        "symbol":            base_symbol,
        "month":             month_label,
        "optionsExpiration": exp_str,
        "dte":               dte,
        "underlyingPrice":   round(spot, 4) if spot else None,
        "atmStrike":         atm_strike,
        "atmIV":             round(atm_iv, 6) if atm_iv else None,
        "atmIVPct":          f"{atm_iv * 100:.1f}%" if atm_iv else None,
        "ivSource":          iv_source,
        "expectedMovePrice": move_price,
        "expectedMoveFmt":   _fmt_move(move_price, commodity) if move_price else None,
        "expectedMovePct":   f"{move_pct * 100:.2f}%" if move_pct else None,
        "expectedMoveRange": {
            "low":  round(spot - move_price, 4),
            "high": round(spot + move_price, 4),
        } if spot and move_price else None,
        "topOIStrikes":      top_oi,
        "oiGainers":         gainers,
        "oiLosers":          losers,
        "oiCompareDate":     compare_date,
        "oiHistoryAvailable":oi_hist_avail,
        "todaySnapshot":     snapshot,
        "dataAvailable":     True,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_options_feed(today: str, feeds_dir: Path, workers: int = 3) -> dict:
    """
    Build the full options feed for all 12 CME commodities.
    Returns a dict suitable for JSON serialisation as options-feed.json.
    """
    from src.config import CONTRACTS, CME_HOLIDAYS, active_symbols_for_date

    oi_dir = feeds_dir / "oi_history"
    oi_dir.mkdir(parents=True, exist_ok=True)

    # Active contracts for the 12 CME commodities (uses same roll logic as home page)
    active_syms = set(active_symbols_for_date(today))
    cme_contracts: dict[str, list[dict]] = {c: [] for c in DISPLAY_ORDER}
    for c in CONTRACTS:
        comm = c["commodity"]
        if comm not in CME_COMMODITIES:
            continue
        if c["base_symbol"] not in active_syms:
            continue
        cme_contracts[comm].append(c)

    # Spot prices (one request per commodity, shared across contracts)
    log.info("Fetching spot prices…")
    spot_map: dict[str, float | None] = {}
    for comm in DISPLAY_ORDER:
        spot_map[comm] = fetch_spot(comm)
        time.sleep(0.1)

    # Per-contract options data (parallel)
    log.info("Fetching options OI and IV…")
    tasks = [(comm, c) for comm in DISPLAY_ORDER for c in cme_contracts[comm]]
    results: dict[str, list[dict]] = {c: [] for c in DISPLAY_ORDER}

    def _process(comm: str, contract: dict) -> tuple[str, dict]:
        sym        = contract["base_symbol"]
        oi_history = _load_oi_history(sym, oi_dir)
        data       = _build_contract(comm, sym, contract["month"], spot_map.get(comm), oi_history, today)
        snapshot   = data.get("todaySnapshot", [])
        if snapshot:
            oi_history[today] = snapshot
            _save_oi_history(sym, oi_history, oi_dir, today, CME_HOLIDAYS)
        log.info("  ✓ %s %s  DTE=%s  OI=%s", comm, sym, data.get("dte"), "ok" if data.get("topOIStrikes") else "—")
        return comm, data

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process, comm, c): (comm, c) for comm, c in tasks}
        for fut in as_completed(futures):
            try:
                comm, data = fut.result()
                results[comm].append(data)
            except Exception as exc:
                comm, c = futures[fut]
                log.warning("Contract failed %s/%s: %s", comm, c["base_symbol"], exc)

    for comm in DISPLAY_ORDER:
        results[comm].sort(key=lambda x: x.get("dte", 9999))

    # COT (serial — avoid rate-limiting CFTC public API)
    log.info("Fetching COT reports…")
    cot: dict[str, Any] = {}
    for comm in DISPLAY_ORDER:
        time.sleep(0.3)
        cot_data = fetch_cot(comm)
        cot[comm] = cot_data if cot_data else {"dataAvailable": False}
        log.info("  COT %-20s %s", comm, cot[comm].get("reportDate", "n/a"))

    # Assemble final feed
    return {
        "builtAt":    datetime.datetime.utcnow().isoformat() + "Z",
        "commodities": {
            comm: {"contracts": results[comm], "cot": cot[comm]}
            for comm in DISPLAY_ORDER
        },
    }
