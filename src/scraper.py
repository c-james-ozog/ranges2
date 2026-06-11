"""
Yahoo Finance data fetcher for ranges2.

Provides fetch_yahoo_history() with automatic retry/backoff,
plus tick-aware formatting helpers used throughout the feed builder.
"""

import json
import logging
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.config import FETCH_DELAY, FETCH_MAX_RETRIES, YAHOO_INTERVAL, YAHOO_RANGE

log = logging.getLogger(__name__)


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
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
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

    last_error: Exception | None = None
    rows = _fetch_url(url, symbol)

    # Detect flat data: if >50% of rows have high == low, data is bad
    # Fall back to hourly data aggregated to daily
    if rows and sum(1 for r in rows if r["high"] == r["low"]) > len(rows) * 0.5:
        log.warning("%s: flat OHLC detected — falling back to hourly aggregation", symbol)
        hourly_url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            f"?range=2mo&interval=1h&includePrePost=false"
        )
        try:
            hourly_rows = _fetch_url(hourly_url, symbol)
            aggregated = _aggregate_hourly_to_daily(hourly_rows)
            if aggregated:
                return aggregated
        except Exception as e:
            log.warning("%s: hourly fallback failed: %s", symbol, e)

    return rows


def _fetch_url(url: str, symbol: str) -> list[RawRow]:
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
    """Aggregate hourly bars into daily OHLC bars."""
    from collections import defaultdict
    from datetime import datetime, timezone, timedelta

    daily: dict[str, dict] = defaultdict(lambda: {"high": None, "low": None, "close": None, "timestamp": None})
    for row in rows:
        dt = datetime.fromtimestamp(row["timestamp"], tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d")
        d = daily[date_str]
        d["high"]      = row["high"] if d["high"] is None else max(d["high"], row["high"])
        d["low"]       = row["low"]  if d["low"]  is None else min(d["low"],  row["low"])
        d["close"]     = row["close"]
        d["timestamp"] = row["timestamp"]

    result = []
    for date_str, d in daily.items():
        if d["high"] is None or d["high"] == d["low"]:
            continue
        result.append({
            "timestamp": d["timestamp"],
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
