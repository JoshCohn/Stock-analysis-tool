"""
Stock Analyst Tool - Backend API
Tracks analyst recommendations, success rates, and YoY stock performance.
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np
from curl_cffi import requests as crequests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import time
import logging
import math
import os
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Analyst Tool API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# curl_cffi session — yfinance 1.x requires this for anti-bot headers
# ---------------------------------------------------------------------------
_yf_session = crequests.Session(impersonate="chrome")

# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------
_cache: Dict[str, Any] = {}
_cache_ts: Dict[str, float] = {}


def cache_get(key: str, max_age: int = 3600) -> Optional[Any]:
    if key in _cache and (time.time() - _cache_ts.get(key, 0)) < max_age:
        return _cache[key]
    return None


def cache_set(key: str, data: Any) -> None:
    _cache[key] = data
    _cache_ts[key] = time.time()


def cache_age(key: str) -> Optional[float]:
    return (time.time() - _cache_ts[key]) if key in _cache_ts else None


# ---------------------------------------------------------------------------
# Module-level stores for price histories and upgrades/downgrades
# ---------------------------------------------------------------------------
_price_histories: Dict[str, pd.Series] = {}
_ud_store: Dict[str, pd.DataFrame] = {}

# ---------------------------------------------------------------------------
# Parallelism — semaphore caps concurrent Yahoo Finance requests
# ---------------------------------------------------------------------------
_fetch_semaphore = threading.Semaphore(6)   # max 6 concurrent analyst calls
_ticker_locks: Dict[str, threading.Lock] = {}
_ticker_locks_lock = threading.Lock()

# Background fetch tracking
_bg_lock = threading.Lock()
_bg_threads: Dict[str, threading.Thread] = {}

# Loading progress: cache_key -> {"done": int, "total": int, "status": str}
_loading_progress: Dict[str, Dict] = {}

def _get_ticker_lock(sym: str) -> threading.Lock:
    with _ticker_locks_lock:
        if sym not in _ticker_locks:
            _ticker_locks[sym] = threading.Lock()
        return _ticker_locks[sym]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SECTORS = [
    "Communication Services", "Consumer Discretionary", "Consumer Staples",
    "Energy", "Financials", "Health Care", "Industrials",
    "Information Technology", "Materials", "Real Estate", "Utilities",
]

# Per-thread politeness delay inside the semaphore (seconds)
_ANALYST_DELAY = 0.1

# Bullish/bearish grade keyword sets for firm scoring
_BULLISH_KEYWORDS = {"buy", "strong buy", "outperform", "overweight", "accumulate", "positive", "market outperform", "add"}
_BEARISH_KEYWORDS = {"sell", "strong sell", "underperform", "underweight", "reduce", "negative", "market underperform", "avoid"}

# ---------------------------------------------------------------------------
# Retry-aware yfinance ticker info fetch
# ---------------------------------------------------------------------------

def _ticker_info(sym: str, retries: int = 4) -> Dict:
    """Fetch ticker.info with exponential back-off on 429 / empty responses."""
    for attempt in range(retries):
        try:
            t = yf.Ticker(sym, session=_yf_session)
            info = t.info
            # yfinance sometimes returns a near-empty dict with just {quoteType,…}
            if info and len(info) > 5:
                return info
            # Small dict → likely a parse failure; wait and retry
            wait = 2 ** attempt + random.uniform(0, 1)
            logger.debug(f"{sym}: thin info dict (attempt {attempt+1}), waiting {wait:.1f}s")
            time.sleep(wait)
        except Exception as exc:
            wait = 2 ** attempt + random.uniform(0, 1)
            logger.debug(f"{sym}: info error on attempt {attempt+1}: {exc}; waiting {wait:.1f}s")
            time.sleep(wait)
    return {}


def _ticker_upgrades_downgrades(sym: str) -> Optional[pd.DataFrame]:
    """Fetch per-firm upgrades/downgrades history (yfinance 1.x API)."""
    try:
        t = yf.Ticker(sym, session=_yf_session)
        ud = t.upgrades_downgrades
        return ud if ud is not None and not ud.empty else None
    except Exception:
        return None


def _ticker_rec_summary(sym: str) -> Optional[pd.DataFrame]:
    """Fetch aggregate buy/hold/sell counts (yfinance 1.x recommendations)."""
    try:
        t = yf.Ticker(sym, session=_yf_session)
        recs = t.recommendations
        return recs if recs is not None and not recs.empty else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Extended universe: S&P 500 + S&P 400 MidCap + NASDAQ-100
# ---------------------------------------------------------------------------

def get_extended_universe() -> pd.DataFrame:
    """
    Fetch and combine S&P 500, S&P 400 MidCap, and NASDAQ-100 constituents.
    Deduplicates by ticker. Adds 'indices' column listing which indices each
    stock belongs to. Cached for 24 hours.
    """
    cached = cache_get("extended_universe", max_age=86400)
    if cached is not None:
        return pd.DataFrame(cached)

    from io import StringIO

    all_rows: Dict[str, Dict] = {}  # ticker -> row dict

    # ── S&P 500 ──────────────────────────────────────────────────────────────
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        resp = _yf_session.get(url)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text), flavor="lxml")
        sp500_df = tables[0][["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]].copy()
        sp500_df.columns = ["ticker", "name", "sector", "industry"]
        sp500_df["ticker"] = sp500_df["ticker"].str.replace(".", "-", regex=False)
        for _, row in sp500_df.iterrows():
            tk = row["ticker"]
            all_rows[tk] = {
                "ticker": tk,
                "name": row["name"],
                "sector": row["sector"],
                "industry": row["industry"],
                "indices": ["S&P 500"],
            }
        logger.info(f"Loaded S&P 500 list: {len(sp500_df)} stocks")
    except Exception as exc:
        logger.error(f"Failed to fetch S&P 500 list: {exc}")

    # ── S&P 400 MidCap ────────────────────────────────────────────────────────
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
        resp = _yf_session.get(url)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text), flavor="lxml")
        sp400_df = tables[0]
        # Column names can vary; try to find Symbol/Ticker and Security/Company
        col_map = {}
        for c in sp400_df.columns:
            cl = str(c).lower()
            if "symbol" in cl or "ticker" in cl:
                col_map["ticker"] = c
            elif "security" in cl or "company" in cl or "name" in cl:
                col_map["name"] = c
            elif "gics sector" in cl or "sector" in cl:
                col_map["sector"] = c
            elif "sub-industry" in cl or "industry" in cl:
                col_map["industry"] = c
        if "ticker" in col_map and "name" in col_map:
            for _, row in sp400_df.iterrows():
                tk = str(row[col_map["ticker"]]).replace(".", "-").lstrip("$").strip()
                if not tk or tk in ("nan", "None"):
                    continue
                sector = str(row.get(col_map.get("sector", ""), "Unknown"))
                industry = str(row.get(col_map.get("industry", ""), "Unknown"))
                if tk in all_rows:
                    if "S&P 400" not in all_rows[tk]["indices"]:
                        all_rows[tk]["indices"].append("S&P 400")
                else:
                    all_rows[tk] = {
                        "ticker": tk,
                        "name": str(row[col_map["name"]]),
                        "sector": sector if sector != "nan" else "Unknown",
                        "industry": industry if industry != "nan" else "Unknown",
                        "indices": ["S&P 400"],
                    }
            logger.info(f"Loaded S&P 400 list: {len(sp400_df)} stocks")
        else:
            logger.warning(f"S&P 400 table columns not recognized: {list(sp400_df.columns)}")
    except Exception as exc:
        logger.error(f"Failed to fetch S&P 400 list: {exc}")

    # ── NASDAQ-100 ────────────────────────────────────────────────────────────
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        resp = _yf_session.get(url)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text), flavor="lxml")
        ndx_df = None
        # Inspect all tables to find the one with Ticker/Symbol and Company columns
        for i, tbl in enumerate(tables):
            cols_lower = [str(c).lower() for c in tbl.columns]
            has_ticker = any("ticker" in c or "symbol" in c for c in cols_lower)
            has_company = any("company" in c or "security" in c or "name" in c for c in cols_lower)
            if has_ticker and has_company and len(tbl) > 50:
                ndx_df = tbl
                logger.info(f"Found NASDAQ-100 table at index {i}, columns: {list(tbl.columns)}")
                break
        if ndx_df is None and len(tables) > 4:
            # Fallback: try table index 4
            ndx_df = tables[4]
            logger.info(f"Falling back to NASDAQ-100 table[4], columns: {list(ndx_df.columns)}")

        if ndx_df is not None:
            col_map = {}
            for c in ndx_df.columns:
                cl = str(c).lower()
                if "ticker" in cl or "symbol" in cl:
                    col_map["ticker"] = c
                elif "company" in cl or "security" in cl or "name" in cl:
                    col_map["name"] = c
                elif "gics sector" in cl or "sector" in cl:
                    col_map["sector"] = c
                elif "sub-industry" in cl or "industry" in cl:
                    col_map["industry"] = c

            if "ticker" in col_map:
                added = 0
                for _, row in ndx_df.iterrows():
                    tk = str(row[col_map["ticker"]]).replace(".", "-").lstrip("$").strip()
                    if not tk or tk in ("nan", "None"):
                        continue
                    name = str(row[col_map.get("name", col_map["ticker"])]) if "name" in col_map else tk
                    # Prefer S&P sector data if we already have this ticker
                    if tk in all_rows:
                        if "NASDAQ-100" not in all_rows[tk]["indices"]:
                            all_rows[tk]["indices"].append("NASDAQ-100")
                    else:
                        sector_val = "Unknown"
                        industry_val = "Unknown"
                        if "sector" in col_map:
                            sv = str(row[col_map["sector"]])
                            sector_val = sv if sv not in ("nan", "None", "") else "Unknown"
                        if "industry" in col_map:
                            iv = str(row[col_map["industry"]])
                            industry_val = iv if iv not in ("nan", "None", "") else "Unknown"
                        all_rows[tk] = {
                            "ticker": tk,
                            "name": name if name not in ("nan", "None") else tk,
                            "sector": sector_val,
                            "industry": industry_val,
                            "indices": ["NASDAQ-100"],
                        }
                        added += 1
                logger.info(f"Loaded NASDAQ-100: {added} new tickers added")
            else:
                logger.warning(f"NASDAQ-100 table ticker column not found in: {list(ndx_df.columns)}")
    except Exception as exc:
        logger.error(f"Failed to fetch NASDAQ-100 list: {exc}")

    if not all_rows:
        logger.error("Extended universe is empty — all sources failed")
        return pd.DataFrame(columns=["ticker", "name", "sector", "industry", "indices"])

    df = pd.DataFrame(list(all_rows.values()))
    logger.info(f"Extended universe: {len(df)} unique tickers")
    cache_set("extended_universe", df.to_dict("records"))
    return df


def get_sp500_df() -> pd.DataFrame:
    """Backward-compatible alias for get_extended_universe()."""
    return get_extended_universe()


# ---------------------------------------------------------------------------
# Bulk price download
# ---------------------------------------------------------------------------

def _process_download(raw: pd.DataFrame, tickers_in_chunk: List[str]) -> Dict[str, Dict]:
    """Parse a yf.download result into our price dict and populate _price_histories."""
    if raw.empty:
        return {}
    close = (
        raw["Close"]
        if isinstance(raw.columns, pd.MultiIndex)
        else raw[["Close"]].rename(columns={"Close": tickers_in_chunk[0]})
    )
    results: Dict[str, Dict] = {}
    for ticker in close.columns:
        series = close[ticker].dropna()
        if len(series) < 100:
            continue
        idx = series.index
        if hasattr(idx, "tz") and idx.tz is not None:
            idx = idx.tz_localize(None)
        tz_naive = pd.Series(series.values, index=idx, dtype=float)
        _price_histories[str(ticker)] = tz_naive
        cur = float(series.iloc[-1])
        ago = float(series.iloc[-252]) if len(series) >= 252 else float(series.iloc[0])
        yoy = round(((cur - ago) / ago) * 100, 2) if ago else 0.0
        results[str(ticker)] = {
            "current_price": round(cur, 2),
            "price_1y_ago": round(ago, 2),
            "yoy_gain_pct": yoy,
            "sparkline": [round(float(v), 2) for v in series.tail(30).values],
        }
    return results


def fetch_bulk_prices(tickers: List[str]) -> Dict[str, Dict]:
    """
    Download 2-year Close prices for all tickers in chunks of 200.
    Retries each chunk once after a backoff on rate-limit errors.
    Never caches an empty result so the next request will retry.
    """
    cached = cache_get("bulk_prices", max_age=3600)
    if cached is not None:
        return cached

    CHUNK = 200
    all_results: Dict[str, Dict] = {}
    total_chunks = math.ceil(len(tickers) / CHUNK)
    logger.info(f"Bulk downloading prices for {len(tickers)} tickers in {total_chunks} chunks…")

    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
        chunk_num = i // CHUNK + 1
        for attempt in range(3):          # up to 3 tries per chunk
            try:
                raw = yf.download(
                    tickers=chunk,
                    period="2y",
                    auto_adjust=True,
                    progress=False,
                    threads=True,
                )
                chunk_results = _process_download(raw, chunk)
                all_results.update(chunk_results)
                logger.info(f"  Chunk {chunk_num}/{total_chunks}: {len(chunk_results)}/{len(chunk)} tickers OK")
                break
            except Exception as exc:
                wait = 10 * (attempt + 1)
                logger.warning(f"  Chunk {chunk_num} attempt {attempt+1} failed: {exc}. Retrying in {wait}s…")
                time.sleep(wait)
        # Brief pause between chunks to avoid rate limits
        if i + CHUNK < len(tickers):
            time.sleep(2)

    if not all_results:
        logger.error("Bulk price download returned no data — will retry on next request")
        return {}   # Do NOT cache empty result

    logger.info(f"Price data ready for {len(all_results)} tickers")
    cache_set("bulk_prices", all_results)
    return all_results


def get_price_near_date(ticker: str, target_date: str, max_days: int = 10) -> Optional[float]:
    """Return close price nearest to target_date (within max_days calendar days), or None."""
    series = _price_histories.get(ticker)
    if series is None or series.empty:
        return None
    idx = series.index
    # Ensure tz-naive
    if hasattr(idx, 'tz') and idx.tz is not None:
        idx = idx.tz_localize(None)
        series = pd.Series(series.values, index=idx)
    try:
        target = pd.Timestamp(target_date)
    except Exception:
        return None

    pos = idx.searchsorted(target)
    candidates = []
    for p in [max(0, pos - 1), pos, min(len(idx) - 1, pos + 1)]:
        diff = abs((idx[p] - target).days)
        if diff <= max_days:
            candidates.append((diff, float(series.iloc[p])))
    if not candidates:
        return None
    return min(candidates, key=lambda x: x[0])[1]


# ---------------------------------------------------------------------------
# Per-ticker analyst data
# ---------------------------------------------------------------------------

def fetch_analyst_info(sym: str) -> Dict:
    key = f"analyst_{sym}"
    # Fast path — no lock needed for cache hit
    cached = cache_get(key, max_age=3600)
    if cached is not None:
        return cached

    # Per-ticker lock prevents duplicate network calls from concurrent threads
    with _get_ticker_lock(sym):
        cached = cache_get(key, max_age=3600)  # re-check after acquiring lock
        if cached is not None:
            return cached

        # Semaphore caps total concurrent Yahoo Finance requests
        with _fetch_semaphore:
            info = _ticker_info(sym)
            time.sleep(_ANALYST_DELAY)

        num_analysts = int(info.get("numberOfAnalystOpinions") or 0)
        rec_mean_raw = info.get("recommendationMean")
        rec_mean = float(rec_mean_raw) if rec_mean_raw is not None else None
        target_price = info.get("targetMeanPrice")
        target_high = info.get("targetHighPrice")
        target_low = info.get("targetLowPrice")

        # Current-month buy/hold/sell breakdown from recommendations summary
        strong_buy = buy = hold = sell = strong_sell = 0
        try:
            rec_summary = _ticker_rec_summary(sym)
            if rec_summary is not None and not rec_summary.empty:
                cur = rec_summary[rec_summary["period"] == "0m"]
                if not cur.empty:
                    strong_buy  = int(cur["strongBuy"].iloc[0])
                    buy         = int(cur["buy"].iloc[0])
                    hold        = int(cur["hold"].iloc[0])
                    sell        = int(cur["sell"].iloc[0])
                    strong_sell = int(cur["strongSell"].iloc[0])
                    total_from_summary = strong_buy + buy + hold + sell + strong_sell
                    if total_from_summary > 0 and num_analysts == 0:
                        num_analysts = total_from_summary
        except Exception:
            pass

        # Firm-level upgrades/downgrades (yfinance 1.x)
        firms: List[str] = []
        grades: List[str] = []
        try:
            ud = _ticker_upgrades_downgrades(sym)
            if ud is not None:
                _ud_store[sym] = ud
                recent = ud.head(30)
                if "Firm" in recent.columns:
                    firms = [str(f) for f in recent["Firm"].tolist() if pd.notna(f) and str(f) != "nan"]
                if "ToGrade" in recent.columns:
                    grades = [str(g) for g in recent["ToGrade"].tolist() if pd.notna(g) and str(g) != "nan"]
        except Exception:
            pass

        result = {
            "num_analysts": num_analysts,
            "recommendation_mean": rec_mean,
            "target_price": float(target_price) if target_price else None,
            "target_high": float(target_high) if target_high else None,
            "target_low": float(target_low) if target_low else None,
            "strong_buy": strong_buy,
            "buy": buy,
            "hold": hold,
            "sell": sell,
            "strong_sell": strong_sell,
            "firms": firms,
            "grades": grades,
        }
        cache_set(key, result)
        return result


# ---------------------------------------------------------------------------
# Analyst firm performance scoring
# ---------------------------------------------------------------------------

def _is_bullish_grade(grade: str) -> bool:
    gl = grade.lower().strip()
    return any(kw in gl for kw in _BULLISH_KEYWORDS)


def _is_bearish_grade(grade: str) -> bool:
    gl = grade.lower().strip()
    return any(kw in gl for kw in _BEARISH_KEYWORDS)


def compute_analyst_firm_scores() -> Dict[str, Dict]:
    """
    Score each analyst firm based on historical recommendation accuracy.

    For each Buy/Strong Buy/Outperform/Overweight rec >= 6 months old:
      - Get stock price at rec date
      - Get stock price 6 months after rec date
      - If price rose → correct call
    For each Sell/Underperform/Underweight rec >= 6 months old:
      - If price fell → correct call

    Score (0–100) = accuracy_pct * 0.5 + avg_return_score * 0.3 + target_accuracy * 0.2
    Requires at least 5 scored calls for a firm to appear.
    """
    cached = cache_get("analyst_firm_scores", max_age=3600)
    if cached is not None:
        return cached

    now = datetime.now()
    cutoff_recent = now - timedelta(days=180)   # must be at least 6 months old
    cutoff_old = now - timedelta(days=730)       # no older than 2 years

    # firm -> aggregation dict
    firm_data: Dict[str, Dict] = {}

    for ticker, ud in _ud_store.items():
        if ud is None or ud.empty:
            continue

        # Reset index so GradeDate is a column
        ud_reset = ud.reset_index()
        date_col = ud_reset.columns[0]  # GradeDate

        for _, row in ud_reset.iterrows():
            try:
                grade_date_raw = row[date_col]
                if pd.isna(grade_date_raw):
                    continue
                grade_date = pd.Timestamp(grade_date_raw)
                # Strip timezone for comparison
                if grade_date.tzinfo is not None:
                    grade_date = grade_date.tz_localize(None)
                grade_date_dt = grade_date.to_pydatetime()

                # Only evaluate recs in our window
                if grade_date_dt > cutoff_recent or grade_date_dt < cutoff_old:
                    continue

                firm = str(row.get("Firm", "")) if pd.notna(row.get("Firm")) else ""
                if not firm or firm in ("nan", "None"):
                    continue

                to_grade = str(row.get("ToGrade", "")) if pd.notna(row.get("ToGrade")) else ""
                if not to_grade or to_grade in ("nan", "None"):
                    continue

                is_bullish = _is_bullish_grade(to_grade)
                is_bearish = _is_bearish_grade(to_grade)
                if not is_bullish and not is_bearish:
                    continue

                # Get price at rec date and 6 months later
                rec_date_str = grade_date.strftime("%Y-%m-%d")
                six_mo_later = (grade_date + pd.DateOffset(months=6)).strftime("%Y-%m-%d")

                price_at_rec = get_price_near_date(ticker, rec_date_str, max_days=10)
                price_6mo = get_price_near_date(ticker, six_mo_later, max_days=10)

                if price_at_rec is None or price_6mo is None or price_at_rec == 0:
                    continue

                actual_return = (price_6mo - price_at_rec) / price_at_rec * 100

                if is_bullish:
                    correct = actual_return > 0
                else:
                    correct = actual_return < 0

                # Target accuracy
                target_acc = None
                current_target = row.get("currentPriceTarget")
                if pd.notna(current_target) and current_target and price_at_rec:
                    try:
                        predicted_return = (float(current_target) - price_at_rec) / price_at_rec * 100
                        target_acc = max(0.0, 1.0 - abs(predicted_return - actual_return) / max(abs(predicted_return), 10))
                    except Exception:
                        target_acc = None

                if firm not in firm_data:
                    firm_data[firm] = {
                        "firm": firm,
                        "total_scored_calls": 0,
                        "correct_calls": 0,
                        "bull_calls": 0,
                        "bear_calls": 0,
                        "returns": [],
                        "target_accs": [],
                        "tickers": set(),
                    }

                fd = firm_data[firm]
                fd["total_scored_calls"] += 1
                if correct:
                    fd["correct_calls"] += 1
                if is_bullish:
                    fd["bull_calls"] += 1
                else:
                    fd["bear_calls"] += 1
                fd["returns"].append(actual_return)
                if target_acc is not None:
                    fd["target_accs"].append(target_acc)
                fd["tickers"].add(ticker)

            except Exception:
                continue

    # Compute scores
    results: Dict[str, Dict] = {}
    min_calls = 5

    for firm, fd in firm_data.items():
        n = fd["total_scored_calls"]
        if n < min_calls:
            continue

        accuracy_pct = (fd["correct_calls"] / n) * 100
        avg_return = float(np.mean(fd["returns"])) if fd["returns"] else 0.0
        avg_return_score = min(30.0, max(0.0, avg_return / 2.0))
        target_accuracy_score = float(np.mean(fd["target_accs"])) * 100 if fd["target_accs"] else 50.0

        score = (accuracy_pct * 0.5) + (avg_return_score * 0.3) + (target_accuracy_score * 0.2)
        score = round(min(100.0, max(0.0, score)), 2)

        results[firm] = {
            "firm": firm,
            "analyst_score": score,
            "accuracy_rate": round(accuracy_pct, 2),
            "avg_return": round(avg_return, 2),
            "total_scored_calls": n,
            "correct_calls": fd["correct_calls"],
            "bull_calls": fd["bull_calls"],
            "bear_calls": fd["bear_calls"],
            "tickers": sorted(fd["tickers"]),
            "stocks_covered": len(fd["tickers"]),
        }

    cache_set("analyst_firm_scores", results)
    logger.info(f"Computed analyst firm scores for {len(results)} firms")
    return results


# ---------------------------------------------------------------------------
# Scoring & record building
# ---------------------------------------------------------------------------

def rec_label(mean: Optional[float]) -> str:
    if mean is None:
        return "N/A"
    if mean <= 1.5:
        return "Strong Buy"
    if mean <= 2.5:
        return "Buy"
    if mean <= 3.5:
        return "Hold"
    if mean <= 4.5:
        return "Underperform"
    return "Sell"


def composite_score(yoy_gain: float, num_analysts: int, rec_mean: Optional[float]) -> float:
    """
    Score = yoy_gain × (1 + analyst_quality × analyst_confidence)
      - analyst_quality : 1.0 = Strong Buy … 0.0 = Sell
      - analyst_confidence: log-scaled by # analysts (saturates at ~60)
    More recommended  → higher quality → bigger multiplier.
    """
    quality = (5.5 - rec_mean) / 4.5 if rec_mean is not None else 0.5
    quality = max(0.0, min(1.0, quality))
    confidence = math.log1p(num_analysts) / math.log1p(60)
    confidence = min(1.0, confidence)
    return round(yoy_gain * (1.0 + quality * confidence), 4)


def build_record(sym: str, sp_row: Dict, price: Dict, analyst: Dict) -> Dict:
    cur = price["current_price"]
    tp = analyst.get("target_price")
    upside = round(((tp - cur) / cur) * 100, 2) if tp and cur else None
    rm = analyst.get("recommendation_mean")
    na = analyst.get("num_analysts", 0)
    return {
        "ticker": sym,
        "name": sp_row.get("name", sym),
        "sector": sp_row.get("sector", "Unknown"),
        "industry": sp_row.get("industry", "Unknown"),
        "indices": sp_row.get("indices", []),
        "current_price": cur,
        "price_1y_ago": price["price_1y_ago"],
        "yoy_gain_pct": price["yoy_gain_pct"],
        "sparkline": price.get("sparkline", []),
        "num_analysts": na,
        "recommendation": rec_label(rm),
        "recommendation_mean": rm,
        "target_price": tp,
        "target_high": analyst.get("target_high"),
        "target_low": analyst.get("target_low"),
        "upside_potential": upside,
        "strong_buy": analyst.get("strong_buy", 0),
        "buy": analyst.get("buy", 0),
        "hold": analyst.get("hold", 0),
        "sell": analyst.get("sell", 0),
        "strong_sell": analyst.get("strong_sell", 0),
        "firms": analyst.get("firms", []),
        "grades": analyst.get("grades", []),
        "composite_score": composite_score(price["yoy_gain_pct"], na, rm),
    }


# ---------------------------------------------------------------------------
# Parallel analyst fetch helpers
# ---------------------------------------------------------------------------

def _fetch_parallel(tickers: List[str], universe_map: Dict, price_map: Dict,
                    max_workers: int = 6) -> List[Dict]:
    """Fetch analyst data for a list of tickers in parallel, return built records."""
    records: List[Dict] = []
    records_lock = threading.Lock()

    def fetch_one(sym: str):
        analyst = fetch_analyst_info(sym)
        return build_record(sym, universe_map[sym], price_map[sym], analyst)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_one, sym): sym for sym in tickers}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                rec = future.result()
                with records_lock:
                    records.append(rec)
            except Exception as exc:
                logger.debug(f"Analyst fetch error for {sym}: {exc}")

    return records


def _background_fill(cache_key: str, remaining: List[str],
                     universe_map: Dict, price_map: Dict) -> None:
    """
    Fetch analyst data for remaining tickers in the background and merge
    into the existing cache entry as results arrive in batches.
    """
    BATCH = 50
    for i in range(0, len(remaining), BATCH):
        batch = remaining[i : i + BATCH]
        new_recs = _fetch_parallel(batch, universe_map, price_map, max_workers=6)

        # Merge into cached result
        existing = cache_get(cache_key, max_age=7200) or []
        existing_set = {r["ticker"] for r in existing}
        merged = existing + [r for r in new_recs if r["ticker"] not in existing_set]
        merged.sort(key=lambda x: x["composite_score"], reverse=True)
        cache_set(cache_key, merged)

        pct = min(100, round((i + len(batch)) / len(remaining) * 100))
        logger.info(f"Background fill [{cache_key}]: {i+len(batch)}/{len(remaining)} ({pct}%)")

    with _bg_lock:
        _bg_threads.pop(cache_key, None)
    logger.info(f"Background fill complete for [{cache_key}]")


# ---------------------------------------------------------------------------
# Main data pipeline
# ---------------------------------------------------------------------------

# How many top-YoY stocks to fetch analyst data for before returning a result
_FAST_ANALYST_COUNT = 150

def get_all_stocks(sector_filter: Optional[str] = None) -> List[Dict]:
    cache_key = f"stocks_{sector_filter or 'all'}"
    cached = cache_get(cache_key, max_age=3600)
    if cached is not None:
        return cached

    universe_df = get_extended_universe()
    if universe_df.empty:
        return []

    full_tickers = universe_df["ticker"].tolist()
    price_map = fetch_bulk_prices(full_tickers)
    if not price_map:
        logger.error("No price data — will retry on next request.")
        return []

    universe_map = {row["ticker"]: row for row in universe_df.to_dict("records")}

    if sector_filter:
        target_tickers = [
            t for t in full_tickers
            if universe_map.get(t, {}).get("sector") == sector_filter and t in price_map
        ]
    else:
        target_tickers = [t for t in full_tickers if t in price_map]

    # Sort by raw YoY gain — best performers get analyst data first
    target_tickers.sort(key=lambda t: price_map[t]["yoy_gain_pct"], reverse=True)

    fast_batch = target_tickers[:_FAST_ANALYST_COUNT]
    remaining  = target_tickers[_FAST_ANALYST_COUNT:]

    logger.info(f"Fast fetch: {len(fast_batch)} tickers (6 workers) — sector={sector_filter or 'all'}")
    records = _fetch_parallel(fast_batch, universe_map, price_map, max_workers=6)
    records.sort(key=lambda x: x["composite_score"], reverse=True)

    if records:
        cache_set(cache_key, records)

    # Kick off background fetch for the rest (doesn't block this response)
    if remaining:
        with _bg_lock:
            if cache_key not in _bg_threads or not _bg_threads[cache_key].is_alive():
                t = threading.Thread(
                    target=_background_fill,
                    args=(cache_key, remaining, universe_map, price_map),
                    daemon=True,
                )
                t.start()
                _bg_threads[cache_key] = t
                logger.info(f"Background fill started for {len(remaining)} remaining tickers")

    logger.info(f"Returning {len(records)} records immediately")
    return records


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/status")
def api_status():
    prices_age = cache_age("bulk_prices")
    stocks_age = cache_age("stocks_all")
    universe_cached = cache_get("extended_universe", max_age=86400)
    universe_size = len(universe_cached) if universe_cached else None
    return {
        "status": "ok",
        "prices_cache_age_seconds": round(prices_age, 0) if prices_age else None,
        "stocks_cache_age_seconds": round(stocks_age, 0) if stocks_age else None,
        "universe_cached": universe_size is not None,
        "universe_size": universe_size,
        "analyst_entries_cached": sum(1 for k in _cache if k.startswith("analyst_")),
        "analyst_scores_cached": cache_get("analyst_firm_scores", max_age=3600) is not None,
        "server_time": datetime.now().isoformat(),
    }


@app.get("/api/sectors")
def api_sectors():
    return {"sectors": SECTORS}


@app.get("/api/top20")
def api_top20(
    sector: Optional[str] = Query(None),
    sort_by: str = Query("composite"),
    min_analysts: int = Query(0),
    recommendation: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    stocks = get_all_stocks(sector)

    if min_analysts > 0:
        stocks = [s for s in stocks if s["num_analysts"] >= min_analysts]
    if recommendation:
        stocks = [s for s in stocks if s["recommendation"].lower() == recommendation.lower()]

    sort_map = {
        "yoy_gain":    lambda s: s["yoy_gain_pct"],
        "num_analysts":lambda s: s["num_analysts"],
        "upside":      lambda s: s["upside_potential"] if s["upside_potential"] is not None else -9999,
        "composite":   lambda s: s["composite_score"],
    }
    fn = sort_map.get(sort_by, sort_map["composite"])
    stocks_sorted = sorted(stocks, key=fn, reverse=True)

    ts_key = f"stocks_{sector or 'all'}"
    last_ts = _cache_ts.get(ts_key)
    return {
        "data": stocks_sorted[:limit],
        "total": len(stocks),
        "last_updated": datetime.fromtimestamp(last_ts).isoformat() if last_ts else None,
    }


@app.get("/api/search")
def api_search(
    q: Optional[str] = Query(None),
    analyst: Optional[str] = Query(None),
    min_analysts: int = Query(0),
    max_analysts: Optional[int] = Query(None),
    sector: Optional[str] = Query(None),
    recommendation: Optional[str] = Query(None),
    min_yoy: Optional[float] = Query(None),
    max_yoy: Optional[float] = Query(None),
    sort_by: str = Query("composite"),
    limit: int = Query(50, ge=1, le=200),
):
    stocks = get_all_stocks(sector)

    if q:
        qu = q.upper().strip()
        ql = q.lower().strip()
        stocks = [s for s in stocks if qu in s["ticker"] or ql in s["name"].lower()]
    if analyst:
        al = analyst.lower().strip()
        stocks = [s for s in stocks if any(al in f.lower() for f in s.get("firms", []))]
    if min_analysts > 0:
        stocks = [s for s in stocks if s["num_analysts"] >= min_analysts]
    if max_analysts is not None:
        stocks = [s for s in stocks if s["num_analysts"] <= max_analysts]
    if recommendation:
        stocks = [s for s in stocks if s["recommendation"].lower() == recommendation.lower()]
    if min_yoy is not None:
        stocks = [s for s in stocks if s["yoy_gain_pct"] >= min_yoy]
    if max_yoy is not None:
        stocks = [s for s in stocks if s["yoy_gain_pct"] <= max_yoy]

    sort_map = {
        "yoy_gain":    lambda s: s["yoy_gain_pct"],
        "num_analysts":lambda s: s["num_analysts"],
        "upside":      lambda s: s["upside_potential"] if s["upside_potential"] is not None else -9999,
        "composite":   lambda s: s["composite_score"],
    }
    fn = sort_map.get(sort_by, sort_map["composite"])
    stocks = sorted(stocks, key=fn, reverse=True)

    return {"data": stocks[:limit], "total": len(stocks)}


@app.get("/api/stock/{ticker}")
def api_stock_detail(ticker: str):
    sym = ticker.upper().strip()
    try:
        info = _ticker_info(sym)

        # Price history
        t = yf.Ticker(sym, session=_yf_session)
        hist = t.history(period="2y", auto_adjust=True)
        monthly_prices = []
        if not hist.empty:
            monthly = hist["Close"].resample("ME").last().dropna()
            monthly_prices = [
                {"date": str(d.date()), "price": round(float(p), 2)}
                for d, p in monthly.items()
            ]

        # Firm-level upgrades/downgrades history (yfinance 1.x)
        recs_list = []
        try:
            ud = _ticker_upgrades_downgrades(sym)
            if ud is not None:
                ud_reset = ud.reset_index()
                date_col = ud_reset.columns[0]  # GradeDate
                for _, row in ud_reset.head(60).iterrows():
                    recs_list.append({
                        "date": str(row[date_col])[:10] if pd.notna(row[date_col]) else "",
                        "firm": str(row.get("Firm", "")) if pd.notna(row.get("Firm")) else "",
                        "from_grade": str(row.get("FromGrade", "")) if pd.notna(row.get("FromGrade")) else "",
                        "to_grade": str(row.get("ToGrade", "")) if pd.notna(row.get("ToGrade")) else "",
                        "action": str(row.get("Action", "")) if pd.notna(row.get("Action")) else "",
                        "current_target": float(row["currentPriceTarget"]) if pd.notna(row.get("currentPriceTarget")) else None,
                        "prior_target": float(row["priorPriceTarget"]) if pd.notna(row.get("priorPriceTarget")) else None,
                    })
        except Exception as exc:
            logger.debug(f"Upgrades/downgrades fetch for {sym}: {exc}")

        # YoY calc
        yoy_gain = None
        if len(monthly_prices) >= 13:
            cp, ap = monthly_prices[-1]["price"], monthly_prices[-13]["price"]
            yoy_gain = round(((cp - ap) / ap) * 100, 2) if ap else None

        rm = info.get("recommendationMean")
        cur_price = info.get("currentPrice") or info.get("regularMarketPrice")
        tp = info.get("targetMeanPrice")
        upside = round(((tp - cur_price) / cur_price) * 100, 2) if tp and cur_price else None

        return {
            "ticker": sym,
            "name": info.get("shortName", sym),
            "long_name": info.get("longName", sym),
            "sector": info.get("sector", "Unknown"),
            "industry": info.get("industry", "Unknown"),
            "description": (info.get("longBusinessSummary") or "")[:600],
            "current_price": cur_price,
            "yoy_gain_pct": yoy_gain,
            "market_cap": info.get("marketCap"),
            "num_analysts": info.get("numberOfAnalystOpinions", 0),
            "recommendation": rec_label(rm),
            "recommendation_mean": rm,
            "target_price": tp,
            "target_high": info.get("targetHighPrice"),
            "target_low": info.get("targetLowPrice"),
            "upside_potential": upside,
            "pe_ratio": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "eps": info.get("trailingEps"),
            "dividend_yield": info.get("dividendYield"),
            "beta": info.get("beta"),
            "week_52_high": info.get("fiftyTwoWeekHigh"),
            "week_52_low": info.get("fiftyTwoWeekLow"),
            "avg_volume": info.get("averageVolume"),
            "price_history": monthly_prices,
            "recommendations": recs_list,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/analysts")
def api_analysts(limit: int = Query(50)):
    stocks = get_all_stocks()
    firm_scores = compute_analyst_firm_scores()

    firm_map: Dict[str, Dict] = {}
    for s in stocks:
        for firm in s.get("firms", []):
            if not firm or firm in ("nan", "None"):
                continue
            if firm not in firm_map:
                firm_map[firm] = {"firm": firm, "tickers": [], "count": 0, "yoys": []}
            if s["ticker"] not in firm_map[firm]["tickers"]:
                firm_map[firm]["tickers"].append(s["ticker"])
                firm_map[firm]["yoys"].append(s["yoy_gain_pct"])
            firm_map[firm]["count"] += 1

    out = []
    for f in firm_map.values():
        score_data = firm_scores.get(f["firm"], {})
        out.append({
            "firm": f["firm"],
            "stocks_covered": len(f["tickers"]),
            "recommendation_count": f["count"],
            "avg_yoy_gain": round(float(np.mean(f["yoys"])), 2) if f["yoys"] else None,
            "tickers": f["tickers"][:10],
            # Score fields (None if not enough data)
            "analyst_score": score_data.get("analyst_score"),
            "accuracy_rate": score_data.get("accuracy_rate"),
            "avg_return": score_data.get("avg_return"),
            "total_scored_calls": score_data.get("total_scored_calls"),
            "correct_calls": score_data.get("correct_calls"),
        })

    # Sort by analyst_score descending; firms with no score go last sorted by stocks_covered
    out.sort(key=lambda x: (
        x["analyst_score"] is not None,
        x["analyst_score"] if x["analyst_score"] is not None else 0,
        x["stocks_covered"],
    ), reverse=True)

    return {"analysts": out[:limit]}


@app.get("/api/analyst-rankings")
def api_analyst_rankings(min_calls: int = Query(5)):
    """Return analyst firms ranked by performance score."""
    firm_scores = compute_analyst_firm_scores()

    # Also pull stocks_covered from the coverage stats
    stocks = cache_get("stocks_all", max_age=3600) or []
    coverage_map: Dict[str, int] = {}
    for s in stocks:
        for firm in s.get("firms", []):
            if firm and firm not in ("nan", "None"):
                coverage_map[firm] = coverage_map.get(firm, 0) + 1

    ranked = []
    for firm, data in firm_scores.items():
        if data["total_scored_calls"] < min_calls:
            continue
        entry = dict(data)
        entry["tickers"] = data["tickers"][:10]  # limit tickers list for response size
        entry["stocks_covered"] = coverage_map.get(firm, data.get("stocks_covered", 0))
        ranked.append(entry)

    ranked.sort(key=lambda x: x["analyst_score"], reverse=True)
    return {"rankings": ranked, "total": len(ranked)}


@app.get("/api/progress")
def api_progress():
    """Return loading progress for each cache key being built."""
    with _bg_lock:
        active = {k: t.is_alive() for k, t in _bg_threads.items()}
    result = {}
    for key in list(_cache.keys()):
        if key.startswith("stocks_"):
            records = _cache[key]
            result[key] = {
                "loaded": len(records),
                "background_running": active.get(key, False),
            }
    prices_cached = "bulk_prices" in _cache
    return {
        "sectors": result,
        "prices_ready": prices_cached,
        "background_tasks": {k: v for k, v in active.items()},
    }


@app.post("/api/refresh")
def api_refresh():
    cleared = [k for k in list(_cache.keys()) if k.startswith("stocks_") or k == "bulk_prices"]
    for k in cleared:
        _cache.pop(k, None)
        _cache_ts.pop(k, None)
    # Note: background threads are daemons, they'll stop naturally or finish harmlessly
    return {"cleared": cleared, "message": "Cache cleared. Next request will re-fetch data."}


# ---------------------------------------------------------------------------
# Serve static frontend
# ---------------------------------------------------------------------------
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", include_in_schema=False)
def root():
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/{path:path}", include_in_schema=False)
def spa_fallback(path: str):
    fp = os.path.join(static_dir, "index.html")
    return FileResponse(fp)
