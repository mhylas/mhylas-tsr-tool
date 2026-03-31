#!/usr/bin/env python3
"""
TSR Dashboard — Nightly Data Builder
=====================================
Fetches daily adjusted-close prices (true TSR, incl. dividends) for the full
Russell 3000 universe — current constituents plus every company that has ever
been in the index (accumulated over successive runs, so historical peers are
never dropped).

Run automatically via GitHub Actions each weekday after market close.
Can also be run manually: python build_data.py

Outputs
-------
data/universe.json            Company metadata used by the dashboard search
data/prices/{TICKER}.json     Per-ticker price history (dates + adj-close)
data/failed_tickers.json      Tickers that could not be fetched (for review)
"""

import json
import time
import io
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

import requests
import pandas as pd
import yfinance as yf

# ── Configuration ─────────────────────────────────────────────────────────────

YEARS       = 5          # years of history to store (increase to 20 later)
BATCH_SIZE  = 50         # tickers per yfinance download call
BATCH_DELAY = 3          # seconds between batches (avoids Yahoo rate-limiting)

DATA_DIR    = Path("data/prices")
UNIVERSE_F  = Path("data/universe.json")
EXTRAS_F    = Path("data/extra_tickers.json")   # optional manual additions

START_DATE  = (datetime.today() - timedelta(days=365 * YEARS + 90)).strftime('%Y-%m-%d')
END_DATE    = datetime.today().strftime('%Y-%m-%d')

# Benchmark ETFs always included regardless of index membership
BENCHMARKS = {
    "SPY": "S&P 500 (SPY)",
    "IWM": "Russell 2000 (IWM)",
    "IWV": "Russell 3000 (IWV)",
    "QQQ": "NASDAQ-100 (QQQ)",
    "MDY": "S&P MidCap 400 (MDY)",
    "DIA": "Dow Jones Industrial Avg (DIA)",
}

# iShares ETF holdings CSV endpoints (covers current Russell 3000 constituents)
ETF_SOURCES = {
    "Russell 3000 (IWV)": (
        "https://www.ishares.com/us/products/239714/ishares-russell-3000-etf"
        "/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund"
    ),
    "Russell 2000 (IWM)": (
        "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf"
        "/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    ),
    "S&P 500 (IVV)": (
        "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf"
        "/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"
    ),
    "S&P MidCap 400 (IJH)": (
        "https://www.ishares.com/us/products/239763/ishares-sp-midcap-400-etf"
        "/1467271812596.ajax?fileType=csv&fileName=IJH_holdings&dataType=fund"
    ),
}


# ── Universe builder ──────────────────────────────────────────────────────────

def parse_ishares_csv(text: str) -> dict:
    """Parse iShares holdings CSV → {ticker: company_name}."""
    lines = text.splitlines()
    # iShares CSVs have several header/metadata rows before the actual data
    hdr_idx = next(
        (i for i, ln in enumerate(lines) if "Ticker" in ln or "Symbol" in ln),
        None,
    )
    if hdr_idx is None:
        return {}
    df = pd.read_csv(
        io.StringIO("\n".join(lines[hdr_idx:])),
        on_bad_lines="skip",
        dtype=str,
    )
    df.columns = [c.strip().lower() for c in df.columns]
    tkr_col  = next((c for c in df.columns if "ticker" in c or "symbol" in c), None)
    name_col = next((c for c in df.columns if c == "name"), None)
    if not tkr_col:
        return {}
    result = {}
    for _, row in df.iterrows():
        t = str(row[tkr_col]).strip().upper()
        # Skip cash, options, blank rows, or anything that isn't a plain ticker
        if (not t or t in {"-", "CASH", "N/A", "NAN"}
                or len(t) > 6 or "/" in t or "." in t):
            continue
        name = str(row[name_col]).strip() if name_col else t
        result[t] = name
    return result


def fetch_etf_holdings(label: str, url: str) -> dict:
    """Download one iShares ETF holdings file and return {ticker: name}."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; TSR-DataBuilder/1.0)"}
    try:
        r = requests.get(url, headers=headers, timeout=45)
        r.raise_for_status()
        holdings = parse_ishares_csv(r.text)
        print(f"    {label}: {len(holdings)} tickers")
        return holdings
    except Exception as exc:
        print(f"    {label}: FAILED — {exc}")
        return {}


def build_universe() -> dict:
    """
    Return {ticker: name} for the full universe.

    Key design: tickers are NEVER removed — once added they stay forever.
    This means that over successive nightly runs the universe accumulates
    every company that has ever been a constituent, handling reconstitutions
    and delistings automatically.
    """
    print("Building ticker universe...")
    universe: dict = {}

    # Current ETF constituents
    for label, url in ETF_SOURCES.items():
        universe.update(fetch_etf_holdings(label, url))

    # Standard benchmarks
    universe.update(BENCHMARKS)

    # Optional manual additions (e.g. foreign-listed peers, specific names)
    if EXTRAS_F.exists():
        extras = json.loads(EXTRAS_F.read_text())
        universe.update(extras)
        print(f"    Extra tickers file: {len(extras)} added")

    # Re-add every ticker seen in previous runs — this is what accumulates
    # historical constituents over time without needing a paid constituent DB
    if UNIVERSE_F.exists():
        prev = json.loads(UNIVERSE_F.read_text())
        added = 0
        for c in prev.get("companies", []):
            t = c["ticker"]
            if t not in universe:
                universe[t] = c.get("name", t)
                added += 1
        if added:
            print(f"    Re-added {added} historical tickers from previous run")

    print(f"  Total universe: {len(universe)} unique tickers\n")
    return universe


# ── Price data helpers ────────────────────────────────────────────────────────

def price_path(ticker: str) -> Path:
    return DATA_DIR / f"{ticker}.json"


def load_existing(ticker: str):
    p = price_path(ticker)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return None
    return None


def needs_update(ticker: str) -> bool:
    """True if the ticker has no data or its data is more than 1 day old."""
    existing = load_existing(ticker)
    if not existing or not existing.get("dates"):
        return True
    try:
        last = datetime.strptime(existing["dates"][-1], "%Y-%m-%d").date()
        return (date.today() - last).days > 1
    except Exception:
        return True


def merge(existing, new_dates: list, new_prices: list) -> dict:
    """Append new trading days to existing data, keeping chronological order."""
    if not existing:
        return {"dates": new_dates, "prices": new_prices}
    known = set(existing["dates"])
    additions = [
        (d, p) for d, p in zip(new_dates, new_prices) if d not in known
    ]
    if not additions:
        return existing
    add_d, add_p = zip(*additions)
    combined = sorted(
        zip(existing["dates"] + list(add_d), existing["prices"] + list(add_p))
    )
    return {
        "dates":  [x[0] for x in combined],
        "prices": [x[1] for x in combined],
    }


def download_batch(tickers: list) -> dict:
    """
    Download adjusted-close prices for up to BATCH_SIZE tickers via yfinance.
    Returns {ticker: {dates: [...], prices: [...]}}

    yfinance auto_adjust=True makes 'Close' the dividend-and-split-adjusted
    price — exactly what we need for true Total Shareholder Return.
    """
    if not tickers:
        return {}

    results = {}
    try:
        raw = yf.download(
            tickers,
            start=START_DATE,
            end=END_DATE,
            auto_adjust=True,   # Close = adjusted for dividends + splits (true TSR)
            progress=False,
            threads=True,
            group_by="ticker",
        )

        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    # Single-ticker download: flat DataFrame
                    series = raw["Close"].dropna() if "Close" in raw.columns else pd.Series(dtype=float)
                else:
                    # Multi-ticker: grouped by ticker
                    series = (
                        raw[ticker]["Close"].dropna()
                        if ticker in raw.columns
                        else pd.Series(dtype=float)
                    )
                if len(series) >= 20:
                    results[ticker] = {
                        "dates":  [d.strftime("%Y-%m-%d") for d in series.index],
                        "prices": [round(float(v), 4) for v in series.values],
                    }
            except Exception:
                pass  # individual ticker parse failure — move on

    except Exception as exc:
        print(f"      Batch download error: {exc}")

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"{'='*60}")
    print(f"  TSR Data Builder  —  {ts}")
    print(f"{'='*60}\n")

    # ── 1. Build universe ──────────────────────────────────────────────────
    universe = build_universe()
    tickers  = sorted(universe)

    # ── 2. Save universe.json ──────────────────────────────────────────────
    UNIVERSE_F.write_text(json.dumps({
        "generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "years":     YEARS,
        "count":     len(universe),
        "companies": [{"ticker": t, "name": universe[t]} for t in tickers],
    }, indent=2))
    print(f"Saved universe.json  ({len(tickers)} companies)\n")

    # ── 3. Find stale tickers ──────────────────────────────────────────────
    stale = [t for t in tickers if needs_update(t)]
    print(f"{len(stale)} of {len(tickers)} tickers need updating\n")

    if not stale:
        print("All data is current — nothing to do.")
        return

    # ── 4. Fetch in batches ────────────────────────────────────────────────
    updated, failed = 0, []
    total_batches   = (len(stale) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(stale), BATCH_SIZE):
        batch     = stale[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        preview   = ", ".join(batch[:4]) + ("…" if len(batch) > 4 else "")
        print(f"  Batch {batch_num}/{total_batches}  [{preview}]", end="  ", flush=True)

        fetched = download_batch(batch)
        ok = 0

        for ticker in batch:
            if ticker in fetched and fetched[ticker]["dates"]:
                merged = merge(load_existing(ticker),
                               fetched[ticker]["dates"],
                               fetched[ticker]["prices"])
                merged["name"] = universe.get(ticker, ticker)
                price_path(ticker).write_text(
                    json.dumps(merged, separators=(",", ":"))  # compact JSON
                )
                ok      += 1
                updated += 1
            else:
                failed.append(ticker)

        print(f"→ {ok}/{len(batch)} ok")

        if i + BATCH_SIZE < len(stale):
            time.sleep(BATCH_DELAY)

    # ── 5. Summary ─────────────────────────────────────────────────────────
    print(f"\n{'─'*40}")
    print(f"Updated : {updated}")
    print(f"Failed  : {len(failed)}")
    if failed:
        Path("data/failed_tickers.json").write_text(json.dumps(failed, indent=2))
        print(f"Failed list saved to data/failed_tickers.json")
        # Non-zero exit so GitHub Actions marks the run as failed for review
        if len(failed) > len(stale) * 0.5:
            sys.exit(1)   # fail only if >50% of tickers failed


if __name__ == "__main__":
    main()
