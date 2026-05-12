# scripts/build_fundamentals.py
"""
Fundamentals builder using Sharadar SF1 (MRY) + SHARADAR/TICKERS.

SF1 columns used:
  revenue, opinc, ncfo, capex, fcf, pe, ps, marketcap
TICKERS columns used:
  name, sector, industry
"""
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path

import nasdaqdatalink
import pandas as pd

nasdaqdatalink.ApiConfig.api_key = os.environ.get("NASDAQ_DATA_LINK_KEY", "NHr5446JR6sysBKtTBp1")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

UNIVERSE_FILE = DATA_DIR / "universe.json"
SP400_FILE    = DATA_DIR / "sp400_current_wiki.json"
SP600_FILE    = DATA_DIR / "sp600_current_wiki.json"
OUT_FILE      = DATA_DIR / "fundamentals.json"

DIMENSION  = "MRY"
CHUNK_SIZE = 200  # tickers per API call


def load_json(path: Path, default=None):
    if not path.exists():
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, allow_nan=False)


def normalize_ticker(t: str) -> str:
    return str(t).strip().upper().replace(".", "-")


def safe_num(x):
    try:
        v = float(x)
        return None if (math.isnan(v) or math.isinf(v)) else v
    except Exception:
        return None


def collect_tickers() -> list[str]:
    tickers: set[str] = set()

    universe = load_json(UNIVERSE_FILE, default={})
    if isinstance(universe, dict):
        for item in universe.get("items", []):
            if isinstance(item, dict):
                t = normalize_ticker(item.get("ticker") or item.get("symbol") or "")
                if t:
                    tickers.add(t)
        for key in ["sp500", "nasdaq100", "dow30", "sp400", "sp600"]:
            for x in universe.get(key, []):
                t = normalize_ticker(x if isinstance(x, str) else x.get("ticker", ""))
                if t:
                    tickers.add(t)

    for wiki_file in [SP400_FILE, SP600_FILE]:
        data = load_json(wiki_file, default=[])
        items = data.get("items", data) if isinstance(data, dict) else data
        for item in items:
            t = normalize_ticker(item if isinstance(item, str) else item.get("ticker", ""))
            if t:
                tickers.add(t)

    return sorted(tickers)


def fetch_sf1_batch(tickers: list[str]) -> pd.DataFrame:
    """Fetch MRY SF1 rows for a batch of tickers."""
    sf1_cols = [
        "ticker", "datekey", "calendardate",
        "revenue", "opinc", "ncfo", "capex", "fcf",
        "pe", "ps", "marketcap",
    ]
    try:
        df = nasdaqdatalink.get_table(
            "SHARADAR/SF1",
            ticker=tickers,
            dimension=DIMENSION,
            qopts={"columns": sf1_cols},
            paginate=True,
        )
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        print(f"  [WARN] SF1 batch error: {e}")
        return pd.DataFrame()


def fetch_tickers_meta(tickers: list[str]) -> dict[str, dict]:
    """Fetch name/sector/industry from SHARADAR/TICKERS."""
    meta: dict[str, dict] = {}
    try:
        df = nasdaqdatalink.get_table(
            "SHARADAR/TICKERS",
            ticker=tickers,
            qopts={"columns": ["ticker", "name", "sector", "industry", "exchange"]},
            paginate=True,
        )
        if df is not None and not df.empty:
            for _, row in df.drop_duplicates("ticker").iterrows():
                meta[str(row["ticker"]).upper()] = {
                    "name":     row.get("name") or None,
                    "sector":   row.get("sector") or None,
                    "industry": row.get("industry") or None,
                    "exchange": row.get("exchange") or None,
                }
    except Exception as e:
        print(f"  [WARN] TICKERS meta error: {e}")
    return meta


def build_latest_sf1(tickers: list[str]) -> dict[str, dict]:
    """Return {ticker: latest_MRY_row_dict} for all tickers."""
    latest: dict[str, dict] = {}
    total_chunks = (len(tickers) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i : i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        print(f"  SF1 chunk {chunk_num}/{total_chunks}: {len(chunk)} tickers")

        df = fetch_sf1_batch(chunk)
        if df.empty:
            continue

        df = df.sort_values("datekey")
        for ticker, grp in df.groupby("ticker"):
            row = grp.iloc[-1]
            latest[str(ticker).upper()] = row.to_dict()

    return latest


def main():
    tickers = collect_tickers()
    total = len(tickers)
    print(f"[INFO] Universe: {total} tickers")
    print(f"[INFO] Fetching Sharadar SF1 (MRY) fundamentals...")

    sf1_data = build_latest_sf1(tickers)
    print(f"[INFO] SF1 data: {len(sf1_data)} tickers with data")

    print(f"[INFO] Fetching SHARADAR/TICKERS metadata...")
    meta = fetch_tickers_meta(tickers)

    out: dict[str, dict] = {}
    ts = datetime.now(timezone.utc).isoformat()

    for ticker in tickers:
        row  = sf1_data.get(ticker, {})
        info = meta.get(ticker, {})

        revenue  = safe_num(row.get("revenue"))
        op_income = safe_num(row.get("opinc"))
        cfo      = safe_num(row.get("ncfo"))
        capex    = safe_num(row.get("capex"))
        fcf      = safe_num(row.get("fcf"))
        pe       = safe_num(row.get("pe"))
        ps       = safe_num(row.get("ps"))
        market_cap = safe_num(row.get("marketcap"))

        out[ticker] = {
            "ticker":            ticker,
            "generated_at_utc":  ts,
            "source":            "SHARADAR/SF1",
            "datekey":           str(row["datekey"])[:10] if row.get("datekey") else None,
            "name":              info.get("name"),
            "sector":            info.get("sector"),
            "industry":          info.get("industry"),
            "exchange":          info.get("exchange"),
            "market_cap":        market_cap,
            "multiples": {
                "pe": pe,
                "ps": ps,
            },
            "annual_latest": {
                "revenue":   revenue,
                "op_income": op_income,
                "fcf":       fcf,
                "cfo":       cfo,
                "capex":     capex,
            },
        }

    save_json(OUT_FILE, out)

    scored = sum(1 for v in out.values() if v.get("annual_latest", {}).get("revenue") is not None)
    print(f"\n[OK] Saved -> {OUT_FILE}")
    print(f"  Total: {total}  With revenue: {scored}")


if __name__ == "__main__":
    main()
