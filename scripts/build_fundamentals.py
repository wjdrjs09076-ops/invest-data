# scripts/build_fundamentals.py
from __future__ import annotations

import json
import math
from pathlib import Path

import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

UNIVERSE_FILE = DATA_DIR / "universe.json"
OUT_FILE = DATA_DIR / "fundamentals.json"


def load_json(path: Path, default=None):
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,  # JSON에 NaN 방지
        )


def normalize_ticker(t: str) -> str:
    return str(t).strip().upper().replace(".", "-")


def safe_num(x):
    try:
        if x is None:
            return None

        v = float(x)

        if math.isnan(v) or math.isinf(v):
            return None

        return v
    except Exception:
        return None


def read_universe_tickers() -> list[str]:
    universe = load_json(UNIVERSE_FILE, default={})
    tickers: set[str] = set()

    if isinstance(universe, dict):
        if isinstance(universe.get("items"), list):
            for item in universe["items"]:
                if isinstance(item, dict):
                    t = normalize_ticker(item.get("ticker") or item.get("symbol") or "")
                    if t:
                        tickers.add(t)

        for key in ["sp500", "nasdaq100", "dow30"]:
            arr = universe.get(key, [])
            if isinstance(arr, list):
                for x in arr:
                    if isinstance(x, str):
                        t = normalize_ticker(x)
                        if t:
                            tickers.add(t)
                    elif isinstance(x, dict):
                        t = normalize_ticker(x.get("ticker") or x.get("symbol") or "")
                        if t:
                            tickers.add(t)

    return sorted(tickers)


def first_available(df, labels: list[str]):
    if df is None or getattr(df, "empty", True):
        return None

    for label in labels:
        if label in df.index:
            try:
                val = df.loc[label].iloc[0]
                return safe_num(val)
            except Exception:
                pass

    idx = [str(x) for x in df.index]

    for label in labels:
        lower = label.lower()

        for i, raw in enumerate(idx):
            if lower == raw.lower():
                try:
                    val = df.iloc[i].iloc[0]
                    return safe_num(val)
                except Exception:
                    pass

    return None


def build_one(ticker: str):
    tk = yf.Ticker(ticker)

    info = {}
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    financials = None
    cashflow = None

    try:
        financials = tk.financials
    except Exception:
        financials = None

    try:
        cashflow = tk.cashflow
    except Exception:
        cashflow = None

    revenue = first_available(
        financials,
        [
            "Total Revenue",
            "Operating Revenue",
            "Revenue",
        ],
    )

    op_income = first_available(
        financials,
        [
            "Operating Income",
            "Operating Income or Loss",
        ],
    )

    cfo = first_available(
        cashflow,
        [
            "Operating Cash Flow",
            "Cash Flow From Continuing Operating Activities",
            "Net Cash Provided By Operating Activities",
            "Net Cash Provided by Operating Activities",
        ],
    )

    capex = first_available(
        cashflow,
        [
            "Capital Expenditure",
            "Capital Expenditures",
            "Purchase Of Property Plant And Equipment",
            "Payments To Acquire Property Plant And Equipment",
        ],
    )

    fcf = None
    if cfo is not None and capex is not None:
        fcf = safe_num(cfo - abs(capex))

    pe = safe_num(info.get("trailingPE"))
    ps = safe_num(info.get("priceToSalesTrailing12Months"))

    sector = info.get("sector") or None
    long_name = info.get("longName") or info.get("shortName") or None
    market_cap = safe_num(info.get("marketCap"))

    return {
        "ticker": ticker,
        "generated_at_utc": None,
        "source": "yfinance",
        "name": long_name,
        "sector": sector,
        "market_cap": market_cap,
        "multiples": {
            "pe": pe,
            "ps": ps,
        },
        "annual_latest": {
            "revenue": revenue,
            "op_income": op_income,
            "fcf": fcf,
            "cfo": cfo,
            "capex": capex,
        },
    }


def main():
    tickers = read_universe_tickers()

    out = {}
    total = len(tickers)

    for i, ticker in enumerate(tickers, start=1):

        try:
            out[ticker] = build_one(ticker)

            if i % 25 == 0 or i == total:
                print(f"[{i}/{total}] {ticker}")

        except Exception as e:

            out[ticker] = {
                "ticker": ticker,
                "generated_at_utc": None,
                "source": "yfinance",
                "error": str(e),
                "multiples": {"pe": None, "ps": None},
                "annual_latest": {
                    "revenue": None,
                    "op_income": None,
                    "fcf": None,
                    "cfo": None,
                    "capex": None,
                },
            }

    save_json(OUT_FILE, out)

    print(f"[OK] Saved -> {OUT_FILE}")


if __name__ == "__main__":
    main()