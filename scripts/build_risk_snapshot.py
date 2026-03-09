# scripts/build_risk_snapshot.py
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

UNIVERSE_FILE = DATA_DIR / "universe.json"
OUT_FILE = DATA_DIR / "risk_snapshot.json"

BATCH_SIZE = 50
LOOKBACK_PERIOD = "8mo"


def load_json(path: Path, default=None):
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, allow_nan=False)


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


def chunked(items: list[str], n: int) -> list[list[str]]:
    return [items[i:i + n] for i in range(0, len(items), n)]


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


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    if series is None or len(series) < period + 1:
        return pd.Series(index=series.index if series is not None else [])
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    roll_up = up.ewm(alpha=1 / period, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / period, adjust=False).mean()

    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def pct_return(series: pd.Series, lookback: int) -> float | None:
    if series is None or len(series) <= lookback:
        return None

    last_val = safe_num(series.iloc[-1])
    prev_val = safe_num(series.iloc[-(lookback + 1)])

    if last_val is None or prev_val is None or prev_val == 0:
        return None

    return (last_val / prev_val - 1.0) * 100.0


def annualized_volatility(series: pd.Series, lookback: int = 20) -> float | None:
    if series is None or len(series) <= lookback:
        return None

    rets = series.pct_change().dropna()
    if len(rets) < lookback:
        return None

    window = rets.iloc[-lookback:]
    vol = float(window.std() * np.sqrt(252))
    return vol


def max_drawdown(series: pd.Series, lookback: int = 60) -> float | None:
    if series is None or len(series) < 10:
        return None

    window = series.iloc[-lookback:] if len(series) > lookback else series
    if window.empty:
        return None

    roll_max = window.cummax()
    dd = (window / roll_max) - 1.0
    return float(dd.min())  # 음수


def download_close_map(tickers: list[str]) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}
    all_tickers = sorted(set(normalize_ticker(t) for t in tickers if t))

    for batch in chunked(all_tickers, BATCH_SIZE):
        try:
            df = yf.download(
                tickers=batch,
                period=LOOKBACK_PERIOD,
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=False,
            )
        except Exception as e:
            print(f"[WARN] download batch failed: {batch[:5]} -> {e}")
            continue

        if df is None or df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            for ticker in batch:
                try:
                    if ticker not in df.columns.get_level_values(0):
                        continue
                    sub = df[ticker]
                    if "Close" not in sub.columns:
                        continue
                    close = sub["Close"].dropna()
                    if not close.empty:
                        out[ticker] = close
                except Exception:
                    continue
        else:
            if len(batch) == 1 and "Close" in df.columns:
                close = df["Close"].dropna()
                if not close.empty:
                    out[batch[0]] = close

    return out


def build_one(ticker: str, close: pd.Series | None):
    if close is None or close.empty:
        return {
            "ticker": ticker,
            "close": None,
            "ret5d": None,
            "ret20d": None,
            "rsi14": None,
            "vol20": None,
            "dd60": None,
        }

    rsi_series = compute_rsi(close, 14)

    return {
        "ticker": ticker,
        "close": safe_num(close.iloc[-1]),
        "ret5d": safe_num(pct_return(close, 5)),
        "ret20d": safe_num(pct_return(close, 20)),
        "rsi14": safe_num(rsi_series.iloc[-1]) if not rsi_series.empty else None,
        "vol20": safe_num(annualized_volatility(close, 20)),
        "dd60": safe_num(max_drawdown(close, 60)),
    }


def main():
    tickers = read_universe_tickers()
    print(f"[INFO] total tickers: {len(tickers)}")

    close_map = download_close_map(tickers)
    print(f"[INFO] downloaded close series: {len(close_map)}")

    out = {}
    total = len(tickers)

    for i, ticker in enumerate(tickers, start=1):
        try:
            out[ticker] = build_one(ticker, close_map.get(ticker))
            if i % 50 == 0 or i == total:
                print(f"[{i}/{total}] {ticker}")
        except Exception as e:
            out[ticker] = {
                "ticker": ticker,
                "error": str(e),
                "close": None,
                "ret5d": None,
                "ret20d": None,
                "rsi14": None,
                "vol20": None,
                "dd60": None,
            }

    save_json(OUT_FILE, out)
    print(f"[OK] Saved -> {OUT_FILE}")


if __name__ == "__main__":
    main()