from __future__ import annotations

import json
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import yfinance as yf

from build_score_snapshot import (
    parse_universe,
    build_metrics_for_group,
    score_group,
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"

LOOKBACK_YEARS = 3
FORWARD_DAYS = 20
MIN_HISTORY = 120


def get_start_date() -> str:
    today = datetime.now(timezone.utc)
    start = today - timedelta(days=365 * LOOKBACK_YEARS)
    return start.strftime("%Y-%m-%d")


def download_prices(tickers: list[str]) -> dict[str, pd.Series]:
    print("Downloading prices...")

    df = yf.download(
        tickers=sorted(set(tickers)),
        start=get_start_date(),
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    price_map: dict[str, pd.Series] = {}

    if isinstance(df.columns, pd.MultiIndex):
        level0 = set(df.columns.get_level_values(0))

        for t in sorted(set(tickers)):
            try:
                if t not in level0:
                    continue
                if "Close" not in df[t].columns:
                    continue

                close = df[t]["Close"].dropna()
                if close is None or close.empty:
                    continue
                if len(close) < MIN_HISTORY:
                    continue

                price_map[t] = close
            except Exception:
                continue
    else:
        if "Close" in df.columns and len(tickers) == 1:
            close = df["Close"].dropna()
            if len(close) >= MIN_HISTORY:
                price_map[tickers[0]] = close

    print("Downloaded series:", len(price_map))
    return price_map


def future_return(series: pd.Series, idx: int) -> float | None:
    if idx + FORWARD_DAYS >= len(series):
        return None

    p0 = series.iloc[idx]
    p1 = series.iloc[idx + FORWARD_DAYS]

    if p0 is None or p1 is None or p0 == 0:
        return None

    return float(p1 / p0 - 1.0)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def copy_to_public(src: Path) -> None:
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst = PUBLIC_DATA_DIR / src.name
    shutil.copy2(src, dst)
    print(f"Copied -> {dst}")


def run() -> None:
    by_ticker, _groups = parse_universe()
    tickers = list(by_ticker.keys())

    price_map = download_prices(tickers)

    if not price_map:
        raise RuntimeError("No price data downloaded")

    trading_dates = list(next(iter(price_map.values())).index)

    samples: list[dict] = []

    # 대략 월별 샘플링
    for i in range(120, len(trading_dates) - FORWARD_DAYS, 20):
        date = trading_dates[i]

        past_prices: dict[str, pd.Series] = {}
        for t, s in price_map.items():
            s2 = s[s.index <= date]
            if len(s2) > 60:
                past_prices[t] = s2

        metrics = build_metrics_for_group(
            list(past_prices.keys()),
            by_ticker,
            past_prices,
        )

        scored = score_group(metrics)

        for r in scored:
            if r.final_score_100 is None:
                continue

            s = price_map.get(r.ticker)
            if s is None:
                continue
            if date not in s.index:
                continue

            idx = s.index.get_loc(date)
            if not isinstance(idx, int):
                continue

            fr = future_return(s, idx)
            if fr is None:
                continue

            samples.append(
                {
                    "date": str(date.date()),
                    "ticker": r.ticker,
                    "score": int(r.final_score_100),
                    "future_return": float(fr),
                }
            )

    df = pd.DataFrame(samples)

    if df.empty:
        raise RuntimeError("No samples generated for score correlation")

    df["quantile"] = pd.qcut(
        df["score"],
        5,
        labels=False,
        duplicates="drop",
    )

    df = df.dropna(subset=["quantile"]).copy()
    df["quantile"] = df["quantile"].astype(int)

    result_rows = []
    max_q = int(df["quantile"].max())

    for q in range(max_q + 1):
        sub = df[df["quantile"] == q]

        if sub.empty:
            continue

        result_rows.append(
            {
                "quantile": f"Q{q + 1}",
                "avg_return": float(sub["future_return"].mean()),
                "median_return": float(sub["future_return"].median()),
                "count": int(len(sub)),
                "avg_score": float(sub["score"].mean()),
            }
        )

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_years": LOOKBACK_YEARS,
        "forward_days": FORWARD_DAYS,
        "data": result_rows,
    }

    out_path = DATA_DIR / "score_correlation.json"
    save_json(out_path, out)
    print(f"Saved -> {out_path}")

    copy_to_public(out_path)


if __name__ == "__main__":
    run()