from __future__ import annotations

import json
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

BACKTEST_YEARS = 3
TOP_N = 10
MIN_HISTORY = 120
REBALANCE = "monthly"
BENCHMARK = "SPY"


def get_start_date() -> str:
    today = datetime.now(timezone.utc)
    start = today - timedelta(days=365 * BACKTEST_YEARS)
    return start.strftime("%Y-%m-%d")


def download_prices(tickers: list[str]) -> dict[str, pd.Series]:
    all_tickers = sorted(set(tickers + [BENCHMARK]))

    print("Downloading prices...")

    df = yf.download(
        tickers=all_tickers,
        start=get_start_date(),
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    price_map: dict[str, pd.Series] = {}

    if isinstance(df.columns, pd.MultiIndex):
        level0 = set(df.columns.get_level_values(0))

        for t in all_tickers:
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

    return price_map


def monthly_rebalance_dates(dates: pd.Index) -> list[pd.Timestamp]:
    out = []
    last = None
    for d in dates:
        key = d.strftime("%Y-%m")
        if key != last:
            out.append(d)
            last = key
    return out


def weekly_rebalance_dates(dates: pd.Index) -> list[pd.Timestamp]:
    out = []
    last = None
    for d in dates:
        y, w, _ = d.isocalendar()
        key = f"{y}-{w}"
        if key != last:
            out.append(d)
            last = key
    return out


def slice_price_map(price_map: dict[str, pd.Series], date: pd.Timestamp) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}
    for t, s in price_map.items():
        s2 = s[s.index <= date]
        if len(s2) > 60:
            out[t] = s2
    return out


def pick_topn(rows, top_n: int) -> list:
    rows = [r for r in rows if r.final_score_100 is not None]
    rows.sort(key=lambda x: x.final_score_100, reverse=True)
    return rows[:top_n]


def main() -> None:
    by_ticker, _groups = parse_universe()
    tickers = list(by_ticker.keys())

    price_map = download_prices(tickers)
    if BENCHMARK not in price_map:
        raise RuntimeError("Benchmark data missing")

    trading_dates = price_map[BENCHMARK].index

    if REBALANCE == "monthly":
        rebalance_dates = monthly_rebalance_dates(trading_dates)
    else:
        rebalance_dates = weekly_rebalance_dates(trading_dates)

    exposure_rows = []

    for date in rebalance_dates:
        past_prices = slice_price_map(price_map, date)

        metrics = build_metrics_for_group(
            list(past_prices.keys()),
            by_ticker,
            past_prices,
        )

        scored = score_group(metrics)
        selected = pick_topn(scored, TOP_N)

        if not selected:
            continue

        sector_counts: dict[str, int] = {}
        for r in selected:
            sector = r.sector or "Unknown"
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        for sector, count in sector_counts.items():
            exposure_rows.append({
                "date": str(date.date()),
                "sector": sector,
                "weight": count / TOP_N,
            })

    df = pd.DataFrame(exposure_rows)

    if df.empty:
        raise RuntimeError("No sector exposure rows generated")

    summary = (
        df.groupby("sector")["weight"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_n": TOP_N,
        "rebalance": REBALANCE,
        "avg_sector_exposure": summary.to_dict(orient="records"),
    }

    out_path = DATA_DIR / "sector_exposure.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(PUBLIC_DATA_DIR / "sector_exposure.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Saved -> {out_path}")
    print(f"Copied -> {PUBLIC_DATA_DIR / 'sector_exposure.json'}")


if __name__ == "__main__":
    main()