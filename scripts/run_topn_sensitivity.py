from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

import numpy as np
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
TRANSACTION_COST = 0.0015
BENCHMARK = "SPY"
MIN_HISTORY = 120
REBALANCE = "monthly"
TOP_N_LIST = [5, 10, 20]


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

    else:
        if "Close" in df.columns:
            close = df["Close"].dropna()
            if len(close) >= MIN_HISTORY:
                price_map[all_tickers[0]] = close

    print("Downloaded series:", len(price_map))
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


def pick_topn(rows, top_n: int) -> list[str]:
    rows = [r for r in rows if r.final_score_100 is not None]
    rows.sort(key=lambda x: x.final_score_100, reverse=True)
    return [r.ticker for r in rows[:top_n]]


def compute_daily_return(
    price_map: dict[str, pd.Series],
    tickers: list[str],
    date: pd.Timestamp,
) -> float:
    rets = []

    for t in tickers:
        s = price_map.get(t)
        if s is None or date not in s.index:
            continue

        idx = s.index.get_loc(date)
        if idx == 0:
            continue

        rets.append(float(s.iloc[idx] / s.iloc[idx - 1] - 1.0))

    if not rets:
        return 0.0

    return float(np.mean(rets))


def calc_metrics(df: pd.DataFrame) -> dict[str, float]:
    equity = float(df["equity"].iloc[-1])
    total_return = equity - 1.0
    years = len(df) / 252

    cagr = equity ** (1 / years) - 1 if years > 0 else 0.0
    vol = float(df["daily_return"].std() * np.sqrt(252))
    sharpe = cagr / vol if vol > 0 else 0.0
    mdd = float(df["drawdown"].min())

    return {
        "total_return": float(total_return),
        "cagr": float(cagr),
        "volatility": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(mdd),
    }


def run_single_topn(
    top_n: int,
    by_ticker: dict[str, dict],
    price_map: dict[str, pd.Series],
    trading_dates: pd.Index,
    rebalance_dates: set[pd.Timestamp],
) -> dict:
    holdings: list[str] = []
    equity = 1.0
    high = 1.0
    history = []

    for date in trading_dates:
        if date in rebalance_dates:
            past_prices = slice_price_map(price_map, date)

            metrics = build_metrics_for_group(
                list(past_prices.keys()),
                by_ticker,
                past_prices,
            )
            scored = score_group(metrics)
            new_holdings = pick_topn(scored, top_n)

            if set(new_holdings) != set(holdings):
                equity *= (1 - TRANSACTION_COST)

            holdings = new_holdings

        daily_ret = compute_daily_return(price_map, holdings, date)
        equity *= (1 + daily_ret)
        high = max(high, equity)
        dd = equity / high - 1.0

        history.append({
            "date": str(date.date()),
            "equity": float(equity),
            "daily_return": float(daily_ret),
            "drawdown": float(dd),
        })

    df = pd.DataFrame(history)
    metrics = calc_metrics(df)

    return {
        "top_n": top_n,
        "metrics": metrics,
    }


def main() -> None:
    by_ticker, _groups = parse_universe()
    tickers = list(by_ticker.keys())

    price_map = download_prices(tickers)

    if BENCHMARK not in price_map:
        raise RuntimeError("Benchmark data missing")

    trading_dates = price_map[BENCHMARK].index

    if REBALANCE == "monthly":
        rebalance_dates = set(monthly_rebalance_dates(trading_dates))
    else:
        rebalance_dates = set(weekly_rebalance_dates(trading_dates))

    results = []
    for top_n in TOP_N_LIST:
        print(f"[RUN] TOP_N={top_n}")
        results.append(
            run_single_topn(
                top_n=top_n,
                by_ticker=by_ticker,
                price_map=price_map,
                trading_dates=trading_dates,
                rebalance_dates=rebalance_dates,
            )
        )

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rebalance": REBALANCE,
        "transaction_cost": TRANSACTION_COST,
        "period_years": BACKTEST_YEARS,
        "results": results,
    }

    out_path = DATA_DIR / "topn_sensitivity.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(PUBLIC_DATA_DIR / "topn_sensitivity.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"Saved -> {out_path}")
    print(f"Copied -> {PUBLIC_DATA_DIR / 'topn_sensitivity.json'}")


if __name__ == "__main__":
    main()