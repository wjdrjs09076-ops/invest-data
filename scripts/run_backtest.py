from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import yfinance as yf

from build_score_snapshot import (
    parse_universe,
    download_close_map,
    build_metrics_for_group,
    score_group,
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

START = "2022-01-01"

REBALANCE = "monthly"  # monthly | weekly
SELECTION = "BUY"  # BUY | TOP
TOP_N = 10

TRANSACTION_COST = 0.001  # 10bps


def get_trading_dates(close_map):
    sample = next(iter(close_map.values()))
    return sample.index


def monthly_rebalance_dates(dates):
    out = []
    last = None
    for d in dates:
        m = d.strftime("%Y-%m")
        if m != last:
            out.append(d)
            last = m
    return out


def weekly_rebalance_dates(dates):
    out = []
    last = None
    for d in dates:
        y, w, _ = d.isocalendar()
        key = f"{y}-{w}"
        if key != last:
            out.append(d)
            last = key
    return out


def pick_portfolio(rows):
    rows = [r for r in rows if r.final_score_100 is not None]

    if SELECTION == "BUY":
        return [r.ticker for r in rows if r.signal == "BUY"]

    rows.sort(key=lambda x: x.final_score_100, reverse=True)
    return [r.ticker for r in rows[:TOP_N]]


def compute_returns(close_map, tickers, date):
    rets = []

    for t in tickers:
        s = close_map.get(t)
        if s is None:
            continue

        if date not in s.index:
            continue

        idx = s.index.get_loc(date)
        if idx == 0:
            continue

        r = s.iloc[idx] / s.iloc[idx - 1] - 1
        rets.append(r)

    if not rets:
        return 0

    return np.mean(rets)


def run_backtest():

    by_ticker, groups = parse_universe()

    tickers = list(by_ticker.keys())

    print("Downloading prices...")
    close_map = download_close_map(tickers)

    trading_dates = get_trading_dates(close_map)

    trading_dates = trading_dates[trading_dates >= START]

    if REBALANCE == "monthly":
        rebalance_dates = monthly_rebalance_dates(trading_dates)
    else:
        rebalance_dates = weekly_rebalance_dates(trading_dates)

    portfolio = []
    current_holdings = []
    equity = 1
    high = 1

    history = []

    for date in trading_dates:

        if date in rebalance_dates:

            metrics = build_metrics_for_group(tickers, by_ticker, close_map)
            scored = score_group(metrics)

            next_holdings = pick_portfolio(scored)

            if set(next_holdings) != set(current_holdings):
                equity *= (1 - TRANSACTION_COST)

            current_holdings = next_holdings

        daily_ret = compute_returns(close_map, current_holdings, date)

        equity *= (1 + daily_ret)

        high = max(high, equity)

        dd = equity / high - 1

        history.append(
            {
                "date": str(date.date()),
                "equity": equity,
                "daily_return": daily_ret,
                "drawdown": dd,
            }
        )

    df = pd.DataFrame(history)

    cagr = equity ** (252 / len(df)) - 1

    vol = df.daily_return.std() * np.sqrt(252)

    sharpe = cagr / vol if vol > 0 else 0

    mdd = df.drawdown.min()

    result = {
        "strategy": {
            "rebalance": REBALANCE,
            "selection": SELECTION,
            "top_n": TOP_N,
        },
        "metrics": {
            "final_return": equity - 1,
            "cagr": cagr,
            "volatility": vol,
            "sharpe": sharpe,
            "max_drawdown": mdd,
        },
    }

    print("\n==== RESULT ====")
    print(json.dumps(result, indent=2))

    out = DATA_DIR / "backtest_result.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2)

    print("saved ->", out)


if __name__ == "__main__":
    run_backtest()