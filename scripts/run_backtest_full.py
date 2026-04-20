from __future__ import annotations

import json
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

from build_score_snapshot import (
    build_metrics_for_group,
    score_group,
    compute_portfolio_weights,
    PORTFOLIO_WEIGHT_METHOD,
    WEIGHT_ALPHA_SCORE,
    MIN_WEIGHT,
    MAX_WEIGHT,
    VOL_FALLBACK,
    VOL_WEIGHT_FLOOR,
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"

# ----------------------------
# CONFIG
# ----------------------------

BACKTEST_YEARS = 10

REBALANCE = "monthly"  # weekly | monthly
SELECTION = "BUY"      # BUY | TOP
TOP_N = 10

TRANSACTION_COST = 0.0015

BENCHMARK = "SPY"

MIN_HISTORY = 120

CURRENT_UNIVERSE_FILE = DATA_DIR / "sp500_current_wiki.json"
MEMBERSHIP_EVENTS_FILE = DATA_DIR / "sp500_membership_events.json"

# ----------------------------


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def copy_to_public(src: Path) -> None:
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst = PUBLIC_DATA_DIR / src.name
    shutil.copy2(src, dst)
    print(f"Copied -> {dst}")


def normalize_ticker(t: str) -> str:
    return str(t).strip().upper().replace(".", "-")


def get_start_date() -> str:
    today = datetime.now(timezone.utc)
    start = today - timedelta(days=365 * BACKTEST_YEARS + 260)
    return start.strftime("%Y-%m-%d")


def load_current_universe() -> dict[str, dict[str, Any]]:
    payload = load_json(CURRENT_UNIVERSE_FILE, default={})
    items = payload.get("items", [])
    out: dict[str, dict[str, Any]] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        ticker = normalize_ticker(item.get("ticker", ""))
        if not ticker:
            continue
        out[ticker] = {
            "ticker": ticker,
            "name": item.get("name", ""),
            "sector": item.get("sector", "Unknown") or "Unknown",
            "subIndustry": item.get("subIndustry", ""),
            "headquarters": item.get("headquarters", ""),
            "indexFlags": item.get("indexFlags", ["sp500"]),
        }

    if not out:
        raise RuntimeError(f"Failed to load current universe from {CURRENT_UNIVERSE_FILE}")

    return out


def load_membership_events() -> list[dict[str, Any]]:
    payload = load_json(MEMBERSHIP_EVENTS_FILE, default={})
    events = payload.get("events", [])
    if not isinstance(events, list) or not events:
        raise RuntimeError(f"Failed to load membership events from {MEMBERSHIP_EVENTS_FILE}")

    cleaned: list[dict[str, Any]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        date_str = str(ev.get("date", "")).strip()
        added = [normalize_ticker(x) for x in ev.get("added", []) if str(x).strip()]
        removed = [normalize_ticker(x) for x in ev.get("removed", []) if str(x).strip()]
        if not date_str:
            continue
        cleaned.append({
            "date": date_str,
            "added": added,
            "removed": removed,
        })

    cleaned.sort(key=lambda x: x["date"])
    return cleaned


def reconstruct_membership_as_of(
    as_of: pd.Timestamp,
    current_universe: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
) -> set[str]:
    """
    Reconstruct S&P500 membership at a target date by reversing later changes
    from the current constituent set.
    """
    members = set(current_universe.keys())

    for ev in sorted(events, key=lambda x: x["date"], reverse=True):
        ev_date = pd.to_datetime(ev["date"])
        if ev_date > as_of:
            for t in ev.get("added", []):
                members.discard(t)
            for t in ev.get("removed", []):
                members.add(t)

    return members


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
    out: list[pd.Timestamp] = []
    last = None

    for d in dates:
        key = d.strftime("%Y-%m")
        if key != last:
            out.append(d)
            last = key

    return out


def weekly_rebalance_dates(dates: pd.Index) -> list[pd.Timestamp]:
    out: list[pd.Timestamp] = []
    last = None

    for d in dates:
        y, w, _ = d.isocalendar()
        key = f"{y}-{w}"

        if key != last:
            out.append(d)
            last = key

    return out


def slice_price_map(
    price_map: dict[str, pd.Series],
    date: pd.Timestamp,
) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}

    for t, s in price_map.items():
        s2 = s[s.index <= date]
        if len(s2) > 60:
            out[t] = s2

    return out


def filter_price_map_by_membership(
    sliced_price_map: dict[str, pd.Series],
    membership: set[str],
) -> dict[str, pd.Series]:
    return {
        t: s for t, s in sliced_price_map.items()
        if t in membership
    }


def pick_portfolio(rows) -> list:
    rows = [r for r in rows if r.final_score_100 is not None]

    if SELECTION == "BUY":
        selected = [r for r in rows if r.signal == "BUY"]
    else:
        rows.sort(key=lambda x: x.final_score_100, reverse=True)
        selected = rows[:TOP_N]

    if not selected:
        return []

    if SELECTION == "BUY" and len(selected) > TOP_N:
        selected = sorted(selected, key=lambda x: x.final_score_100, reverse=True)[:TOP_N]

    selected = compute_portfolio_weights(
        selected,
        method=PORTFOLIO_WEIGHT_METHOD,
        alpha_score=WEIGHT_ALPHA_SCORE,
        min_w=MIN_WEIGHT,
        max_w=MAX_WEIGHT,
        vol_fallback=VOL_FALLBACK,
        vol_floor=VOL_WEIGHT_FLOOR,
    )
    return selected


def holdings_to_weight_map(selected_rows) -> dict[str, float]:
    if not selected_rows:
        return {}

    weights: dict[str, float] = {}
    for r in selected_rows:
        w = float(r.portfolio_weight or 0.0)
        if w > 0:
            weights[r.ticker] = w

    total = sum(weights.values())
    if total <= 0:
        return {}

    return {k: v / total for k, v in weights.items()}


def compute_daily_return(
    price_map: dict[str, pd.Series],
    holdings: dict[str, float],
    date: pd.Timestamp,
) -> float:
    if not holdings:
        return 0.0

    weighted_ret = 0.0
    used_weight = 0.0

    for t, w in holdings.items():
        s = price_map.get(t)
        if s is None:
            continue

        if date not in s.index:
            continue

        idx = s.index.get_loc(date)
        if idx == 0:
            continue

        r = float(s.iloc[idx] / s.iloc[idx - 1] - 1.0)
        weighted_ret += w * r
        used_weight += w

    if used_weight <= 0:
        return 0.0

    return float(weighted_ret / used_weight)


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


def build_benchmark_df(benchmark_series: pd.Series) -> pd.DataFrame:
    spy_equity = benchmark_series / benchmark_series.iloc[0]
    spy_returns = benchmark_series.pct_change().fillna(0.0)

    spy_df = pd.DataFrame({
        "date": [str(d.date()) for d in benchmark_series.index],
        "equity": spy_equity.values,
        "daily_return": spy_returns.values,
    })

    spy_df["drawdown"] = spy_df["equity"] / spy_df["equity"].cummax() - 1.0
    return spy_df


def compute_subperiod_metrics(df: pd.DataFrame, windows_years: list[int]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if df.empty:
        return out

    for years in windows_years:
        n = years * 252
        if len(df) < max(60, n // 2):
            continue

        sub = df.tail(n).copy() if len(df) > n else df.copy()

        # rebase equity to 1
        start_equity = float(sub["equity"].iloc[0])
        if start_equity <= 0:
            continue

        sub["equity"] = sub["equity"] / start_equity
        sub["drawdown"] = sub["equity"] / sub["equity"].cummax() - 1.0

        metrics = calc_metrics(sub)
        out.append({
            "label": f"{years}y",
            "metrics": metrics,
        })

    return out


def run_backtest() -> None:
    current_universe = load_current_universe()
    membership_events = load_membership_events()

    tickers = list(current_universe.keys())
    price_map = download_prices(tickers)

    if BENCHMARK not in price_map:
        raise RuntimeError("Benchmark data missing")

    benchmark_series = price_map[BENCHMARK]
    trading_dates = benchmark_series.index

    if REBALANCE == "monthly":
        rebalance_dates = set(monthly_rebalance_dates(trading_dates))
    else:
        rebalance_dates = set(weekly_rebalance_dates(trading_dates))

    current_holdings: dict[str, float] = {}
    current_holdings_list: list[str] = []

    pending_holdings: dict[str, float] | None = None
    pending_holdings_list: list[str] | None = None
    pending_cost: bool = False

    equity = 1.0
    high = 1.0
    history: list[dict] = []

    for date in trading_dates:
        # 1) execute yesterday's rebalance decision today
        if pending_holdings is not None:
            if pending_cost:
                equity *= (1 - TRANSACTION_COST)

            current_holdings = pending_holdings
            current_holdings_list = pending_holdings_list or []

            pending_holdings = None
            pending_holdings_list = None
            pending_cost = False

        # 2) today's return is earned by the holdings already in place
        daily_ret = compute_daily_return(price_map, current_holdings, date)
        equity *= (1 + daily_ret)
        high = max(high, equity)
        dd = equity / high - 1.0

        history.append({
            "date": str(date.date()),
            "equity": float(equity),
            "daily_return": float(daily_ret),
            "drawdown": float(dd),
            "holdings_count": len(current_holdings),
            "holdings": current_holdings_list,
            "weights": {k: round(v, 6) for k, v in current_holdings.items()},
        })

        # 3) decide next holdings using today's close data and point-in-time membership
        if date in rebalance_dates:
            sliced = slice_price_map(price_map, date)

            membership = reconstruct_membership_as_of(
                as_of=date,
                current_universe=current_universe,
                events=membership_events,
            )

            eligible_prices = filter_price_map_by_membership(sliced, membership)

            by_ticker_for_date = {
                t: current_universe.get(
                    t,
                    {
                        "ticker": t,
                        "sector": "Unknown",
                        "index_flags": ["sp500"],
                    },
                )
                for t in eligible_prices.keys()
            }

            metrics = build_metrics_for_group(
                list(eligible_prices.keys()),
                by_ticker_for_date,
                eligible_prices,
            )

            scored = score_group(metrics)
            selected_rows = pick_portfolio(scored)
            new_holdings = holdings_to_weight_map(selected_rows)
            new_holdings_list = sorted(new_holdings.keys())

            will_change = set(new_holdings_list) != set(current_holdings_list)

            pending_holdings = new_holdings
            pending_holdings_list = new_holdings_list
            pending_cost = will_change

    df = pd.DataFrame(history)
    strategy_metrics = calc_metrics(df)
    subperiods = compute_subperiod_metrics(df, windows_years=[3, 5, 10])

    spy_df = build_benchmark_df(benchmark_series)
    benchmark_metrics = calc_metrics(spy_df)

    result = {
        "strategy": {
            "rebalance": REBALANCE,
            "selection": SELECTION,
            "top_n": TOP_N,
            "transaction_cost": TRANSACTION_COST,
            "period_years": BACKTEST_YEARS,
            "execution_lag_days": 1,
            "universe_method": "point_in_time_sp500_membership_from_wikipedia_events",
            "portfolio_construction": {
                "method": PORTFOLIO_WEIGHT_METHOD,
                "score_alpha": WEIGHT_ALPHA_SCORE,
                "min_weight": MIN_WEIGHT,
                "max_weight": MAX_WEIGHT,
                "vol_fallback": VOL_FALLBACK,
                "vol_floor": VOL_WEIGHT_FLOOR,
                "formula": "weight_i ∝ (score_i ^ alpha) / max(vol20_i, vol_floor)",
            },
        },
        "metrics": strategy_metrics,
        "subperiods": subperiods,
        "benchmark": {
            "ticker": BENCHMARK,
            "metrics": benchmark_metrics,
        },
        "notes": [
            "Universe is reconstructed point-in-time using current S&P500 membership plus Wikipedia change events.",
            "Ranking uses historical slice through rebalance date close.",
            "Selected portfolio is applied with a 1-day execution lag.",
            "Transaction cost is applied when holdings change on execution day.",
            "Portfolio weights use score × inverse volatility weighting instead of equal weighting.",
            "Historical membership reconstruction depends on the quality of parsed Wikipedia change events.",
        ],
    }

    curve_df = df[["date", "equity"]].copy()
    curve_df = curve_df.rename(columns={"equity": "strategy"})

    bench_curve = spy_df[["date", "equity"]].copy()
    bench_curve = bench_curve.rename(columns={"equity": "benchmark"})

    merged_curve = pd.merge(curve_df, bench_curve, on="date", how="inner")
    equity_curve = merged_curve.to_dict(orient="records")

    backtest_result_path = DATA_DIR / "backtest_result.json"
    equity_curve_path = DATA_DIR / "equity_curve.json"

    save_json(backtest_result_path, result)
    save_json(equity_curve_path, equity_curve)

    print("\n===== RESULT =====\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print(f"\nSaved -> {backtest_result_path}")
    print(f"Saved -> {equity_curve_path}")

    copy_to_public(backtest_result_path)
    copy_to_public(equity_curve_path)


if __name__ == "__main__":
    run_backtest()