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
    compute_sector_strength,
    percentile_score_map,
    WEIGHT_MOM_21D,
    WEIGHT_MOM_63D,
    WEIGHT_SECTOR,
    WEIGHT_RISK,
    WEIGHT_VOL,
    WEIGHT_DD,
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"

BACKTEST_YEARS = 3
TOP_N = 10
BENCHMARK = "SPY"
MIN_HISTORY = 220
REBALANCE = "monthly"


def get_start_date() -> str:
    today = datetime.now(timezone.utc)
    start = today - timedelta(days=365 * BACKTEST_YEARS + 260)
    return start.strftime("%Y-%m-%d")


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def copy_to_public(src: Path) -> None:
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst = PUBLIC_DATA_DIR / src.name
    shutil.copy2(src, dst)
    print(f"Copied -> {dst}")


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
                if close is None or close.empty or len(close) < MIN_HISTORY:
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

    history_rows = []

    for date in rebalance_dates:
        past_prices = slice_price_map(price_map, date)

        metrics = build_metrics_for_group(
            list(past_prices.keys()),
            by_ticker,
            past_prices,
        )

        sector_strength_map, _ = compute_sector_strength(metrics)
        for r in metrics:
            r.sector_strength_63d = sector_strength_map.get(r.sector, None)

        mom21_map = percentile_score_map(metrics, lambda r: r.ret21d, higher_is_better=True)
        mom63_map = percentile_score_map(metrics, lambda r: r.ret63d, higher_is_better=True)
        sector_map = percentile_score_map(metrics, lambda r: r.sector_strength_63d, higher_is_better=True)
        vol_map = percentile_score_map(metrics, lambda r: r.vol20, higher_is_better=False)
        dd_map = percentile_score_map(metrics, lambda r: r.dd60, higher_is_better=False)

        rows = []
        for r in metrics:
            if r.close is None:
                continue

            mom21 = mom21_map.get(r.ticker)
            mom63 = mom63_map.get(r.ticker)
            sector_score = sector_map.get(r.ticker)
            vol_s = vol_map.get(r.ticker)
            dd_s = dd_map.get(r.ticker)

            risk_score = None
            if vol_s is not None and dd_s is not None:
                risk_score = (WEIGHT_VOL * vol_s + WEIGHT_DD * dd_s) / (WEIGHT_VOL + WEIGHT_DD)
            elif vol_s is not None:
                risk_score = vol_s
            elif dd_s is not None:
                risk_score = dd_s

            final_parts = []
            if mom21 is not None:
                final_parts.append(("momentum_21d", WEIGHT_MOM_21D, mom21))
            if mom63 is not None:
                final_parts.append(("momentum_63d", WEIGHT_MOM_63D, mom63))
            if sector_score is not None:
                final_parts.append(("sector", WEIGHT_SECTOR, sector_score))
            if risk_score is not None:
                final_parts.append(("risk", WEIGHT_RISK, risk_score))

            if not final_parts:
                continue

            total_weight = sum(w for _, w, _ in final_parts)
            final_score = sum(w * s for _, w, s in final_parts) / total_weight

            rows.append({
                "ticker": r.ticker,
                "momentum_21d": mom21,
                "momentum_63d": mom63,
                "sector": sector_score,
                "risk": risk_score,
                "final_score": final_score,
            })

        df = pd.DataFrame(rows)
        if df.empty:
            continue

        top = df.sort_values("final_score", ascending=False).head(TOP_N)

        history_rows.append({
            "date": str(date.date()),
            "momentum_21d": float(top["momentum_21d"].mean()),
            "momentum_63d": float(top["momentum_63d"].mean()),
            "sector": float(top["sector"].mean()),
            "risk": float(top["risk"].mean()),
        })

    hist_df = pd.DataFrame(history_rows)
    if hist_df.empty:
        raise RuntimeError("No factor decomposition rows generated")

    avg = {
        "momentum_21d": float(hist_df["momentum_21d"].mean()),
        "momentum_63d": float(hist_df["momentum_63d"].mean()),
        "sector": float(hist_df["sector"].mean()),
        "risk": float(hist_df["risk"].mean()),
    }

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_n": TOP_N,
        "rebalance": REBALANCE,
        "average_factor_scores": avg,
        "history": history_rows,
    }

    out_path = DATA_DIR / "factor_decomposition.json"
    save_json(out_path, out)
    print(f"Saved -> {out_path}")

    copy_to_public(out_path)


if __name__ == "__main__":
    main()