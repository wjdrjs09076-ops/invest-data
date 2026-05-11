#!/usr/bin/env python3
"""
update_live_performance.py — 일별 실전 수익률 경량 업데이터

live_performance.json의 마지막 날짜 이후 거래일을 yfinance로 가져와
current_portfolio 가중치 기준으로 일별 수익률을 계산·추가한다.

매일 장 마감 후 GitHub Actions에서 실행.
run_backtest_regime.py 전체 재실행 없이 빠르게 업데이트.
"""
from __future__ import annotations

import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf
import pandas as pd

ROOT            = Path(__file__).resolve().parents[1]
DATA_DIR        = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _recompute_summary(daily: list[dict]) -> dict:
    if not daily:
        return {}
    total_ret = daily[-1]["equity"] - 1.0
    days = len(daily)
    years = days / 252
    cagr = daily[-1]["equity"] ** (1 / years) - 1.0 if years > 0 else 0.0
    rets = [d["daily_return"] for d in daily[1:]]
    if rets:
        mean_r = sum(rets) / len(rets)
        vol = (sum((r - mean_r) ** 2 for r in rets) / len(rets)) ** 0.5 * math.sqrt(252)
    else:
        vol = 0.0
    sharpe = cagr / vol if vol > 0 else 0.0
    mdd = min(d["drawdown"] for d in daily)
    return {
        "total_return": round(total_ret, 6),
        "days":         days,
        "cagr":         round(cagr, 6),
        "sharpe":       round(sharpe, 4),
        "mdd":          round(mdd, 6),
        "last_date":    daily[-1]["date"],
        "last_equity":  daily[-1]["equity"],
    }


def update() -> None:
    perf_path = DATA_DIR / "live_performance.json"
    if not perf_path.exists():
        print("[오류] live_performance.json 없음 — build_live_performance.py 먼저 실행하세요.")
        return

    data = _load_json(perf_path)
    if not data or not data.get("daily"):
        print("[오류] live_performance.json 데이터 없음")
        return

    daily: list[dict] = data["daily"]
    last_date = daily[-1]["date"]
    last_equity = float(daily[-1]["equity"])
    portfolio: dict[str, float] = data.get("current_portfolio", {})

    if not portfolio:
        print("[WARN] current_portfolio 비어있음 — 포트폴리오 가중치 없이 업데이트 불가")
        return

    # SPY도 포함해서 벤치마크 업데이트
    tickers = sorted(set(list(portfolio.keys()) + ["SPY"]))
    fetch_start = (pd.Timestamp(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if fetch_start > today_str:
        print(f"[OK] 이미 최신 ({last_date}). 추가할 데이터 없음.")
        return

    print(f"[FETCH] {fetch_start} ~ {today_str}  ({len(tickers)} tickers)")
    raw = yf.download(
        tickers=tickers,
        start=fetch_start,
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    if raw is None or raw.empty:
        print("[WARN] yfinance 데이터 없음")
        return

    # 종목별 종가 시리즈 추출
    prices: dict[str, pd.Series] = {}
    if isinstance(raw.columns, pd.MultiIndex):
        for t in tickers:
            try:
                s = raw[t]["Close"].dropna()
                if not s.empty:
                    prices[t] = s
            except Exception:
                continue
    elif "Close" in raw.columns and len(tickers) == 1:
        prices[tickers[0]] = raw["Close"].dropna()

    if not prices:
        print("[WARN] 가격 데이터 파싱 실패")
        return

    # 날짜 정렬 후 전날 SPY equity (벤치마크 연속성)
    all_dates = sorted(set().union(*[set(s.index) for s in prices.values()]))
    bench_last = float(daily[-1].get("benchmark", 1.0))

    # 포트폴리오 가중치 정규화
    w_total = sum(portfolio.values())
    norm_w = {t: w / w_total for t, w in portfolio.items() if w > 0}

    # 드로우다운 계속 추적: 현재까지의 running max (재기준화된 equity 기준)
    running_max = max(d["equity"] for d in daily)

    new_records: list[dict] = []
    prev_prices: dict[str, float] = {}  # 직전일 종가 (수익률 계산용)

    for date in all_dates:
        date_str = str(date.date())
        if date_str <= last_date:
            continue

        # 종목별 당일 종가
        cur_prices: dict[str, float] = {}
        for t, s in prices.items():
            if date in s.index:
                cur_prices[t] = float(s[date])

        if not cur_prices:
            continue

        # 포트폴리오 수익률
        if prev_prices:
            weighted_ret = 0.0
            used_w = 0.0
            for t, w in norm_w.items():
                if t in cur_prices and t in prev_prices and prev_prices[t] > 0:
                    r = cur_prices[t] / prev_prices[t] - 1.0
                    weighted_ret += w * r
                    used_w += w
            dr = weighted_ret / used_w if used_w > 0 else 0.0
        else:
            dr = 0.0

        equity = last_equity * (1.0 + dr) if not new_records else new_records[-1]["equity"] * (1.0 + dr)
        running_max = max(running_max, equity)
        dd = equity / running_max - 1.0

        # SPY 벤치마크
        if "SPY" in cur_prices and "SPY" in prev_prices and prev_prices.get("SPY", 0) > 0:
            bench_dr = cur_prices["SPY"] / prev_prices["SPY"] - 1.0
            bench_last = bench_last * (1.0 + bench_dr)
        bench_eq = bench_last

        new_records.append({
            "date":          date_str,
            "equity":        round(equity, 6),
            "daily_return":  round(dr, 6),
            "drawdown":      round(dd, 6),
            "benchmark":     round(bench_eq, 6),
            "regime":        "live",
            "stock_exposure": 1.0,
        })

        prev_prices = cur_prices

    if not new_records:
        print(f"[OK] 새 거래일 없음 (last: {last_date})")
        return

    daily.extend(new_records)
    data["daily"]        = daily
    data["summary"]      = _recompute_summary(daily)
    data["generated_at"] = datetime.now(timezone.utc).isoformat()

    # live_state.json에서 최신 포트폴리오 로드 (리밸런싱 있었을 경우 반영)
    live_state = _load_json(DATA_DIR / "live_state.json")
    if live_state and live_state.get("portfolio_weights"):
        data["current_portfolio"] = live_state["portfolio_weights"]
        data["regime_bucket"]     = live_state.get("regime_bucket", "unknown")
        data["stock_exposure"]    = live_state.get("stock_exposure", 1.0)

    with open(perf_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(perf_path, PUBLIC_DATA_DIR / "live_performance.json")

    added = len(new_records)
    last = new_records[-1]
    print(f"[OK] {added}일 추가 ({new_records[0]['date']} ~ {last['date']})")
    print(f"     equity={last['equity']:.4f}  총수익={data['summary']['total_return']:+.2%}  Sharpe={data['summary']['sharpe']:.2f}")


if __name__ == "__main__":
    update()
