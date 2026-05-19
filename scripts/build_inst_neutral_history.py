#!/usr/bin/env python3
"""
build_inst_neutral_history.py
사이즈 중립 기관 크라우딩 신호 사전 계산

각 분기 말 시점에 크로스섹셔널 회귀:
    inst_ownership_pct ~ log(market_cap)
잔차 = 사이즈 효과를 제거한 순수 기관 소유 편차

결과: data/inst_neutral_history.pkl
  {ticker: {"quarters": ["2014-03-31", ...], "residuals": [float, ...]}}
"""
from __future__ import annotations

import bisect
import math
import pickle
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SF3_FILE   = DATA_DIR / "sf3_history.pkl"
DAILY_FILE = DATA_DIR / "daily_history.pkl"
OUT_FILE   = DATA_DIR / "inst_neutral_history.pkl"

# 13F 파일링 딜레이 보정 (45일)
FILING_LAG_DAYS = 45


def load_sf3() -> dict:
    payload = pickle.load(open(SF3_FILE, "rb"))
    return payload.get("lookup", {})


def load_daily() -> dict:
    payload = pickle.load(open(DAILY_FILE, "rb"))
    return payload.get("lookup", {})


def get_daily_mcap(daily_lookup: dict, ticker: str, as_of_str: str) -> float | None:
    rec = daily_lookup.get(ticker)
    if not rec:
        return None
    dates  = rec["dates"]
    mcaps  = rec["marketcap"]
    idx = bisect.bisect_right(dates, as_of_str) - 1
    if idx < 0:
        return None
    v = mcaps[idx]
    return float(v) if v is not None and v > 0 else None


def get_sf3_level(sf3_lookup: dict, ticker: str, as_of_str: str) -> float | None:
    rec = sf3_lookup.get(ticker)
    if not rec:
        return None
    quarters = rec["quarters"]
    values   = rec["values"]
    idx = bisect.bisect_right(quarters, as_of_str) - 1
    if idx < 0 or values[idx] is None:
        return None
    return float(values[idx])


def ols_residuals(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """단순 OLS: y ~ a + b*x, 잔차 반환"""
    X = np.column_stack([np.ones(len(x)), x])
    try:
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        return y - X @ beta
    except Exception:
        return y - y.mean()


def main():
    print("=== 사이즈 중립 기관 크라우딩 신호 사전 계산 ===")
    sf3   = load_sf3()
    daily = load_daily()
    print(f"SF3 tickers: {len(sf3)}, DAILY tickers: {len(daily)}")

    # 모든 SF3 분기 날짜 수집
    all_quarters: set[str] = set()
    for rec in sf3.values():
        all_quarters.update(rec["quarters"])
    all_quarters = sorted(all_quarters)
    print(f"분기 날짜 수: {len(all_quarters)}  ({all_quarters[0]} ~ {all_quarters[-1]})")

    # 결과 저장: {ticker: {quarters: [], residuals: []}}
    result: dict[str, dict] = {}

    for q_date in all_quarters:
        # 45일 딜레이 보정: 실제 사용 시점 기준으로 DAILY mcap 조회
        q_ts   = pd.Timestamp(q_date)
        use_ts = q_ts + pd.Timedelta(days=FILING_LAG_DAYS)
        use_str = str(use_ts.date())

        # 이 분기에 데이터 있는 종목 수집
        rows: list[tuple[str, float, float]] = []  # (ticker, inst_pct, log_mcap)
        for ticker, rec in sf3.items():
            # SF3: 분기 기준 (딜레이 없이 q_date로 조회)
            inst_val = get_sf3_level(sf3, ticker, q_date)
            if inst_val is None or inst_val <= 0:
                continue
            # DAILY: 딜레이 적용 후 시점
            mcap = get_daily_mcap(daily, ticker, use_str)
            if mcap is None or mcap <= 0:
                continue
            pct = inst_val / (mcap * 1_000_000)
            if pct <= 0 or pct > 5.0:   # 500% 초과는 데이터 오류
                continue
            rows.append((ticker, pct, math.log(mcap)))

        if len(rows) < 20:
            continue

        tickers_q = [r[0] for r in rows]
        pcts      = np.array([r[1] for r in rows])
        log_mcaps = np.array([r[2] for r in rows])

        # 크로스섹셔널 OLS 잔차
        residuals = ols_residuals(log_mcaps, pcts)

        # 결과 누적
        for i, ticker in enumerate(tickers_q):
            if ticker not in result:
                result[ticker] = {"quarters": [], "residuals": []}
            result[ticker]["quarters"].append(use_str)   # 실제 사용 시점 저장
            result[ticker]["residuals"].append(float(residuals[i]))

        if all_quarters.index(q_date) % 10 == 0:
            print(f"  {q_date} -> 사용시점 {use_str}  종목수={len(rows)}  잔차 범위=[{residuals.min():.3f}, {residuals.max():.3f}]")

    print(f"\n완료: {len(result)}개 종목 잔차 계산")

    payload = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "filing_lag_days": FILING_LAG_DAYS,
        "description":     "inst_ownership_pct OLS residual controlling for log(market_cap)",
        "lookup":          result,
    }
    pickle.dump(payload, open(OUT_FILE, "wb"))
    print(f"저장: {OUT_FILE}  ({OUT_FILE.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
