#!/usr/bin/env python3
"""
build_sector_neutral_history.py
섹터 + 사이즈 이중 중립 기관 크라우딩 신호 사전 계산

각 분기 말 시점에 크로스섹셔널 회귀:
    inst_ownership_pct ~ log(market_cap) + Σ γ_s * sector_dummy_s
잔차 = 사이즈·섹터 효과를 동시에 제거한 순수 기관 소유 편차

기존 inst_neutral_history.pkl(사이즈만 통제)과 비교해
섹터 내에서 유독 기관이 적게/많이 보유하는 종목을 더 정확히 식별함.

결과: data/sector_neutral_history.pkl
  {ticker: {"quarters": ["2014-xx-xx", ...], "residuals": [float, ...]}}
"""
from __future__ import annotations

import bisect
import math
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path

import nasdaqdatalink
import numpy as np
import pandas as pd

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SF3_FILE   = DATA_DIR / "sf3_history.pkl"
DAILY_FILE = DATA_DIR / "daily_history.pkl"
OUT_FILE   = DATA_DIR / "sector_neutral_history.pkl"

FILING_LAG_DAYS = 45

nasdaqdatalink.ApiConfig.api_key = os.environ.get("NASDAQ_DATA_LINK_KEY", "NHr5446JR6sysBKtTBp1")


def load_sf3() -> dict:
    return pickle.load(open(SF3_FILE, "rb")).get("lookup", {})


def load_daily() -> dict:
    return pickle.load(open(DAILY_FILE, "rb")).get("lookup", {})


def load_sector_map() -> dict[str, str]:
    """SHARADAR/TICKERS에서 ticker→sector 매핑 다운로드 (정적)"""
    print("섹터 매핑 다운로드 중 (SHARADAR/TICKERS)...")
    df = nasdaqdatalink.get_table(
        "SHARADAR/TICKERS",
        qopts={"columns": ["ticker", "sector"]},
        paginate=True,
    )
    # 중복 ticker 있을 경우 첫 번째 non-null 섹터 사용
    df = df.dropna(subset=["sector"])
    df = df.drop_duplicates(subset=["ticker"], keep="first")
    mapping = dict(zip(df["ticker"], df["sector"]))
    sectors = sorted(set(mapping.values()))
    print(f"  tickers with sector: {len(mapping)}  sectors: {len(sectors)}")
    return mapping, sectors


def get_daily_mcap(daily_lookup: dict, ticker: str, as_of_str: str) -> float | None:
    rec = daily_lookup.get(ticker)
    if not rec:
        return None
    dates = rec["dates"]
    mcaps = rec["marketcap"]
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


def ols_residuals_with_sector(
    log_mcaps: np.ndarray,
    pcts: np.ndarray,
    sector_codes: np.ndarray,
    n_sectors: int,
) -> np.ndarray:
    """OLS: inst_pct ~ 1 + log(mcap) + Σ γ_s * sector_s  →  잔차 반환"""
    n = len(pcts)
    # 설계행렬: [intercept, log_mcap, sector_dummies (n_sectors-1)]
    # 첫 번째 섹터를 기준(reference)으로 제거 → 다중공선성 방지
    dummies = np.zeros((n, n_sectors - 1), dtype=float)
    for i, sc in enumerate(sector_codes):
        if sc > 0:  # sc==0 is reference sector
            dummies[i, sc - 1] = 1.0
    X = np.column_stack([np.ones(n), log_mcaps, dummies])
    try:
        beta = np.linalg.lstsq(X, pcts, rcond=None)[0]
        return pcts - X @ beta
    except Exception:
        # OLS 실패 시 섹터 평균 제거 fallback
        return pcts - pcts.mean()


def main():
    print("=== 섹터 + 사이즈 이중 중립 기관 크라우딩 신호 계산 ===")

    sector_map, all_sectors = load_sector_map()
    sector_to_code = {s: i for i, s in enumerate(all_sectors)}

    sf3   = load_sf3()
    daily = load_daily()
    print(f"SF3 tickers: {len(sf3)}, DAILY tickers: {len(daily)}")

    all_quarters: set[str] = set()
    for rec in sf3.values():
        all_quarters.update(rec["quarters"])
    all_quarters = sorted(all_quarters)
    print(f"분기 날짜 수: {len(all_quarters)}  ({all_quarters[0]} ~ {all_quarters[-1]})")

    result: dict[str, dict] = {}

    for q_date in all_quarters:
        q_ts    = pd.Timestamp(q_date)
        use_ts  = q_ts + pd.Timedelta(days=FILING_LAG_DAYS)
        use_str = str(use_ts.date())

        rows: list[tuple[str, float, float, int]] = []  # (ticker, inst_pct, log_mcap, sector_code)
        for ticker, rec in sf3.items():
            inst_val = get_sf3_level(sf3, ticker, q_date)
            if inst_val is None or inst_val <= 0:
                continue
            mcap = get_daily_mcap(daily, ticker, use_str)
            if mcap is None or mcap <= 0:
                continue
            pct = inst_val / (mcap * 1_000_000)
            if pct <= 0 or pct > 5.0:
                continue
            sec = sector_map.get(ticker, None)
            sector_code = sector_to_code.get(sec, 0) if sec else 0
            rows.append((ticker, pct, math.log(mcap), sector_code))

        if len(rows) < 30:
            continue

        tickers_q    = [r[0] for r in rows]
        pcts         = np.array([r[1] for r in rows])
        log_mcaps    = np.array([r[2] for r in rows])
        sector_codes = np.array([r[3] for r in rows])

        residuals = ols_residuals_with_sector(log_mcaps, pcts, sector_codes, len(all_sectors))

        for i, ticker in enumerate(tickers_q):
            if ticker not in result:
                result[ticker] = {"quarters": [], "residuals": []}
            result[ticker]["quarters"].append(use_str)
            result[ticker]["residuals"].append(float(residuals[i]))

        if all_quarters.index(q_date) % 10 == 0:
            print(
                f"  {q_date} -> 사용시점 {use_str}  종목수={len(rows)}"
                f"  잔차=[{residuals.min():.3f}, {residuals.max():.3f}]"
            )

    print(f"\n완료: {len(result)}개 종목 잔차 계산")

    payload = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "filing_lag_days": FILING_LAG_DAYS,
        "description":     "inst_ownership_pct OLS residual controlling for log(market_cap) + sector dummies",
        "sectors":         all_sectors,
        "lookup":          result,
    }
    pickle.dump(payload, open(OUT_FILE, "wb"))
    print(f"저장: {OUT_FILE}  ({OUT_FILE.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
