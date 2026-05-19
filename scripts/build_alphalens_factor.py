#!/usr/bin/env python3
"""
build_alphalens_factor.py — AE_INPUT_FEATURES 16개 팩터를 alphalens 형식으로 분석

alphalens가 필요로 하는 형식:
  factor : pd.Series (MultiIndex: date × asset) — 팩터 신호값
  prices : pd.DataFrame (index=date, columns=tickers) — 일별 가격

출력:
  data/alphalens/<factor_name>_tearsheet.html  — 팩터별 tear sheet
  data/alphalens/factor_summary.json           — IC 요약 (포털용)

실행:
  python scripts/build_alphalens_factor.py
  python scripts/build_alphalens_factor.py --factor mom12_1   # 단일 팩터
  python scripts/build_alphalens_factor.py --periods 1,5,21   # 선행수익률 기간
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

OUT_DIR = ROOT / "data" / "alphalens"
OUT_DIR.mkdir(parents=True, exist_ok=True)  # B-1: parents=True for CI/clean-clone safety

# IS 기간 — run_backtest_new.py와 단일 source of truth (B-3)
from run_backtest_new import IS_START, IS_END

# 기본 분석할 팩터 목록 (AE_INPUT_FEATURES 기준)
DEFAULT_FACTORS = [
    "mom12_1", "mom9_1", "mom6_1", "mom3_1", "mom1",
    "rs_spy_12m", "rs_spy_6m", "rs_spy_3m",
    "evebit", "evebitda", "pb", "pe", "ps",
    "inst_crowding_neutral", "institutional", "insider",
]


# ─────────────────────────────────────────────────────────────
# 1. 팩터 시계열 빌드
# ─────────────────────────────────────────────────────────────

def build_factor_series(engine, factor_name: str, rebal_dates) -> pd.Series:
    """
    월별 리밸런싱 날짜 × 전 유니버스 종목에 대해 팩터 값을 계산한다.
    반환: MultiIndex(date, asset) Series
    """
    from run_backtest_new import (
        _monthly_dates, _reconstruct_universe, MIN_HISTORY, FACTOR_UNIVERSE,
    )

    cfg  = FACTOR_UNIVERSE.get(factor_name, {})
    rank = cfg.get("rank", "high_good")

    records = []

    for rebal_date in rebal_dates:
        members = _reconstruct_universe(rebal_date, engine.universe, engine.events)
        for ticker in members:
            ps    = engine.price_map.get(ticker)
            bench = engine.price_map.get("SPY")
            if ps is None or bench is None:
                continue
            ps_cut = ps[ps.index <= rebal_date]
            if len(ps_cut) < MIN_HISTORY:
                continue
            val = engine._raw_signal(ticker, factor_name, ps_cut, bench, rebal_date)
            if val is None or not np.isfinite(val):
                continue
            # low_good 팩터는 부호 반전해서 "높을수록 좋음"으로 통일
            if rank == "low_good":
                val = -val
            records.append((rebal_date, ticker, val))

    if not records:
        return pd.Series(dtype=float)

    idx = pd.MultiIndex.from_tuples(
        [(d, t) for d, t, _ in records], names=["date", "asset"]
    )
    values = [v for _, _, v in records]
    return pd.Series(values, index=idx, name=factor_name)


# ─────────────────────────────────────────────────────────────
# 2. 가격 DataFrame 빌드
# ─────────────────────────────────────────────────────────────

def build_price_df(engine, tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """sep_prices.pkl 에서 tickers × date DataFrame 구성."""
    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end) + pd.Timedelta(days=30)  # 선행수익률용 여유

    frames = {}
    for t in tickers:
        ps = engine.price_map.get(t)
        if ps is None:
            continue
        ps_cut = ps[(ps.index >= start_ts) & (ps.index <= end_ts)]
        if len(ps_cut) > 20:
            frames[t] = ps_cut

    if not frames:
        return pd.DataFrame()

    df = pd.DataFrame(frames)
    df.index = pd.DatetimeIndex(df.index)
    df = df.sort_index()
    # reindex strips freq; assign bdays directly to preserve freq='B'
    bdays = pd.bdate_range(df.index.min(), df.index.max())
    df = df.reindex(bdays).ffill()
    df.index = bdays  # preserve freq='B' attribute
    return df


# ─────────────────────────────────────────────────────────────
# 3. 팩터 분석 실행
# ─────────────────────────────────────────────────────────────

def _compute_ic_manual(
    factor_series: pd.Series,
    price_df: pd.DataFrame,
    periods: list[int],
) -> dict[int, list[float]]:
    """
    Spearman IC를 직접 계산 (alphalens 주파수 이슈 우회).
    factor_series: MultiIndex(date, asset)
    price_df:      index=business-day DatetimeIndex, columns=tickers, freq='B'
    """
    from scipy.stats import spearmanr

    # date -> {ticker: factor_val}
    factor_by_date: dict = {}
    for (date, asset), val in factor_series.items():
        factor_by_date.setdefault(date, {})[asset] = val

    price_idx = price_df.index  # DatetimeIndex with freq='B'

    ic_by_period: dict[int, list[float]] = {p: [] for p in periods}

    for rebal_date in sorted(factor_by_date.keys()):
        fvals = factor_by_date[rebal_date]

        # rebal_date가 price_df 인덱스에 없으면 가장 가까운 날 사용
        loc0 = price_idx.searchsorted(rebal_date)
        if loc0 >= len(price_idx):
            continue

        for period in periods:
            loc1 = loc0 + period
            if loc1 >= len(price_idx):
                continue

            p0_row = price_df.iloc[loc0]
            p1_row = price_df.iloc[loc1]

            fvec, rvec = [], []
            for ticker, fval in fvals.items():
                if ticker not in price_df.columns:
                    continue
                p0 = p0_row[ticker]
                p1 = p1_row[ticker]
                if pd.isna(p0) or pd.isna(p1) or p0 == 0:
                    continue
                fvec.append(fval)
                rvec.append((p1 - p0) / p0)

            if len(fvec) < 30:  # B-8: 통계적 의미 최소 30개
                continue

            ic, _ = spearmanr(fvec, rvec)
            if np.isfinite(ic):
                ic_by_period[period].append(float(ic))

    return ic_by_period


def _summarize_ic(ic_by_period: dict[int, list[float]], periods: list[int]) -> dict:
    summary: dict = {}
    for p in periods:
        vals = ic_by_period.get(p, [])
        if not vals:
            continue
        arr = np.array(vals)
        col = f"{p}D"
        summary[col] = {
            "mean_ic":  round(float(arr.mean()), 4),
            "std_ic":   round(float(arr.std()), 4),
            "ir":       round(float(arr.mean() / (arr.std() + 1e-8)), 3),
            "hit_rate": round(float((arr > 0).mean()), 3),
            "n":        int(len(arr)),
        }
        d = summary[col]
        print(f"  IC ({col}): mean={d['mean_ic']:+.4f}  "
              f"IR={d['ir']:+.3f}  hit={d['hit_rate']:.1%}  n={d['n']}")
    return summary


def analyze_factor(
    engine,
    factor_name: str,
    rebal_dates,
    price_df: pd.DataFrame,
    periods: list[int],
    save_html: bool = True,
) -> dict:
    """단일 팩터 alphalens 분석. alphalens 실패 시 수동 Spearman IC 사용."""
    print(f"\n[{factor_name}] 팩터 시계열 계산 중...")
    factor_series = build_factor_series(engine, factor_name, rebal_dates)

    if factor_series.empty:
        print(f"  [SKIP] {factor_name} — 유효 데이터 없음")
        return {}

    n_obs = len(factor_series)
    n_dates = factor_series.index.get_level_values("date").nunique()
    print(f"  관측치: {n_obs:,}개 ({n_dates}개 날짜)")

    tickers_in_factor = factor_series.index.get_level_values("asset").unique().tolist()
    prices_filtered   = price_df[
        [t for t in tickers_in_factor if t in price_df.columns]
    ]

    # ── 1차 시도: alphalens ──────────────────────────────────────
    factor_data = None
    ic_source   = "manual_spearman"  # B-4: 어느 경로로 계산됐는지 명시
    try:
        import alphalens
        factor_data = alphalens.utils.get_clean_factor_and_forward_returns(
            factor=factor_series,
            prices=prices_filtered,
            periods=periods,
            quantiles=5,
            max_loss=0.35,
        )
        ic_table = alphalens.performance.factor_information_coefficient(factor_data)
        summary: dict = {}
        for p in periods:
            col = f"{p}D"
            if col in ic_table.columns:
                ic_vals = ic_table[col].dropna()
                summary[col] = {
                    "mean_ic":  round(float(ic_vals.mean()), 4),
                    "std_ic":   round(float(ic_vals.std()), 4),
                    "ir":       round(float(ic_vals.mean() / (ic_vals.std() + 1e-8)), 3),
                    "hit_rate": round(float((ic_vals > 0).mean()), 3),
                    "n":        int(len(ic_vals)),
                }
                d = summary[col]
                print(f"  IC ({col}): mean={d['mean_ic']:+.4f}  "
                      f"IR={d['ir']:+.3f}  hit={d['hit_rate']:.1%}")
        ic_source = "alphalens"
    except Exception as e:
        print(f"  [alphalens 실패 → 수동 Spearman] {e}")
        factor_data = None

    # ── 2차 폴백: 수동 Spearman IC ─────────────────────────────
    if factor_data is None:
        ic_by_period = _compute_ic_manual(factor_series, prices_filtered, periods)
        summary = _summarize_ic(ic_by_period, periods)

    if not summary:
        print(f"  [SKIP] 유효 IC 없음")
        return {}

    summary["ic_source"] = ic_source  # B-4: JSON에 계산 경로 기록

    # ── tear sheet (alphalens 성공 시에만) ──────────────────────
    if save_html and factor_data is not None:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            from alphalens.tears import create_full_tear_sheet

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                create_full_tear_sheet(factor_data, long_short=True)

            png_path = OUT_DIR / f"{factor_name}_tearsheet.png"
            plt.savefig(png_path, bbox_inches="tight", dpi=100)
            plt.close("all")
            print(f"  --> 저장: {png_path.name}")
        except Exception as e:
            print(f"  [WARN] tear sheet 저장 실패: {e}")

    return summary


# ─────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factor",  default=None, help="팩터 이름 (쉼표로 여러 개 지정 가능, 없으면 전체)")
    parser.add_argument("--periods", default="1,5,21", help="선행수익률 기간 (쉼표 구분)")
    parser.add_argument("--no-html", action="store_true", help="HTML 저장 안 함")
    args = parser.parse_args()

    periods = [int(p) for p in args.periods.split(",")]
    factors = [f.strip() for f in args.factor.split(",")] if args.factor else DEFAULT_FACTORS

    print("=== alphalens 팩터 분석 ===")
    print(f"IS 기간: {IS_START} ~ {IS_END}")
    print(f"분석 팩터: {factors}")
    print(f"선행수익률 기간: {periods}일\n")

    # 엔진 로드
    from run_backtest_new import BacktestEngine, _monthly_dates
    engine = BacktestEngine()
    engine.load_data()

    # IS 기간 월별 리밸런싱 날짜
    is_dates    = engine.trading_dates[
        (engine.trading_dates >= pd.Timestamp(IS_START)) &
        (engine.trading_dates <= pd.Timestamp(IS_END))
    ]
    rebal_dates = _monthly_dates(is_dates)
    print(f"리밸런싱 날짜: {len(rebal_dates)}개\n")

    # 전 유니버스 종목 가격 DataFrame (한 번만 빌드)
    all_tickers = list(engine.price_map.keys())
    print(f"가격 DataFrame 빌드 중 ({len(all_tickers)}개 종목)...")
    price_df = build_price_df(engine, all_tickers, IS_START, IS_END)
    print(f"  → {price_df.shape[1]}개 종목 × {len(price_df)}일\n")

    # 팩터별 분석
    # 기존 요약 로드 (단일 팩터 실행 시 기존 결과 보존)
    summary_path = OUT_DIR / "factor_summary.json"
    if summary_path.exists():
        with open(summary_path, "r", encoding="utf-8") as f:
            all_summary = json.load(f)
    else:
        all_summary = {}

    for factor_name in factors:
        result = analyze_factor(
            engine, factor_name, rebal_dates, price_df, periods,
            save_html=not args.no_html,
        )
        if result:
            all_summary[factor_name] = result

    # 요약 JSON 저장
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(all_summary, f, indent=2, ensure_ascii=False)
    print(f"\n[저장] 팩터 요약 → {summary_path}")

    # 콘솔 요약 테이블
    print(f"\n{'팩터':<22} {'IC(21D)':>9} {'IR(21D)':>8} {'hit%':>7}")
    print("=" * 52)
    for fname, res in sorted(all_summary.items(),
                              key=lambda x: abs(x[1].get("21D", {}).get("mean_ic", 0)),
                              reverse=True):
        d = res.get("21D", {})
        if d:
            print(f"  {fname:<20} {d['mean_ic']:>+9.4f} {d['ir']:>8.3f} {d['hit_rate']:>6.1%}")

    print("\n완료.")


if __name__ == "__main__":
    main()
