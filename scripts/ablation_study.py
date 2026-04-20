from __future__ import annotations

import json
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any

# ==========================================
# [필수 임포트] 스코어링 및 가중치 모듈
# ==========================================
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
    ABS_MOM_63D_MIN,
    ABS_MOM_252D_MIN,
    SECTOR_MAX_NAMES,
    annualized_volatility, 
)

# ==========================================
# 1. 경로 설정 및 공통 상수
# ==========================================
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

CURRENT_UNIVERSE_FILES = [DATA_DIR / "sp500_current_wiki.json", DATA_DIR / "sp400_current_wiki.json", DATA_DIR / "sp600_current_wiki.json"]
MEMBERSHIP_EVENTS_FILES = [DATA_DIR / "sp500_membership_events.json", DATA_DIR / "sp400_membership_events.json", DATA_DIR / "sp600_membership_events.json"]

BACKTEST_YEARS = 10
BENCHMARK = "SPY"
VIX_TICKER = "^VIX"

TRANSACTION_COST = 0.0015
RISK_FREE_RATE = 0.04
DAILY_RF = (1 + RISK_FREE_RATE) ** (1/252) - 1
MIN_HISTORY = 220

REGIME_MA_WINDOW = 200
REGIME_MOM_WINDOW = 63
REGIME_BUFFER = 0.005
REGIME_CONFIRM_DAYS = 2
VIX_CRASH_THRESHOLD = 40.0

RISK_ON_EXPOSURE = 1.00
MID_EXPOSURE = 0.85
RISK_OFF_EXPOSURE = 0.40
TOP_N = 15

# [수정 1] 리포트 용어 순화: OOS 대신 '최근 변동성 구간 (Stress Test)' 명칭 사용
STRESS_TEST_START_DATE = "2021-01-01"

# ==========================================
# 2. 어블레이션 시나리오 구성
# ==========================================
SCENARIOS = {
    "1. V1.0 (Master - 전체 로직)": {"BUF": 25, "WGT": PORTFOLIO_WEIGHT_METHOD, "SEC": 3, "DEF": ["TAIL", "DBMF"], "REG": "daily"},
    "2. Equal Weight (역가중 제외)": {"BUF": 25, "WGT": "equal", "SEC": 3, "DEF": ["TAIL", "DBMF"], "REG": "daily"},
    "3. No Sector Cap (섹터 쏠림 허용)": {"BUF": 25, "WGT": PORTFOLIO_WEIGHT_METHOD, "SEC": 999, "DEF": ["TAIL", "DBMF"], "REG": "daily"},
    "4. Cash in Risk-Off (현금 100%)": {"BUF": 25, "WGT": PORTFOLIO_WEIGHT_METHOD, "SEC": 3, "DEF": [], "REG": "daily"},
    "5. Monthly Regime (일일 방어 제거)": {"BUF": 25, "WGT": PORTFOLIO_WEIGHT_METHOD, "SEC": 3, "DEF": ["TAIL", "DBMF"], "REG": "monthly"}
}

# (데이터 로딩 함수 생략 - 이전과 100% 동일)
def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists(): return default
    with open(path, "r", encoding="utf-8") as f: return json.load(f)
def normalize_ticker(t: str) -> str: return str(t).strip().upper().replace(".", "-")
def get_start_date() -> str: return (datetime.now(timezone.utc) - timedelta(days=365 * BACKTEST_YEARS + 320)).strftime("%Y-%m-%d")
def load_current_universe() -> dict[str, dict[str, Any]]:
    out = {}
    for file in CURRENT_UNIVERSE_FILES:
        for item in load_json(file, default={}).get("items", []):
            t = normalize_ticker(item.get("ticker", "")); out[t] = {"ticker": t, "sector": item.get("sector", "Unknown") or "Unknown"} if t else out.get(t, {})
    return out
def load_membership_events() -> list[dict[str, Any]]:
    cleaned = []
    for file in MEMBERSHIP_EVENTS_FILES:
        for ev in load_json(file, default={}).get("events", []):
            d = str(ev.get("date", "")).strip()
            if d: cleaned.append({"date": d, "added": [normalize_ticker(x) for x in ev.get("added", []) if str(x).strip()], "removed": [normalize_ticker(x) for x in ev.get("removed", []) if str(x).strip()]})
    cleaned.sort(key=lambda x: x["date"]); return cleaned
def reconstruct_membership_as_of(as_of: pd.Timestamp, univ: dict, events: list) -> set[str]:
    members = set(univ.keys())
    for ev in sorted(events, key=lambda x: x["date"], reverse=True):
        if pd.to_datetime(ev["date"]) > as_of:
            for t in ev.get("added", []): members.discard(t)
            for t in ev.get("removed", []): members.add(t)
    return members
def monthly_rebalance_dates(dates: pd.Index) -> list[pd.Timestamp]:
    out, last = [], None
    for d in dates:
        key = d.strftime("%Y-%m")
        if key != last: out.append(d); last = key
    return out

# ==========================================
# 3. 동적 포트폴리오 픽 로직
# ==========================================
def pick_portfolio_dynamic(rows, current_holdings, dynamic_floor, config):
    ranked = [r for r in rows if r.final_score_100 is not None]
    ranked.sort(key=lambda x: x.final_score_100, reverse=True)
    filtered = [r for r in ranked if r.ret63d is not None and r.ret252d is not None and r.ret63d > ABS_MOM_63D_MIN and r.ret252d > ABS_MOM_252D_MIN]
    if not filtered: filtered = ranked

    out, sector_counts = [], {}
    def add_to_out(r):
        sec = r.sector or "Unknown"
        if config["SEC"] <= 0 or sector_counts.get(sec, 0) < config["SEC"]:
            out.append(r); sector_counts[sec] = sector_counts.get(sec, 0) + 1; return True
        return False

    held_eligible = [r for i, r in enumerate(filtered) if r.ticker in current_holdings and (i + 1) <= config["BUF"]]
    for r in held_eligible:
        if len(out) < TOP_N: add_to_out(r)
    for r in filtered:
        if len(out) >= TOP_N: break
        if any(ex.ticker == r.ticker for ex in out): continue
        add_to_out(r)

    if not out: return []
    if config["WGT"] == "equal":
        for r in out: r.portfolio_weight = 1.0 / len(out)
        return out
    else:
        return compute_portfolio_weights(out, method=config["WGT"], alpha_score=WEIGHT_ALPHA_SCORE, min_w=MIN_WEIGHT, max_w=MAX_WEIGHT, vol_fallback=VOL_FALLBACK, vol_floor=VOL_WEIGHT_FLOOR, dynamic_vol_floor=dynamic_floor)

# ==========================================
# 4. 성과 지표 계산 엔진 (정석 Sharpe)
# ==========================================
def calc_metrics(df_slice):
    if len(df_slice) < 10: return "N/A", "N/A", "N/A"
    
    years = len(df_slice) / 252
    equity = (1 + df_slice['ret']).cumprod()
    
    cagr = (equity.iloc[-1] ** (1/years) - 1) if years > 0 else 0
    mdd = (equity / equity.cummax() - 1).min()
    
    # [수정 2] 산술 평균 초과수익 기반 정석 Sharpe 공식 적용
    ann_excess_ret = (df_slice['ret'] - DAILY_RF).mean() * 252
    vol = df_slice['ret'].std() * np.sqrt(252)
    sharpe = ann_excess_ret / vol if vol > 0 else 0
    
    return f"{cagr:.2%}", f"{sharpe:.2f}", f"{mdd:.2%}"

# ==========================================
# 5. 메인 어블레이션 실행 엔진
# ==========================================
def run_ablation_study():
    print("\n" + "="*90)
    print("🔬 [V3.0] 실전 투입용 퀀트 검증 엔진 (Benchmark 비교 & 결측치 방어 & 정석 Sharpe)")
    print("="*90)
    
    current_universe = load_current_universe()
    membership_events = load_membership_events()
    tickers = list(current_universe.keys())
    
    print("▶ 1/3: 가격 데이터 다운로드 중...")
    all_tickers = sorted(set(tickers + [BENCHMARK, VIX_TICKER, "TAIL", "DBMF"]))
    df_dl = yf.download(tickers=all_tickers, start=get_start_date(), auto_adjust=True, progress=False, group_by="ticker", threads=True)
    
    price_map = {}
    if isinstance(df_dl.columns, pd.MultiIndex):
        level0 = set(df_dl.columns.get_level_values(0))
        for t in all_tickers:
            if t in level0 and "Close" in df_dl[t]:
                s = df_dl[t]["Close"].dropna()
                if len(s) >= MIN_HISTORY: price_map[t] = s
    
    prices_df = pd.DataFrame(price_map)
    returns_df = prices_df.pct_change().fillna(0.0)
    
    benchmark_series = price_map[BENCHMARK]
    vix_series = price_map.get(VIX_TICKER)
    trading_dates = benchmark_series.index
    stock_rebalance_dates = set(monthly_rebalance_dates(trading_dates))

    print("▶ 2/3: 거시경제(Regime) 시그널 사전 계산 중...")
    df_bench = benchmark_series.to_frame(name='price')
    df_bench['ma200'] = df_bench['price'].rolling(REGIME_MA_WINDOW).mean()
    df_bench['mom63'] = df_bench['price'].pct_change(REGIME_MOM_WINDOW)
    df_bench['vix'] = vix_series.ffill() if vix_series is not None else np.nan
    
    cond_on = (df_bench['price'] >= df_bench['ma200'] * (1+REGIME_BUFFER)) & (df_bench['mom63'] >= 0)
    cond_off = (df_bench['price'] <= df_bench['ma200'] * (1-REGIME_BUFFER)) & (df_bench['mom63'] < 0)
    
    cand_values = np.full(len(df_bench), 'mid', dtype=object)
    cand_values[cond_on] = 'risk_on'; cand_values[cond_off] = 'risk_off'
    
    precomputed_exposures = {}
    prev = 'risk_on'
    for i, date in enumerate(trading_dates):
        cand = cand_values[i]
        if cand in ["risk_on", "risk_off"] and i >= REGIME_CONFIRM_DAYS - 1:
            recent = cand_values[i - REGIME_CONFIRM_DAYS + 1 : i + 1]
            regime = cand if all(x == cand for x in recent) else prev
        elif cand in ["risk_on", "risk_off"]: regime = cand
        else: regime = prev
        prev = regime
        
        is_crash = df_bench['price'].iloc[i] < df_bench['ma200'].iloc[i] and df_bench['vix'].iloc[i] > VIX_CRASH_THRESHOLD
        precomputed_exposures[date] = RISK_OFF_EXPOSURE if is_crash else (RISK_ON_EXPOSURE if regime == 'risk_on' else (RISK_OFF_EXPOSURE if regime == 'risk_off' else MID_EXPOSURE))

    print("▶ 3/3: 월간 종목 스코어링 캐싱 중 (10년치 연산)...")
    scored_cache = {}
    for date in sorted(list(stock_rebalance_dates)):
        sliced_df = prices_df[prices_df.index <= date]
        valid_cols = sliced_df.columns[sliced_df.count() > 60]
        sliced = {col: sliced_df[col].dropna() for col in valid_cols}
        
        membership = reconstruct_membership_as_of(date, current_universe, membership_events)
        eligible_prices = {t: s for t, s in sliced.items() if t in membership}
        if BENCHMARK in sliced: eligible_prices[BENCHMARK] = sliced[BENCHMARK]
        
        by_ticker = {t: current_universe.get(t, {"ticker": t, "sector": "Unknown"}) for t in eligible_prices.keys()}
        metrics = build_metrics_for_group(list(eligible_prices.keys()), by_ticker, eligible_prices)
        scored_cache[date] = {"scored": score_group(metrics, quality_score_map=None), "floor": max(VOL_WEIGHT_FLOOR, annualized_volatility(sliced.get(BENCHMARK), 20) if sliced.get(BENCHMARK) is not None else VOL_WEIGHT_FLOOR)}

    print("\n🚀 병렬 백테스트 및 검증 시작...\n")
    results = []

    # [수정 3] SPY Benchmark 강제 추가 (리포트의 기준선 제공)
    bench_history = [{"date": d, "ret": returns_df.loc[d, BENCHMARK] if BENCHMARK in returns_df.columns else 0.0} for d in trading_dates]
    bench_df = pd.DataFrame(bench_history).set_index("date")
    b_is_cagr, b_is_shp, b_is_mdd = calc_metrics(bench_df[bench_df.index < STRESS_TEST_START_DATE])
    b_oos_cagr, b_oos_shp, b_oos_mdd = calc_metrics(bench_df[bench_df.index >= STRESS_TEST_START_DATE])
    _, _, b_full_mdd = calc_metrics(bench_df)
    results.append({"시나리오": "▶ 0. Benchmark (단순 SPY 보유)", "IS CAGR": b_is_cagr, "IS Sharpe": b_is_shp, "OOS CAGR": b_oos_cagr, "OOS Sharpe": b_oos_shp, "Full MDD": b_full_mdd, "회전율": "0.0회"})

    # 시나리오 루프
    for name, config in SCENARIOS.items():
        equity, total_turnover = 1.0, 0.0
        current_holdings, target_holdings = {}, {}
        current_exp, target_exp = RISK_ON_EXPOSURE, RISK_ON_EXPOSURE
        history = []
        
        for date in trading_dates:
            if target_holdings != current_holdings:
                turnover = sum(abs(target_holdings.get(t, 0) - current_holdings.get(t, 0)) for t in set(current_holdings.keys()) | set(target_holdings.keys()))
                one_way_turnover = turnover / 2.0
                total_turnover += one_way_turnover
                equity *= (1 - (one_way_turnover * TRANSACTION_COST))
                current_holdings = target_holdings
            
            daily_ret = 0.0
            actual_invested_weight = 0.0
            
            if current_holdings:
                row_rets = returns_df.loc[date]
                for t, w in current_holdings.items():
                    # [수정 4] 방어 자산(TAIL/DBMF) 가격 결측치 자동 현금(0%) 처리 로직
                    if pd.isna(row_rets.get(t)) or row_rets.get(t, 0.0) == 0.0 and t in ["TAIL", "DBMF"]:
                        pass # 가격이 없으면 수익률 0%, actual_invested_weight에도 더하지 않음 (자동 현금화)
                    else:
                        daily_ret += w * row_rets.get(t, 0.0)
                        actual_invested_weight += w
            
            # 미투자 잔고(현금)에 대해 연 4% 일할 이자 지급
            uninvested_weight = max(0.0, 1.0 - actual_invested_weight)
            daily_ret += uninvested_weight * DAILY_RF
                
            equity *= (1 + daily_ret)
            history.append({"date": date, "ret": daily_ret})
            
            stock_changed = False
            raw_stock_map = {k: v for k, v in current_holdings.items() if k not in ["TAIL", "DBMF"]}
            if raw_stock_map:
                s = sum(raw_stock_map.values())
                raw_stock_map = {k: v/s for k, v in raw_stock_map.items()} if s > 0 else {}
                
            if date in stock_rebalance_dates:
                cache = scored_cache[date]
                selected = pick_portfolio_dynamic(cache["scored"], set(raw_stock_map.keys()), cache["floor"], config)
                raw_stock_map = {r.ticker: float(r.portfolio_weight) for r in selected if r.portfolio_weight > 0}
                if raw_stock_map: raw_stock_map = {k: v/sum(raw_stock_map.values()) for k, v in raw_stock_map.items()}
                stock_changed = True

            exp_changed = False
            if config["REG"] == "daily" or (config["REG"] == "monthly" and date in stock_rebalance_dates):
                if target_exp != precomputed_exposures[date]:
                    target_exp = precomputed_exposures[date]
                    exp_changed = True
                
            if stock_changed or exp_changed:
                new_target = {}
                for t, w in raw_stock_map.items(): new_target[t] = w * target_exp
                def_tickers = config["DEF"]
                if def_tickers and target_exp < 1.0:
                    def_map = {"TAIL": 0.3, "DBMF": 0.7} if set(def_tickers) == {"TAIL", "DBMF"} else {t: 1.0/len(def_tickers) for t in def_tickers}
                    for t, w in def_map.items(): new_target[t] = w * (1.0 - target_exp)
                target_holdings = new_target

        res_df = pd.DataFrame(history).set_index("date")
        is_cagr, is_shp, _ = calc_metrics(res_df[res_df.index < STRESS_TEST_START_DATE])
        oos_cagr, oos_shp, _ = calc_metrics(res_df[res_df.index >= STRESS_TEST_START_DATE])
        _, _, full_mdd = calc_metrics(res_df)
        annual_turnover = total_turnover / (len(res_df) / 252) if len(res_df) > 0 else 0
        
        results.append({"시나리오": name, "IS CAGR": is_cagr, "IS Sharpe": is_shp, "OOS CAGR": oos_cagr, "OOS Sharpe": oos_shp, "Full MDD": full_mdd, "회전율": f"{annual_turnover:.1f}회"})
        print(f"✔️ {name} 테스트 완료!")

    # ==========================================
    # 6. 마크다운 표 출력
    # ==========================================
    print("\n\n📊 [Ablation Study 리포트 (Benchmark 비교 포함)]")
    print("| 시나리오 | IS CAGR (~2020) | IS Sharpe | OOS CAGR (2021~) | OOS Sharpe | Full MDD | 연간 회전율 |")
    print("|:---|:---:|:---:|:---:|:---:|:---:|:---:|")
    for r in results:
        print(f"| {r['시나리오']} | {r['IS CAGR']} | {r['IS Sharpe']} | **{r['OOS CAGR']}** | **{r['OOS Sharpe']}** | {r['Full MDD']} | {r['회전율']} |")

if __name__ == "__main__":
    run_ablation_study()