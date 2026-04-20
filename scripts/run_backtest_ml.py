from __future__ import annotations

import json
import logging
import math
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta

logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 파라미터 및 유틸리티
# ==========================================
from build_score_snapshot import (
    compute_portfolio_weights, 
    annualized_volatility,
    PORTFOLIO_WEIGHT_METHOD, MIN_WEIGHT, MAX_WEIGHT,
    VOL_FALLBACK, VOL_WEIGHT_FLOOR,
    TOP_N, SECTOR_MAX_NAMES
)

REGIME_MA_WINDOW = 200
REGIME_MOM_WINDOW = 63
VIX_CRASH_THRESHOLD = 40.0
RISK_ON_EXPOSURE = 1.00
MID_EXPOSURE = 0.85
RISK_OFF_EXPOSURE = 0.40
HOLD_BUFFER_N = 25 

from build_score_ml import build_metrics_ml, score_group_ml, load_ml_model

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

CURRENT_UNIVERSE_FILES = [
    DATA_DIR / "sp500_current_wiki.json", 
    DATA_DIR / "sp400_current_wiki.json", 
    DATA_DIR / "sp600_current_wiki.json"
]

# 🌟 타임머신을 위한 이벤트 파일 추가
MEMBERSHIP_FILES = [
    DATA_DIR / "sp500_membership_events.json", 
    DATA_DIR / "sp400_membership_events.json", 
    DATA_DIR / "sp600_membership_events.json"
]

OUT_REGIME = DATA_DIR / "equity_curve_ml_regime.json"
OUT_CURVE = DATA_DIR / "equity_curve_ml.json" 

START_DATE = "2015-01-01"
END_DATE = datetime.today().strftime('%Y-%m-%d')

# ==========================================
# 2. 헬퍼 함수 (Point-In-Time 추가)
# ==========================================
def load_json(path: Path, default: dict = None) -> dict:
    if not path.exists(): return default or {}
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def load_master_universe() -> dict:
    """현재 종목 및 과거 모든 종목의 섹터 매핑을 최대한 긁어옵니다."""
    out = {}
    for file in CURRENT_UNIVERSE_FILES + [DATA_DIR / "universe.json"]:
        if not file.exists(): continue
        data = load_json(file)
        items = data.get("items", []) if isinstance(data, dict) else data if isinstance(data, list) else []
        for item in items:
            if isinstance(item, dict):
                t = str(item.get("ticker", "")).strip().upper().replace(".", "-")
                if t and t not in out:
                    out[t] = {"sector": item.get("sector", "Unknown") or "Unknown"}
            elif isinstance(item, str):
                t = item.strip().upper().replace(".", "-")
                if t and t not in out: out[t] = {"sector": "Unknown"}
    return out

def get_historical_universe(target_date_str: str, membership_data: list[dict]) -> set:
    """특정 과거 시점에 존재했던 진짜 S&P 1500 명단을 추출합니다."""
    target_date = pd.to_datetime(target_date_str)
    pit_universe = set()
    
    for data in membership_data:
        current_tickers = set(data.get("current_tickers", []))
        events = []
        if isinstance(data, list): events = data
        elif "events" in data: events = data["events"]
        elif isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict) and "date" in val[0]:
                    events = val
                    break

        events.sort(key=lambda x: x.get("date", "1900-01-01"), reverse=True)
        
        for event in events:
            event_date = pd.to_datetime(event.get("date"))
            if event_date > target_date:
                current_tickers.difference_update(event.get("added", []))
                current_tickers.update(event.get("removed", []))
                
        pit_universe.update(current_tickers)
        
    return pit_universe

def calc_metrics(equity_series: pd.Series, years_back: int = None) -> dict:
    if years_back:
        cutoff_date = equity_series.index[-1] - pd.DateOffset(years=years_back)
        eq = equity_series.loc[cutoff_date:]
        if len(eq) == 0: return None
        eq = eq / eq.iloc[0] 
        actual_years = years_back
    else:
        eq = equity_series
        actual_years = (eq.index[-1] - eq.index[0]).days / 365.25

    if len(eq) < 2: return None

    cum_ret = eq.iloc[-1] / eq.iloc[0] - 1
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / actual_years) - 1 if actual_years > 0 else 0
    
    daily_rets = eq.pct_change().dropna()
    vol = daily_rets.std() * math.sqrt(252)
    sharpe = (daily_rets.mean() * 252) / vol if vol > 0 else 0
    
    drawdown = eq / eq.cummax() - 1
    mdd = drawdown.min()

    return {"cum_ret": cum_ret, "cagr": cagr, "sharpe": sharpe, "vol": vol, "mdd": mdd}

# ==========================================
# 3. 메인 시뮬레이터
# ==========================================
def run_ml_backtest():
    print("🤖 Point-In-Time 머신러닝 백테스트 가동 준비 중...")
    model = load_ml_model()
    
    univ = load_master_universe()
    membership_data = [load_json(f) for f in MEMBERSHIP_FILES if f.exists()]
    
    # 🌟 과거에 존재했던 모든 종목 풀(Pool) 수집
    all_possible_tickers = set(univ.keys())
    for data in membership_data:
        all_possible_tickers.update(data.get("current_tickers", []))
        events = []
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict) and "date" in val[0]:
                events = val; break
        for event in events:
            all_possible_tickers.update(event.get("added", []))
            all_possible_tickers.update(event.get("removed", []))
            
    tickers = list(all_possible_tickers)
    
    print(f"📥 1/3. 전체 주가 다운로드 중 (망한 기업 포함 총 {len(tickers)}개)...")
    start_dt = pd.to_datetime(START_DATE) - timedelta(days=400)
    
    spy_vix = yf.download(["SPY", "^VIX"], start=start_dt, end=END_DATE, progress=False)["Close"]
    prices_df = yf.download(tickers, start=start_dt, end=END_DATE, auto_adjust=True, progress=False)["Close"]
    defensive_df = yf.download(["TAIL", "DBMF"], start=start_dt, end=END_DATE, auto_adjust=True, progress=False)["Close"]
    
    all_prices = pd.concat([prices_df, defensive_df], axis=1)
    daily_returns = all_prices.pct_change()
    
    trading_days = spy_vix["SPY"].loc[START_DATE:END_DATE].dropna().index
    rebalance_dates = set(trading_days.to_series().groupby([trading_days.year, trading_days.month]).last().values)
    
    print(f"⚙️ 2/3. 총 {len(trading_days)}일간의 진짜 복리 시뮬레이션 시작...")
    
    portfolio_value = 1.0
    current_weights = {} 
    equity_curve = {}
    regime_log = {}
    
    for eval_date in trading_days:
        date_str = eval_date.strftime("%Y-%m-%d")
        
        # 💰 1. 일일 정산 (Mark-to-Market)
        if current_weights:
            daily_port_ret = 0.0
            for t, w in current_weights.items():
                if t in daily_returns.columns:
                    ret = daily_returns.loc[eval_date, t]
                    if not pd.isna(ret): 
                        daily_port_ret += w * ret
            portfolio_value *= (1 + daily_port_ret)
            
        equity_curve[date_str] = portfolio_value
        
        # 🔄 2. 월말 리밸런싱
        if eval_date in rebalance_dates:
            past_spy = spy_vix["SPY"].loc[:eval_date].dropna()
            past_vix = spy_vix["^VIX"].loc[:eval_date].dropna()
            if len(past_spy) < REGIME_MA_WINDOW: continue
                
            current_spy = past_spy.iloc[-1]
            ma200_val = past_spy.rolling(REGIME_MA_WINDOW).mean().iloc[-1]
            mom63_val = past_spy.pct_change(REGIME_MOM_WINDOW).iloc[-1]
            current_vix = past_vix.iloc[-1]
            
            is_crash = current_spy < ma200_val and current_vix > VIX_CRASH_THRESHOLD
            if is_crash:
                regime = 'risk_off (VIX CRASH)'; target_exp = RISK_OFF_EXPOSURE
            elif current_spy >= ma200_val * 1.005 and mom63_val >= 0:
                regime = 'risk_on'; target_exp = RISK_ON_EXPOSURE
            elif current_spy <= ma200_val * 0.995 and mom63_val < 0:
                regime = 'risk_off'; target_exp = RISK_OFF_EXPOSURE
            else:
                regime = 'mid'; target_exp = MID_EXPOSURE
                
            # 🌟 [핵심] 리밸런싱 당일, 진짜로 투자 가능했던 종목들만 뽑아옵니다 (타임머신)
            valid_pit_universe = get_historical_universe(date_str, membership_data)
            
            # 미래 데이터 스니핑 방지를 위해 현재 살아있는 종목들로만 price_map 구성
            price_map = {t: prices_df[t].loc[:eval_date].dropna() for t in valid_pit_universe if t in prices_df.columns}
            
            # AI 스코어링
            metrics = build_metrics_ml(list(price_map.keys()), univ, price_map)
            scored = score_group_ml(metrics, vix_level=current_vix, regime_str=regime, model=model)
            scored.sort(key=lambda x: x.final_score_100, reverse=True)
            
            # 종목 선정
            final_picks, sector_counts = [], {}
            top_25_tickers = {r.ticker for r in scored[:HOLD_BUFFER_N]}
            
            for r in scored:
                if len(final_picks) >= TOP_N: break
                is_held = r.ticker in current_weights
                sec = r.sector
                
                if is_held and r.ticker in top_25_tickers:
                    final_picks.append(r)
                    sector_counts[sec] = sector_counts.get(sec, 0) + 1
                elif sector_counts.get(sec, 0) < SECTOR_MAX_NAMES:
                    final_picks.append(r)
                    sector_counts[sec] = sector_counts.get(sec, 0) + 1
                    
            # 비중 계산
            spy_vol20 = annualized_volatility(past_spy, 20)
            dynamic_floor = max(VOL_WEIGHT_FLOOR, spy_vol20)
            weighted_picks = compute_portfolio_weights(
                final_picks, method=PORTFOLIO_WEIGHT_METHOD, alpha_score=2.5,
                min_w=MIN_WEIGHT, max_w=MAX_WEIGHT, vol_fallback=VOL_FALLBACK, 
                vol_floor=VOL_WEIGHT_FLOOR, dynamic_vol_floor=dynamic_floor
            )
            
            new_weights = {}
            detailed_holdings_log = []
            
            for w in weighted_picks:
                actual_w = w.portfolio_weight * target_exp
                new_weights[w.ticker] = actual_w
                detailed_holdings_log.append({"ticker": w.ticker, "weight_pct": round(actual_w * 100, 2)})
            
            def_wgt = 1.0 - target_exp
            if def_wgt > 0.001:
                tail_w = def_wgt * 0.3
                dbmf_w = def_wgt * 0.7
                new_weights["TAIL"] = tail_w; new_weights["DBMF"] = dbmf_w
                detailed_holdings_log.append({"ticker": "TAIL", "weight_pct": round(tail_w * 100, 2)})
                detailed_holdings_log.append({"ticker": "DBMF", "weight_pct": round(dbmf_w * 100, 2)})
                
            regime_log[date_str] = {
                "regime": regime,
                "exposure_pct": target_exp * 100,
                "holdings": detailed_holdings_log
            }
            current_weights = new_weights
            
    # ----------------------------------------------------
    # 📊 3. 최종 성과 보고서 (Metrics) 출력
    # ----------------------------------------------------
    print(f"\n✅ 3/3. 시뮬레이션 완료! (최종 원금 배수: {portfolio_value:.2f}배)")
    
    eq_series = pd.Series(equity_curve)
    eq_series.index = pd.to_datetime(eq_series.index)
    
    m_3y = calc_metrics(eq_series, years_back=3)
    m_5y = calc_metrics(eq_series, years_back=5)
    m_10y = calc_metrics(eq_series, years_back=10)
    m_all = calc_metrics(eq_series)
    
    print("\n" + "="*70)
    print("🏆 [Invest Portal V2.0 PIT 머신러닝 백테스트 성과 요약]")
    print("="*70)
    print(f"| {'구간':<4} | {'누적수익률':>9} | {'CAGR':>7} | {'Sharpe':>6} | {'Vol(변동성)':>9} | {'MDD(낙폭)':>9} |")
    print("-" * 70)
    
    def print_row(name, m):
        if m: print(f"| {name:<4} | {m['cum_ret']*100:>8.1f}% | {m['cagr']*100:>6.1f}% | {m['sharpe']:>6.2f} | {m['vol']*100:>7.1f}% | {m['mdd']*100:>8.1f}% |")
        
    print_row("3Y", m_3y)
    print_row("5Y", m_5y)
    print_row("10Y", m_10y)
    print_row("ALL", m_all)
    print("="*70)

    with open(OUT_REGIME, "w") as f: json.dump(regime_log, f, indent=4)
    with open(OUT_CURVE, "w") as f: json.dump(equity_curve, f, indent=4)
        
    print(f"\n📁 의사결정 로그 저장됨: {OUT_REGIME.name}")
    print(f"📁 일일 계좌잔고 저장됨: {OUT_CURVE.name}")

if __name__ == "__main__":
    run_ml_backtest()