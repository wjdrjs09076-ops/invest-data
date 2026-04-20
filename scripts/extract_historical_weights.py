from __future__ import annotations

import json
import logging
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 기존 백테스트 모듈 임포트
# ==========================================
from build_score_snapshot import (
    build_metrics_for_group,
    score_group,
    compute_portfolio_weights,
    PORTFOLIO_WEIGHT_METHOD,
    MIN_WEIGHT,
    MAX_WEIGHT,
    VOL_FALLBACK,
    VOL_WEIGHT_FLOOR,
    ABS_MOM_63D_MIN,
    ABS_MOM_252D_MIN,
    annualized_volatility, 
)

# 경로 설정
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CURRENT_UNIVERSE_FILES = [
    DATA_DIR / "sp500_current_wiki.json", 
    DATA_DIR / "sp400_current_wiki.json", 
    DATA_DIR / "sp600_current_wiki.json"
]

# 파라미터 세팅 (V1.0 엔진 동일)
WEIGHT_ALPHA_SCORE = 2.5  
TOP_N = 15
SECTOR_MAX_NAMES = 3
REGIME_MA_WINDOW = 200
REGIME_MOM_WINDOW = 63
VIX_CRASH_THRESHOLD = 40.0
RISK_ON_EXPOSURE = 1.00
MID_EXPOSURE = 0.85
RISK_OFF_EXPOSURE = 0.40

# ==========================================
# 2. 🚀 [사용자 설정] 추출할 기간 세팅
# ==========================================
TARGET_START = "2019-01-02"  # 타임머신 시작일 (예: 코로나 폭락장 직전)
TARGET_END = "2019-12-31"    # 타임머신 종료일
FREQ = "W"                   # 추출 주기 ('M': 매월 말일, 'W': 매주 금요일)

OUTPUT_FILE = DATA_DIR / f"portfolio_history_{TARGET_START}_to_{TARGET_END}.json"

# ==========================================
# 3. 헬퍼 함수
# ==========================================
def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists(): return default
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def load_current_universe() -> dict[str, dict[str, Any]]:
    out = {}
    for file in CURRENT_UNIVERSE_FILES:
        for item in load_json(file, default={}).get("items", []):
            t = str(item.get("ticker", "")).strip().upper().replace(".", "-")
            if t: out[t] = {"ticker": t, "sector": item.get("sector", "Unknown") or "Unknown"}
    return out

# ==========================================
# 4. 메인 실행 함수 (Time Machine)
# ==========================================
def extract_historical_weights():
    print(f"⏳ 타임머신 가동: {TARGET_START} ~ {TARGET_END} (주기: {FREQ})")
    
    univ = load_current_universe()
    tickers = list(univ.keys())
    
    # 데이터는 계산을 위해 시작일보다 400일 전부터 넉넉히 다운로드
    start_dt = pd.to_datetime(TARGET_START) - timedelta(days=400)
    end_dt = pd.to_datetime(TARGET_END) + timedelta(days=1)
    
    print(f"📥 1/3. 전체 기간 주가 데이터 다운로드 중 (SPY, VIX & S&P 1500)...")
    spy_vix = yf.download(["SPY", "^VIX"], start=start_dt, end=end_dt, progress=False)["Close"]
    df_dl = yf.download(tickers, start=start_dt, end=end_dt, auto_adjust=True, progress=False, group_by="ticker", threads=True)
    
    # 타임라인 생성 (예: 매월 말일)
    dates_to_eval = pd.date_range(start=TARGET_START, end=TARGET_END, freq=FREQ)
    
    history_log = {}

    print(f"⚙️ 2/3. 총 {len(dates_to_eval)}개의 시점에 대한 포트폴리오 추출 시작...")
    
    for eval_date in dates_to_eval:
        date_str = eval_date.strftime("%Y-%m-%d")
        
        # 1. 해당 시점까지의 데이터만 '싹둑' 자르기 (미래 데이터 참조 방지)
        past_spy = spy_vix["SPY"].loc[:eval_date].dropna()
        past_vix = spy_vix["^VIX"].loc[:eval_date].dropna()
        
        if len(past_spy) < REGIME_MA_WINDOW:
            continue # 데이터가 부족하면 패스
            
        current_spy = past_spy.iloc[-1]
        ma200_val = past_spy.rolling(REGIME_MA_WINDOW).mean().iloc[-1]
        mom63_val = past_spy.pct_change(REGIME_MOM_WINDOW).iloc[-1]
        current_vix = past_vix.iloc[-1]

        # 2. 당시의 레짐 판단
        is_crash = current_spy < ma200_val and current_vix > VIX_CRASH_THRESHOLD
        if is_crash:
            final_regime = 'risk_off (VIX CRASH)'
        elif current_spy >= ma200_val * 1.005 and mom63_val >= 0:
            final_regime = 'risk_on'
        elif current_spy <= ma200_val * 0.995 and mom63_val < 0:
            final_regime = 'risk_off'
        else:
            final_regime = 'mid'
            
        target_exp = RISK_ON_EXPOSURE if 'risk_on' in final_regime else (RISK_OFF_EXPOSURE if 'risk_off' in final_regime else MID_EXPOSURE)

        # 3. 당시의 종목 주가 데이터 자르기
        price_map = {}
        if isinstance(df_dl.columns, pd.MultiIndex):
            level0 = set(df_dl.columns.get_level_values(0))
            for t in tickers:
                if t in level0 and "Close" in df_dl[t]:
                    s = df_dl[t]["Close"].loc[:eval_date].dropna()
                    if len(s) >= 252: 
                        price_map[t] = s
                        
        # 4. 당시 기준으로 스코어링
        metrics = build_metrics_for_group(list(price_map.keys()), univ, price_map)
        scored = score_group(metrics, quality_score_map=None)
        
        ranked = [r for r in scored if r.final_score_100 is not None and r.ret63d is not None and r.ret252d is not None]
        ranked = [r for r in ranked if r.ret63d > ABS_MOM_63D_MIN and r.ret252d > ABS_MOM_252D_MIN]
        ranked.sort(key=lambda x: x.final_score_100, reverse=True)

        # 5. 종목 선정 (섹터 캡 적용)
        final_picks, sector_counts = [], {}
        for r in ranked:
            if len(final_picks) >= TOP_N: break
            sec = r.sector or "Unknown"
            if SECTOR_MAX_NAMES <= 0 or sector_counts.get(sec, 0) < SECTOR_MAX_NAMES:
                final_picks.append(r)
                sector_counts[sec] = sector_counts.get(sec, 0) + 1
        
        # 6. 비중 계산
        spy_vol20 = annualized_volatility(past_spy, 20)
        dynamic_floor = max(VOL_WEIGHT_FLOOR, spy_vol20)
        weighted_picks = compute_portfolio_weights(
            final_picks, method=PORTFOLIO_WEIGHT_METHOD, alpha_score=WEIGHT_ALPHA_SCORE, 
            min_w=MIN_WEIGHT, max_w=MAX_WEIGHT, vol_fallback=VOL_FALLBACK, 
            vol_floor=VOL_WEIGHT_FLOOR, dynamic_vol_floor=dynamic_floor
        )

        # 7. 결과 저장 (JSON 구조화)
        daily_holdings = []
        for r in weighted_picks:
            actual_w = r.portfolio_weight * target_exp
            daily_holdings.append({
                "ticker": r.ticker,
                "sector": r.sector,
                "score": round(r.final_score_100, 1),
                "target_weight_pct": round(actual_w * 100, 2)
            })
            
        def_wgt = 1.0 - target_exp
        if def_wgt > 0.001:
            daily_holdings.append({"ticker": "TAIL", "sector": "Defensive", "score": 0, "target_weight_pct": round((def_wgt * 0.3) * 100, 2)})
            daily_holdings.append({"ticker": "DBMF", "sector": "Defensive", "score": 0, "target_weight_pct": round((def_wgt * 0.7) * 100, 2)})

        history_log[date_str] = {
            "regime": final_regime,
            "stock_exposure_pct": round(target_exp * 100, 2),
            "holdings": daily_holdings
        }
        
        print(f"   ✓ {date_str} 완료 (레짐: {final_regime.upper()}, 종목 수: {len(daily_holdings)})")

    # 최종 저장
    print(f"\n💾 3/3. JSON 파일 저장 중: {OUTPUT_FILE.name}")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(history_log, f, indent=4, ensure_ascii=False)
    
    print("🎉 추출이 완료되었습니다!")

if __name__ == "__main__":
    extract_historical_weights()