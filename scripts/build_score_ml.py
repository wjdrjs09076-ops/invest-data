from __future__ import annotations

import pandas as pd
import numpy as np
import pickle
import math
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

# ==========================================
# 1. 경로 및 환경 설정
# ==========================================
ROOT = Path(__file__).resolve().parents[1]
MODEL_FILE = ROOT / "data" / "xgb_factor_model.pkl"

# AI 모델이 요구하는 피처(Feature) 순서 (절대 변경 금지)
ML_FEATURES = ['vix_level', 'regime_state', 'mom_21d', 'mom_63d', 'mom_252d', 'vol_20d', 'vol_252d']

# ==========================================
# 2. 데이터 클래스 (V2.0 전용)
# ==========================================
@dataclass
class MetricRecordML:
    ticker: str
    sector: str
    mom_21d: Optional[float]
    mom_63d: Optional[float]
    mom_252d: Optional[float]
    vol_20d: Optional[float]
    vol_252d: Optional[float]

@dataclass
class ScoreRecordML:
    ticker: str
    sector: str
    ml_predicted_return: float  # AI가 예측한 1달 뒤 예상 수익률
    final_score_100: float      # 기존 로직 호환을 위한 0~100 환산 점수
    # 절대 모멘텀 하드 필터(V1.0 로직) 유지를 위해 원본 수익률 보존
    ret63d: float
    ret252d: float
    vol20: float                # ✅ [추가 완료] 비중 계산기 에러 방지용 변수

# ==========================================
# 3. 모델 로드 및 레짐 변환 함수
# ==========================================
def load_ml_model():
    """훈련된 XGBoost 모델을 불러옵니다."""
    if not MODEL_FILE.exists():
        raise FileNotFoundError(f"AI 모델 파일을 찾을 수 없습니다: {MODEL_FILE}")
    with open(MODEL_FILE, 'rb') as f:
        return pickle.load(f)

def convert_regime_to_int(regime_str: str) -> int:
    """V1.0의 문자열 레짐을 AI가 이해하는 숫자로 번역합니다."""
    r = regime_str.lower()
    if 'crash' in r: return -2
    if 'risk_off' in r: return -1
    if 'risk_on' in r: return 1
    return 0  # mid

def annualized_volatility(series: pd.Series, window: int = 20) -> float:
    pct = series.pct_change().dropna()
    if len(pct) < 2: return np.nan
    return float(pct.tail(window).std() * math.sqrt(252))

# ==========================================
# 4. 피처 계산 (Feature Engineering)
# ==========================================
def build_metrics_ml(tickers: list[str], universe_map: dict, price_data_map: dict) -> list[MetricRecordML]:
    """AI에게 먹일 개별 종목의 5가지 피처를 계산합니다."""
    records = []
    for t in tickers:
        sec = universe_map.get(t, {}).get("sector", "Unknown") if isinstance(universe_map.get(t), dict) else universe_map.get(t, "Unknown")
        
        if t not in price_data_map:
            continue
            
        s = price_data_map[t]
        s_clean = s.dropna()
        if len(s_clean) < 252:
            continue # 1년 치 데이터 안 되면 패스
            
        # 모멘텀 계산 (V1.0과 동일한 방식 적용)
        try:
            p_now = s_clean.iloc[-1]
            m21 = (p_now / s_clean.iloc[-22]) - 1 if len(s_clean) >= 22 else np.nan
            m63 = (p_now / s_clean.iloc[-64]) - 1 if len(s_clean) >= 64 else np.nan
            m252 = (p_now / s_clean.iloc[-253]) - 1 if len(s_clean) >= 253 else np.nan
            
            v20 = annualized_volatility(s_clean, 20)
            v252 = annualized_volatility(s_clean, 252)
            
            records.append(MetricRecordML(
                ticker=t, sector=sec,
                mom_21d=m21, mom_63d=m63, mom_252d=m252,
                vol_20d=v20, vol_252d=v252
            ))
        except IndexError:
            continue
            
    return records

# ==========================================
# 5. 핵심: AI 스코어링 (Predict & Score)
# ==========================================
def score_group_ml(metrics: list[MetricRecordML], vix_level: float, regime_str: str, model) -> list[ScoreRecordML]:
    """AI 모델을 사용해 예상 수익률을 구하고, 이를 0~100점의 스코어로 변환합니다."""
    
    # 결측치 제거
    valid_metrics = [m for m in metrics if not any(pd.isna([m.mom_21d, m.mom_63d, m.mom_252d, m.vol_20d, m.vol_252d]))]
    if not valid_metrics:
        return []
        
    regime_val = convert_regime_to_int(regime_str)
    
    # XGBoost 예측을 위한 DataFrame 조립
    df_predict = pd.DataFrame([
        {
            'ticker': m.ticker,
            'sector': m.sector,
            'vix_level': vix_level,
            'regime_state': regime_val,
            'mom_21d': m.mom_21d,
            'mom_63d': m.mom_63d,
            'mom_252d': m.mom_252d,
            'vol_20d': m.vol_20d,
            'vol_252d': m.vol_252d
        } for m in valid_metrics
    ])
    
    # 피처 순서를 훈련 때와 완벽히 동일하게 정렬
    X = df_predict[ML_FEATURES]
    
    # 🧠 AI 예측 실행 (1달 뒤 예상 수익률 산출)
    predictions = model.predict(X)
    df_predict['ml_predicted_return'] = predictions
    
    # 💯 0~100 점수화 (백분위수 랭킹 사용)
    # 예상 수익률이 가장 높은 종목이 100점, 가장 낮은 종목이 0점을 받습니다.
    df_predict['final_score_100'] = df_predict['ml_predicted_return'].rank(pct=True) * 100
    
    # 결과 객체로 변환
    results = []
    for _, row in df_predict.iterrows():
        results.append(ScoreRecordML(
            ticker=row['ticker'],
            sector=row['sector'],
            ml_predicted_return=row['ml_predicted_return'],
            final_score_100=row['final_score_100'],
            ret63d=row['mom_63d'],
            ret252d=row['mom_252d'],
            vol20=row['vol_20d']     # ✅ [추가 완료] 비중 계산기 에러 방지용 변수 전달
        ))
        
    return results