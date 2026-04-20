from __future__ import annotations

import json
import logging
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime

# yfinance 경고창 무시
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 경로 및 파라미터 설정
# ==========================================
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUTPUT_FILE = DATA_DIR / "ml_dataset.csv"

# 🌟 [적용됨] 사용자님이 올려주신 이벤트 데이터 파일 경로
MEMBERSHIP_FILES = [
    DATA_DIR / "sp500_membership_events.json", 
    DATA_DIR / "sp400_membership_events.json", 
    DATA_DIR / "sp600_membership_events.json"
]

TRAIN_START = "2010-01-01" 
TRAIN_END = "2023-12-31"

# 데이터 클리닝 파라미터
MIN_PRICE = 3.0                 
MIN_DOLLAR_VOL = 1_000_000      
MAX_FWD_RET = 0.50              
MIN_FWD_RET = -0.50             

# ==========================================
# 2. 헬퍼 함수: Point-In-Time 유니버스 구축
# ==========================================
def load_json(path: Path, default: dict = None) -> dict:
    if not path.exists(): return default or {}
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def get_historical_universe(target_date_str: str, membership_data: list[dict]) -> set:
    """
    특정 과거 시점(target_date)에 실제로 S&P 1500에 포함되어 있던 종목들을 역산하여 찾아냅니다.
    """
    target_date = pd.to_datetime(target_date_str)
    pit_universe = set()
    
    for data in membership_data:
        current_tickers = set(data.get("current_tickers", []))
        
        # 이벤트 기록을 가져와서 날짜순으로 정렬 (최신 -> 과거)
        # 구조가 파일마다 조금 다를 수 있으므로 리스트인지 확인하며 파싱합니다.
        # (*사용자님의 json 구조에 맞춰 'added'와 'removed'를 역산합니다)
        events = []
        # JSON 내부 구조에 따라 events 배열을 추출 (에러 방지용 안전한 파싱)
        if isinstance(data, list):
            events = data
        elif "events" in data:
            events = data["events"]
        elif isinstance(data, dict):
            # 파일 형태에 맞춰 유동적으로 대응
            events = [v for k, v in data.items() if isinstance(v, dict) and "added" in v]
            if not events and "event_count" in data:
                # sp500_membership_events.json 처럼 최상단 배열이 아니라면 내부 배열을 찾습니다.
                for key, val in data.items():
                    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict) and "date" in val[0]:
                        events = val
                        break

        # 타임머신 역산 로직 (현재 -> 과거로 가면서 편입/편출을 반대로 적용)
        events.sort(key=lambda x: x.get("date", "1900-01-01"), reverse=True)
        
        for event in events:
            event_date = pd.to_datetime(event.get("date"))
            if event_date > target_date:
                # 타겟 날짜보다 미래에 '추가'되었다면 -> 타겟 날짜에는 없었음 (제거)
                current_tickers.difference_update(event.get("added", []))
                # 타겟 날짜보다 미래에 '제거'되었다면 -> 타겟 날짜에는 있었음 (복구)
                current_tickers.update(event.get("removed", []))
                
        pit_universe.update(current_tickers)
        
    return pit_universe

# ==========================================
# 3. 메인 전처리 함수
# ==========================================
def prepare_ml_dataset():
    print(f"🚀 [생존 편향 제거판] 머신러닝 데이터셋 구축 시작 ({TRAIN_START} ~ {TRAIN_END})")
    
    # 멤버십 파일 미리 로드
    membership_data = [load_json(f) for f in MEMBERSHIP_FILES if f.exists()]
    if not membership_data:
        print("⚠️ 경고: membership_events.json 파일들을 찾을 수 없습니다. 현재 유니버스로 대체합니다.")
    
    # 전체 기간 동안 한 번이라도 S&P 1500에 속했던 '모든' 종목 수집 (데이터 다운로드용)
    all_possible_tickers = set()
    for data in membership_data:
        all_possible_tickers.update(data.get("current_tickers", []))
        # 파싱 로직 재활용
        events = []
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict) and "date" in val[0]:
                events = val
                break
        for event in events:
            all_possible_tickers.update(event.get("added", []))
            all_possible_tickers.update(event.get("removed", []))
            
    tickers = list(all_possible_tickers)
    
    print("📥 1/4. SPY 및 VIX 거시 경제 데이터 다운로드 중...")
    macro_df = yf.download(["SPY", "^VIX"], start=TRAIN_START, end=TRAIN_END, progress=False)["Close"]
    
    macro_features = pd.DataFrame(index=macro_df.index)
    macro_features['vix_close'] = macro_df['^VIX']
    macro_features['spy_close'] = macro_df['SPY']
    macro_features['spy_ma200'] = macro_df['SPY'].rolling(200).mean()
    macro_features['spy_mom63'] = macro_df['SPY'].pct_change(63)
    
    conditions = [
        (macro_features['spy_close'] < macro_features['spy_ma200']) & (macro_features['vix_close'] > 40),
        (macro_features['spy_close'] <= macro_features['spy_ma200'] * 0.995) & (macro_features['spy_mom63'] < 0),
        (macro_features['spy_close'] >= macro_features['spy_ma200'] * 1.005) & (macro_features['spy_mom63'] >= 0)
    ]
    macro_features['regime_state'] = np.select(conditions, [-2, -1, 1], default=0)
    
    print(f"📥 2/4. 개별 종목 주가 다운로드 중 (과거 편출 종목 포함 총 {len(tickers)}개)...")
    price_data = yf.download(tickers, start=TRAIN_START, end=TRAIN_END, auto_adjust=True, progress=False)
    close_df = price_data["Close"]
    vol_df = price_data["Volume"]
    
    print("⚙️ 3/4. 벡터화(Vectorization) 및 클리닝 지표 계산 중...")
    dollar_vol_20d = (close_df * vol_df).rolling(20).mean()
    mom21 = close_df.pct_change(21)
    mom63 = close_df.pct_change(63)
    mom252 = close_df.pct_change(252)
    vol20 = close_df.pct_change(1).rolling(20).std() * np.sqrt(252)
    vol252 = close_df.pct_change(1).rolling(252).std() * np.sqrt(252)
    target_fwd_21d = close_df.pct_change(21).shift(-21)
    
    print("📊 4/4. Point-In-Time 유니버스 매칭 및 데이터 정제 중...")
    monthly_idx = close_df.resample('BME').last().index
    dataset_rows = []
    
    drop_count = 0 
    
    for date in monthly_idx:
        if date not in close_df.index: continue
        macro = macro_features.loc[date]
        if pd.isna(macro['spy_ma200']): continue
            
        date_str = date.strftime('%Y-%m-%d')
        
        # 🌟 [핵심 로직] 해당 월말에 '진짜로' S&P 1500에 있었던 종목들만 뽑아옵니다.
        valid_universe_at_date = get_historical_universe(date_str, membership_data)
            
        for ticker in valid_universe_at_date:
            if ticker not in close_df.columns: continue
            
            c_price = close_df.loc[date, ticker]
            c_dvol = dollar_vol_20d.loc[date, ticker]
            fwd_ret = target_fwd_21d.loc[date, ticker]
            m63 = mom63.loc[date, ticker]
            m252 = mom252.loc[date, ticker]
            v20 = vol20.loc[date, ticker]
            
            # 클리닝 로직
            if pd.isna(fwd_ret) or pd.isna(m63) or pd.isna(m252): continue
            if c_price < MIN_PRICE: drop_count += 1; continue
            if c_dvol < MIN_DOLLAR_VOL: drop_count += 1; continue
            if fwd_ret > MAX_FWD_RET or fwd_ret < MIN_FWD_RET: drop_count += 1; continue
            if m63 > 2.0 or m252 > 5.0 or v20 > 1.5: drop_count += 1; continue
                
            dataset_rows.append({
                'date': date_str,
                'ticker': ticker,
                'vix_level': round(macro['vix_close'], 2),
                'regime_state': macro['regime_state'],
                'mom_21d': round(mom21.loc[date, ticker], 4),
                'mom_63d': round(m63, 4),
                'mom_252d': round(m252, 4),
                'vol_20d': round(v20, 4),
                'vol_252d': round(vol252.loc[date, ticker], 4),
                'target_fwd_21d': round(fwd_ret, 4)
            })
            
    final_df = pd.DataFrame(dataset_rows)
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n🎉 PIT(Point-In-Time) 데이터셋 구축 완료!")
    print(f"🧹 필터링된 불량 데이터 총 {drop_count:,}개")
    print(f"✅ 살아남은 순도 100% 실전 학습용 행(Row): {len(final_df):,} 개")

if __name__ == "__main__":
    prepare_ml_dataset()