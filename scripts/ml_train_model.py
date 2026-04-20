import pandas as pd
import xgboost as xgb
import pickle
from pathlib import Path

# ==========================================
# 1. 경로 설정
# ==========================================
ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "data" / "ml_dataset.csv"
MODEL_FILE = ROOT / "data" / "xgb_factor_model.pkl"

# ==========================================
# 2. 메인 학습 함수
# ==========================================
def train_dynamic_factor_model():
    print("🧠 XGBoost 머신러닝 모델 학습을 시작합니다...")
    
    # 1. 데이터 불러오기
    print("📥 데이터셋 로딩 중...")
    df = pd.read_csv(DATA_FILE)
    
    # 타겟(정답지)이 없는 데이터는 혹시 모르니 한 번 더 제거
    df = df.dropna(subset=['target_fwd_21d'])
    
    # AI가 학습할 변수(Features)와 맞춰야 할 정답(Target) 설정
    features = ['vix_level', 'regime_state', 'mom_21d', 'mom_63d', 'mom_252d', 'vol_20d', 'vol_252d']
    target = 'target_fwd_21d'
    
    # 2. 전진 분석(Walk-Forward)을 위한 데이터 분할
    # 2010~2020년 데이터로 공부하고, 2021~2023년 데이터로 모의고사를 봅니다.
    df['date'] = pd.to_datetime(df['date'])
    train_df = df[df['date'].dt.year <= 2020]
    val_df = df[df['date'].dt.year > 2020]
    
    X_train, y_train = train_df[features], train_df[target]
    X_val, y_val = val_df[features], val_df[target]
    
    print(f"📊 훈련(Train) 데이터: {len(X_train):,} 개 (2010~2020)")
    print(f"📊 검증(Validation) 데이터: {len(X_val):,} 개 (2021~2023)")
    
    # 3. 🛡️ 과최적화 방지: 단조성 제약 (Monotonic Constraints)
    # 1: 이 값이 높을수록 타겟(수익률)도 높게 예측해라.
    # -1: 이 값이 낮을수록 타겟(수익률)을 높게 예측해라.
    # 0: AI 네가 알아서 판단해라 (거시 경제 지표 등)
    constraints = {
        'mom_63d': 1,    # 모멘텀이 높을수록 좋다
        'mom_252d': 1,   # 모멘텀이 높을수록 좋다
        'vol_252d': -1,  # 변동성이 낮을수록 좋다
        'vix_level': 0,
        'regime_state': 1,
        'mom_21d': 0,
        'vol_20d': 0
    }
    
    # 4. 모델 뼈대 구축 (의도적으로 멍청하게 만들어 과최적화 방지)
    model = xgb.XGBRegressor(
        n_estimators=100,         # 트리 개수 (너무 많으면 과거에 얽매임)
        learning_rate=0.05,       # 학습 속도
        max_depth=4,              # 트리 깊이 (단순하게 유지)
        subsample=0.8,            # 데이터의 80%만 무작위로 뽑아서 학습
        colsample_bytree=0.8,     # 피처의 80%만 무작위로 뽑아서 학습
        monotone_constraints=constraints, # 절대 규칙 주입
        random_state=42
    )
    
    # 5. 본격적인 학습 (Training)
    print("\n⚙️ 학습 진행 중... (약 10~30초 소요)")
    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train), (X_val, y_val)],
        verbose=10 # 10번마다 모의고사 점수 출력
    )
    
    # 6. 피처 중요도(Feature Importance) 출력
    print("\n🏆 [AI가 판단한 가장 중요한 투자 지표 순위]")
    importance = model.feature_importances_
    for name, score in sorted(zip(features, importance), key=lambda x: x[1], reverse=True):
        print(f"  - {name}: {score * 100:.2f}%")
        
    # 7. 모델 저장
    with open(MODEL_FILE, 'wb') as f:
        pickle.dump(model, f)
        
    print(f"\n🎉 모델 학습 완료 및 저장 성공! 📁 {MODEL_FILE.name}")

if __name__ == "__main__":
    train_dynamic_factor_model()