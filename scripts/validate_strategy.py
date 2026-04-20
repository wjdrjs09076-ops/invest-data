import yfinance as yf
import pandas as pd
import numpy as np

# ==========================================
# 1. 고정된 레짐 파라미터 (Regime Overlay Constants)
# ==========================================
REGIME_MA_WINDOW = 200
REGIME_MOM_WINDOW = 63
REGIME_BUFFER = 0.005
REGIME_CONFIRM_DAYS = 2
VIX_CRASH_THRESHOLD = 40.0

# 1993~2020년 구간 탐색을 통해 선정 후 '고정'한 노출도 파라미터
RISK_ON_EXPOSURE = 1.00
MID_EXPOSURE = 0.85
RISK_OFF_EXPOSURE = 0.40

# [FIX] 방어 구간(노출 제외 60%)에 대한 명시적 현금 수익률 가정 (보수적으로 0% 설정)
CASH_RETURN = 0.0  

# 데이터 분할 기준일 (2020년 말까지 훈련, 2021년부터는 미래 검증)
OOS_START_DATE = "2021-01-01"

# ==========================================
# 2. 성과 측정 헬퍼 함수 (Sharpe, Volatility 추가)
# ==========================================
def calculate_metrics(df_slice, prefix=""):
    if len(df_slice) < 5:
        return {}
    
    # 누적 수익률 재조정 (구간 시작점을 1.0으로)
    equity = df_slice['equity'] / df_slice['equity'].iloc[0]
    spy_cum = df_slice['spy_cum'] / df_slice['spy_cum'].iloc[0]
    
    years = len(df_slice) / 252
    
    # CAGR
    strat_cagr = equity.iloc[-1] ** (1 / years) - 1
    spy_cagr = spy_cum.iloc[-1] ** (1 / years) - 1
    
    # MDD
    strat_mdd = (equity / equity.cummax() - 1).min()
    spy_mdd = (spy_cum / spy_cum.cummax() - 1).min()
    
    # [NEW] Volatility (연환산 변동성)
    strat_vol = df_slice['strategy_daily_ret'].std() * np.sqrt(252)
    spy_vol = df_slice['spy_daily_ret'].std() * np.sqrt(252)
    
    # [NEW] Sharpe Ratio (무위험 수익률 0% 가정)
    strat_sharpe = strat_cagr / strat_vol if strat_vol != 0 else 0
    spy_sharpe = spy_cagr / spy_vol if spy_vol != 0 else 0
    
    # Calmar Ratio
    strat_calmar = strat_cagr / abs(strat_mdd) if strat_mdd != 0 else 0
    spy_calmar = spy_cagr / abs(spy_mdd) if spy_mdd != 0 else 0
    
    return {
        f"{prefix}Strategy CAGR": strat_cagr,
        f"{prefix}SPY CAGR": spy_cagr,
        f"{prefix}Strategy Vol": strat_vol,
        f"{prefix}SPY Vol": spy_vol,
        f"{prefix}Strategy Sharpe": strat_sharpe,
        f"{prefix}SPY Sharpe": spy_sharpe,
        f"{prefix}Strategy MDD": strat_mdd,
        f"{prefix}SPY MDD": spy_mdd,
        f"{prefix}Strategy Calmar": strat_calmar,
        f"{prefix}SPY Calmar": spy_calmar,
    }

# ==========================================
# 3. 메인 시뮬레이션 및 검증 엔진
# ==========================================
def run_validation_engine():
    print("\n" + "="*65)
    print("🛡️ SPY 레짐 오버레이 강건성 검증 엔진")
    print("   * 주의: 종목 선택(Stock Selection) 알파는 제외되었으며,")
    print("     SPY 기준 타이밍(노출도 조절) 로직의 OOS 유효성만 테스트합니다.")
    print("="*65)

    print("데이터 다운로드 중 (SPY, VIX)...")
    spy_df = yf.download("SPY", start="1993-01-01", auto_adjust=True, progress=False)
    vix_df = yf.download("^VIX", start="1993-01-01", progress=False)
    
    spy = spy_df['Close'].squeeze()
    vix = vix_df['Close'].squeeze()
    
    df = pd.DataFrame({'price': spy, 'vix': vix}).dropna()
    
    # [NEW] VIX 결측치 등으로 인한 실제 시작일 확인
    print(f"▶ 실제 백테스트 병합 시작일: {df.index.min().date()}\n")
    
    # --- 핵심 로직 연산 (초고속 벡터화) ---
    df['ma200'] = df['price'].rolling(window=REGIME_MA_WINDOW).mean()
    df['mom63'] = df['price'].pct_change(periods=REGIME_MOM_WINDOW)
    df['prev_price'] = df['price'].shift(1)
    df['spy_daily_ret'] = df['price'] / df['prev_price'] - 1.0
    
    df = df.dropna().copy()
    
    # 후보 판단
    df['candidate'] = 'mid'
    cond_on = (df['price'] > df['ma200'] * (1.0 + REGIME_BUFFER)) & (df['mom63'] > 0)
    cond_off = (df['price'] < df['ma200'] * (1.0 - REGIME_BUFFER)) & (df['mom63'] < 0)
    df.loc[cond_on, 'candidate'] = 'risk_on'
    df.loc[cond_off, 'candidate'] = 'risk_off'
    
    # 2일 유지(Confirm) 로직
    candidates = df['candidate'].values
    regimes = np.full(len(candidates), 'risk_on', dtype=object)
    prev = 'risk_on'
    for i in range(len(candidates)):
        cand = candidates[i]
        if cand in ["risk_on", "risk_off"] and REGIME_CONFIRM_DAYS > 1:
            if i + 1 >= REGIME_CONFIRM_DAYS:
                recent = candidates[i - REGIME_CONFIRM_DAYS + 1 : i + 1]
                if all(x == cand for x in recent):
                    regimes[i] = cand
                else:
                    regimes[i] = prev
            else:
                regimes[i] = prev
        elif cand in ["risk_on", "risk_off"]:
            regimes[i] = cand
        else:
            regimes[i] = prev
        prev = regimes[i]
        
    df['regime'] = regimes
    
    # VIX 폭락 필터
    vix_crash = (df['price'] < df['ma200']) & (df['vix'] > VIX_CRASH_THRESHOLD)
    df.loc[vix_crash, 'regime'] = 'risk_off'
    
    # 비중(Exposure) 적용 및 1일 지연(Lag)
    conds = [df['regime'] == 'risk_on', df['regime'] == 'mid', df['regime'] == 'risk_off']
    choices = [RISK_ON_EXPOSURE, MID_EXPOSURE, RISK_OFF_EXPOSURE]
    df['exposure'] = np.select(conds, choices, default=1.0)
    
    df['applied_exposure'] = df['exposure'].shift(1).fillna(1.0)
    
    # [FIX] 전략 수익률 계산 방식 수정: 명시적 현금 수익률(cash_ret) 반영
    df['strategy_daily_ret'] = (
        df['applied_exposure'] * df['spy_daily_ret'] + 
        (1 - df['applied_exposure']) * CASH_RETURN
    )
    
    # 전체 누적 자산
    df['equity'] = (1 + df['strategy_daily_ret']).cumprod()
    df['spy_cum'] = (1 + df['spy_daily_ret']).cumprod()

    # ==========================================
    # 4. 구간별 리포트 분할 출력
    # ==========================================
    in_sample_df = df[df.index < OOS_START_DATE]
    out_of_sample_df = df[df.index >= OOS_START_DATE]

    is_metrics = calculate_metrics(in_sample_df)
    oos_metrics = calculate_metrics(out_of_sample_df)
    full_metrics = calculate_metrics(df)

    def print_section(title, m):
        print(f"[{title}]")
        print(f"▶ 전략 CAGR:   {m.get('Strategy CAGR', 0):>6.2%}  |  SPY CAGR:   {m.get('SPY CAGR', 0):>6.2%}")
        print(f"▶ 전략 Vol:    {m.get('Strategy Vol', 0):>6.2%}  |  SPY Vol:    {m.get('SPY Vol', 0):>6.2%}")
        print(f"▶ 전략 Sharpe: {m.get('Strategy Sharpe', 0):>6.3f}  |  SPY Sharpe: {m.get('SPY Sharpe', 0):>6.3f}")
        print(f"▶ 전략 MDD:    {m.get('Strategy MDD', 0):>6.2%}  |  SPY MDD:    {m.get('SPY MDD', 0):>6.2%}")
        print(f"▶ 전략 Calmar: {m.get('Strategy Calmar', 0):>6.3f}  |  SPY Calmar: {m.get('SPY Calmar', 0):>6.3f}\n")

    print_section("1. 훈련 구간 (In-Sample: 파라미터 탐색 구간 ~ 2020.12)", is_metrics)
    print_section("2. 미래 검증 구간 (Out-of-Sample: 2021.01 ~ 현재)", oos_metrics)
    print_section("3. 전체 기간 (Full Period: 1993 ~ 현재)", full_metrics)

    print("="*65)
    
    # [FIX] 내부 휴리스틱(Heuristic) 기반 강건성 체크로 언어 순화
    is_calmar = is_metrics.get('Strategy Calmar', 0)
    oos_calmar = oos_metrics.get('Strategy Calmar', 0)
    
    print("[내부 기준 휴리스틱 체크 (Robustness Check)]")
    if oos_calmar >= is_calmar * 0.7:
        print("✅ PASS: OOS 구간 캘마 비율이 IS 구간의 70% 이상을 유지했습니다.")
        print("   -> 레짐 필터와 노출 조절 오버레이가 과거 특정 구간에 과최적화되지 않고,")
        print("      미래 구간(2021~)에서도 일정 수준 이상의 리스크 관리 효율을 보입니다.")
    else:
        print("⚠️ WARNING: OOS 구간 효율이 크게 하락했습니다. 파라미터 재검토가 필요할 수 있습니다.")

if __name__ == "__main__":
    run_validation_engine()