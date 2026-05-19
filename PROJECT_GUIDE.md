# invest-data — 프로젝트 구조 설명서

> 미국 S&P 1500 양자영감 알파 백테스트 시스템  
> 처음 보는 개발자 또는 AI 에이전트가 전체 구조를 빠르게 파악하기 위한 문서.

---

## 목차

1. [시스템 전체 구조](#1-시스템-전체-구조)
2. [데이터 흐름 요약](#2-데이터-흐름-요약)
3. [핵심 엔진: run_backtest_new.py](#3-핵심-엔진-run_backtest_newpy)
4. [양자영감 신호 모듈](#4-양자영감-신호-모듈)
5. [데이터 빌더 스크립트](#5-데이터-빌더-스크립트)
6. [스냅샷 & 포털 피드](#6-스냅샷--포털-피드)
7. [실험 검증 프레임워크](#7-실험-검증-프레임워크)
8. [라이브 트레이딩](#8-라이브-트레이딩)
9. [GitHub Actions 자동화](#9-github-actions-자동화)
10. [데이터 파일 레퍼런스](#10-데이터-파일-레퍼런스)
11. [IS/OOS 설계 원칙](#11-isoos-설계-원칙)
12. [개발 규칙 요약](#12-개발-규칙-요약)

---

## 1. 시스템 전체 구조

```
invest-portal/                   ← Next.js 프론트엔드 포털
│
├── public/data/                 ← 프론트엔드가 읽는 정적 JSON
│   ├── score_snapshot.json
│   ├── equity_curve_regime.json
│   └── ...
│
└── invest-data/                 ← 이 리포지토리 (Python 데이터 파이프라인)
    ├── scripts/                 ← 모든 Python 스크립트
    ├── data/                    ← 중간 산출물 (pkl/json, git 미추적)
    ├── .github/workflows/       ← GitHub Actions CI/CD
    └── PROJECT_GUIDE.md         ← 이 문서
```

**데이터 소스 (외부)**

| 소스 | 내용 | 접근 방식 |
|------|------|-----------|
| SHARADAR/SEP | 일별 주가 (수정 기준) | Nasdaq Data Link API |
| SHARADAR/DAILY | 밸류에이션 (EV/EBIT 등) | Nasdaq Data Link API |
| SHARADAR/SF3 | 기관 13F 보유 내역 | Nasdaq Data Link API |
| SHARADAR/SF2 | 내부자 Form4 거래 | Nasdaq Data Link API |
| SHARADAR/SF1 | 재무제표 (Altman Z-score) | Nasdaq Data Link API |
| Wikipedia | S&P 500/400/600 현재 구성 | 웹 스크레이핑 |
| yfinance | 실시간 가격, VIX | yfinance 라이브러리 |
| Finnhub | 뉴스 기사 | Finnhub API |
| Alpaca | 브로커리지 (주문 실행) | Alpaca API |

---

## 2. 데이터 흐름 요약

```
[외부 API]
    │
    ▼
[데이터 빌더] ──────────────────────────────────────────────────────┐
  build_sep_cache.py          → data/sep_prices.pkl                 │
  build_daily_history.py      → data/daily_history.pkl              │
  build_sf3_history.py        → data/sf3_history.pkl                │ PIT 캐시
  build_sf2_history.py        → data/sf2_history.pkl                │ (Point-in-Time
  build_inst_neutral_history.py → data/inst_neutral_history.pkl     │  무결성 보장)
  build_sp{500|400|600}_membership.py → data/*_membership_events.json │
    │
    ▼
[백테스트 엔진]
  run_backtest_new.py (BacktestEngine)
    ├── engine.load_data()       # 위 pkl 파일 전부 메모리 로드
    ├── engine.run(weights)      # IS/OOS 백테스트 실행
    └── engine.use_quantum_signal_ae()  # AE-VQC 신호 활성화
    │
    ▼
[신호 모듈]
  quantum_signal_ae.py (AE-VQC)     학습된 회로 → 매수 확률
  quantum_signal.py   (VQC)         구 버전
  quantum_optimizer.py (QUBO)       포트폴리오 최적화
    │
    ▼
[실험 검증]
  experiment_harness.py             실험 실행·로그 누적
  compute_dsr.py                    Deflated Sharpe Ratio 검증
  analyze_feature_ic.py             팩터 IC 분석
    │
    ▼
[스냅샷 생성]
  build_score_snapshot.py     → data/score_snapshot.json
  build_risk_snapshot.py      → data/risk_snapshot.json
  build_market_regime.py      → data/market_regime.json
  build_live_performance.py   → data/live_performance.json
    │
    ▼
[포털 배포]
  public/data/*.json          # Next.js 포털이 읽음
    │
    ▼
[라이브 트레이딩]
  alpaca_rebalance.py         # score_snapshot → 실제 주문 실행
```

---

## 3. 핵심 엔진: run_backtest_new.py

**역할**: 시장 환경을 최대한 정밀하게 복제한 백테스트 실행 엔진. "어떤 모델이 좋은가"는 결정하지 않고, 외부에서 팩터 가중치를 주입받아 성과만 계산한다.

### 주요 클래스

#### `BacktestEngine`

```python
engine = BacktestEngine()
engine.load_data()                    # 데이터 로드 (약 30~60초)
engine.use_quantum_signal_ae()        # AE-VQC 신호 활성화 (선택)
result = engine.run(weights)          # 백테스트 실행
```

**`engine.run(weights)` 반환값 (`BacktestResult`)**

| 속성 | 내용 |
|------|------|
| `is_sharpe` | IS 기간 Sharpe Ratio |
| `oos_sharpe` | OOS 기간 Sharpe Ratio |
| `is_metrics` | IS 전체 지표 dict (CAGR, MaxDD, Sortino 등) |
| `oos_metrics` | OOS 전체 지표 dict |
| `equity_curve` | 일별 자산 가치 시계열 |

**`weights` 포맷**

```python
weights = {
    "mom12_1":   0.30,   # 12개월 모멘텀
    "evebit":    0.20,   # EV/EBIT 밸류에이션
    "pb":        0.15,   # P/B 밸류에이션
    "institutional": 0.15,  # 기관 순매수 13F
    "insider":   0.10,   # 내부자 순매수 Form4
    "quantum_ml_ae": 0.10,  # AE-VQC 신호 (use_quantum_signal_ae() 필요)
}
```

### FACTOR_UNIVERSE — 사용 가능한 팩터

| 팩터 키 | 타입 | 방향 | 설명 |
|---------|------|------|------|
| `mom12_1` | price | high_good | 12개월 모멘텀 (최근 1개월 제외) |
| `mom9_1` | price | high_good | 9개월 모멘텀 |
| `mom6_1` | price | high_good | 6개월 모멘텀 |
| `mom3_1` | price | high_good | 3개월 모멘텀 |
| `mom1` | price | high_good | 1개월 단기 모멘텀 |
| `rs_spy_12m` | price_vs_bench | high_good | SPY 대비 12개월 초과수익 |
| `rs_spy_6m` | price_vs_bench | high_good | SPY 대비 6개월 초과수익 |
| `rs_spy_3m` | price_vs_bench | high_good | SPY 대비 3개월 초과수익 |
| `evebit` | daily | low_good | EV/EBIT (낮을수록 저평가) |
| `evebitda` | daily | low_good | EV/EBITDA |
| `pb` | daily | low_good | Price/Book |
| `pe` | daily | low_good | Price/Earnings |
| `ps` | daily | low_good | Price/Sales |
| `zscore` | zscore | high_good | Altman Z-score 재무건전성 |
| `institutional` | inst | high_good | SF3 13F 기관 QoQ 순매수 |
| `insider` | insider | high_good | SF2 Form4 내부자 순매수 |
| `inst_crowding` | inst_crowding | low_good | 기관 보유 비율 (낮을수록 미발견) |
| `inst_crowding_neutral` | inst_crowding_neutral | low_good | 시가총액 통제 기관 크라우딩 |
| `inst_new_holders` | inst_detail | high_good | 신규 진입 기관 수 비율 QoQ (sf3_detail_history.pkl) |
| `inst_n_holders_chg` | inst_detail | high_good | 보유 기관 수 QoQ 변화율 (sf3_detail_history.pkl) |
| `inst_hhi` | inst_detail | low_good | 기관 보유 집중도 HHI (sf3_detail_history.pkl) |
| `inst_smart_proxy` | inst_smart_proxy | high_good | 스마트머니 집중도: (-inst_neutral_residual) × HHI (미발견+소수집중) |
| `inst_crowding_sector_neutral` | inst_crowding_sector_neutral | low_good | 섹터+사이즈 이중 통제 기관 크라우딩 (sector_neutral_history.pkl) |
| `quantum_ml_ae` | quantum | — | AE-VQC 앙상블 신호 |

### 핵심 내부 메서드

| 메서드 | 역할 |
|--------|------|
| `load_data()` | sep_prices.pkl, daily_history.pkl, sf3/sf2_history.pkl 로드 |
| `_raw_signal(ticker, factor, ...)` | 개별 팩터 값 계산 (PIT 보장) |
| `_score_all(tickers, sliced, bench, date, weights)` | 전 종목 팩터 점수 계산 |
| `use_quantum_signal_ae()` | AEVQCSignal 객체 로드 및 활성화 |

### PIT(Point-In-Time) 무결성

모든 팩터 계산은 리밸런싱 날짜 기준 **이전 데이터만** 사용한다. SF3(13F)는 45일 필링 래그를 적용해 look-ahead bias를 제거한다.

```python
# SF3 13F 필링 래그 예시
filing_lag = pd.Timedelta(days=45)
inst_val = self._sf3.get_level(ticker, as_of - filing_lag)
```

---

## 4. 양자영감 신호 모듈

### quantum_signal_ae.py — AE-VQC (현재 사용 중)

**역할**: Amplitude Encoding 기반 Variational Quantum Circuit. 16개 팩터를 4큐비트로 압축해 매수 확률을 예측한다.

```
입력 (16개 팩터)
  ↓
AmplitudeEmbedding — 16차원 벡터 → 4큐비트 진폭 상태
  ↓
VQC Layer × 3 — Rot(φ,θ,ω) + CNOT 순환 얽힘
  ↓
PauliZ(0) 측정 → sigmoid → [0,1] 매수 확률
  ↓
앙상블 평균 (5개 seed)
```

**입력 피처 (AE_INPUT_FEATURES, 16개 — 순서 고정)**

```
가격 모멘텀 : mom12_1, mom9_1, mom6_1, mom3_1, mom1
상대강도    : rs_spy_12m, rs_spy_6m, rs_spy_3m
밸류에이션  : evebit, evebitda, pb, pe, ps
기관크라우딩: inst_crowding_neutral
수급        : institutional, insider
```

**주요 클래스**

| 클래스 | 역할 |
|--------|------|
| `AEVQCTrainer` | 회로 학습 (`fit(X, y)`) |
| `AEVQCSignal` | 추론 (`load()` → `predict(ticker, date, engine)`) |

**파라미터 파일**

| 파일 | 내용 |
|------|------|
| `data/quantum_vqc_ae_params.pkl` | 앙상블 파라미터 (list of 5 arrays) |
| `data/quantum_vqc_ae_norm.pkl` | 정규화 mu/std |
| `data/quantum_vqc_ae_meta.json` | 회로 구성 메타 |

**학습 파이프라인**

```bash
# 1. 앙상블 학습 (5 seeds)
python scripts/train_ae_vqc_ensemble.py

# 2. 백테스트에서 사용
engine.use_quantum_signal_ae()
result = engine.run({"quantum_ml_ae": 0.5, "mom12_1": 0.5})
```

**IS/OOS 분리 설계**

- 학습 데이터: IS 기간(2014-05-01 ~ 2019-12-31) 내부 rebal_date + future_date 쌍만 사용
- `future_date > IS_END`인 샘플은 학습에서 제외 (OOS 라벨 오염 방지)
- 결측 피처는 NaN → `np.nanmean`/`np.nanstd`로 편향 없는 정규화

---

### quantum_signal.py — VQC (구 버전)

AngleEmbedding 기반 6큐비트 회로. 6개 팩터만 인코딩 가능. `quantum_vqc_params.pkl` 사용. AE 버전 도입 후 실험 비교용으로만 사용.

---

### quantum_optimizer.py — QUBO 포트폴리오 최적화

QUBO(Quadratic Unconstrained Binary Optimization)로 종목 선택 문제를 정식화하고 Simulated Annealing으로 푼다.

```
목적함수:
  Minimize  -α × Σ score_i·x_i          (알파 최대화)
            + λ × Σ_ij cov_ij·x_i·x_j   (리스크 최소화)
            + P × (Σ_i x_i - K)²         (종목 수 제약)
```

```python
opt = QuantumPortfolioOptimizer(top_n=15, risk_aversion=0.3)
weights = opt.optimize(scores, price_slices)
```

---

## 5. 데이터 빌더 스크립트

모든 빌더는 `BacktestEngine().load_data()`를 통해 데이터에 접근한다. pkl 파일을 직접 열지 않는다.

### 가격 데이터

| 스크립트 | 출력 | 주기 |
|---------|------|------|
| `build_sep_cache.py` | `sep_prices.pkl` (~41MB) | 주 1회 |
| `build_daily_history.py` | `daily_history.pkl` (~300MB) | 주 1회 |

- `sep_prices.pkl`: {ticker: pd.Series(날짜→조정가)} 딕셔너리
- `daily_history.pkl`: {ticker: {metric: pd.Series}} 딕셔너리 (EV/EBIT 등 밸류에이션)

### 재무·신호 데이터

| 스크립트 | 출력 | 내용 |
|---------|------|------|
| `build_sf3_history.py` | `sf3_history.pkl` | SF3 13F 기관 보유 (분기별, PIT) |
| `build_sf2_history.py` | `sf2_history.pkl` | SF2 Form4 내부자 거래 (롤링 12개월) |
| `build_altman_zscore.py` | `altman_zscore.json` | SF1 기반 Altman Z-score |
| `build_zscore_history.py` | `zscore_history.json` | Z-score 역사적 시계열 |
| `build_inst_neutral_history.py` | `inst_neutral_history.pkl` | 시가총액 통제 기관 크라우딩 (OLS 잔차) |
| `build_sf3_detail_history.py` | `sf3_detail_history.pkl` | SF3 투자자별 상세 신호 (n_holders, new_holders, HHI) |
| `build_sector_neutral_history.py` | `sector_neutral_history.pkl` | 섹터+사이즈 이중 중립 기관 크라우딩 OLS 잔차 |

### 멤버십 데이터

| 스크립트 | 출력 | 내용 |
|---------|------|------|
| `build_sp500_membership.py` | `sp500_membership_events.json` | S&P 500 편입/편출 이벤트 |
| `build_sp400_membership.py` | `sp400_membership_events.json` | S&P 400 Mid-cap |
| `build_sp600_membership.py` | `sp600_membership_events.json` | S&P 600 Small-cap |

멤버십 이벤트는 각 날짜에 실제로 S&P 지수에 포함된 종목만 유니버스로 사용하기 위한 PIT 기록이다.

### 신호 빌더

| 스크립트 | 출력 | 내용 |
|---------|------|------|
| `build_institutional_signal.py` | `institutional_signal.json` | 포털 표시용 기관 신호 스냅샷 |
| `build_insider_signal.py` | `insider_signal.json` | 포털 표시용 내부자 신호 스냅샷 |
| `build_fundamentals.py` | `fundamentals.json` | 주요 재무지표 요약 |

---

## 6. 스냅샷 & 포털 피드

### build_score_snapshot.py

**역할**: 현재 시점 기준으로 전 종목의 팩터 점수를 계산해 포털이 읽는 JSON을 생성한다.

**동작 흐름**

```
1. 유니버스 로드 (SP500 + SP400 + SP600 + NASDAQ100 + DOW30)
2. 각 종목 팩터 점수 계산 (momentum, valuation, quality, risk)
3. 절대 모멘텀 필터 적용 (63D > -5%, 252D > -10%)
4. 뉴스 필터 (HARD_KILL_REGEX 매칭 시 포트폴리오에서 제외)
5. 섹터별 상위 N 종목 선정
6. score_snapshot.json + public/data/score_snapshot.json 저장
```

**핵심 상수**

| 상수 | 값 | 의미 |
|------|----|------|
| `ABS_MOM_63D_MIN` | -5 | 63일 수익률 하한 (pct 단위) |
| `ABS_MOM_252D_MIN` | -10 | 252일 수익률 하한 (pct 단위) |
| `SECTOR_MAX_NAMES` | 3 | 섹터당 최대 편입 종목 수 |
| `MAX_WEIGHT` | 0.20 | 단일 종목 최대 비중 |
| `HARD_KILL_REGEX` | 파산/상장폐지 등 | 이 패턴 뉴스 있으면 즉시 제외 |

### build_risk_snapshot.py

**역할**: 포트폴리오 리스크 지표 계산 (변동성, 최대낙폭, Sortino 등).

### build_market_regime.py

**역할**: 시장 레짐 분류 (Bull/Bear/Sideways) — SPY MA200 + 63일 모멘텀 기반. `market_regime.json` 생성.

### build_live_performance.py

**역할**: 라이브 시작일(2026-05-04) 이후 실제 성과를 `equity_curve_regime.json`에서 추출해 `live_performance.json` 생성.

### update_live_performance.py

**역할**: 매일 yfinance로 현재 포트폴리오 가격을 업데이트하고 live_performance.json을 갱신. GitHub Actions에서 일 1회 실행.

### build_banners.py / build_news.py

**역할**: 포털 배너 및 뉴스 피드 생성. Finnhub API 사용.

---

## 7. 실험 검증 프레임워크

### experiment_harness.py

**역할**: 여러 팩터 가중치 조합을 일괄 실행하고 결과를 JSON에 누적 저장.

```python
from experiment_harness import run_experiments

run_experiments([
    {
        "id":      "baseline",
        "weights": {"mom12_1": 0.5, "evebit": 0.5},
    },
    {
        "id":      "ae_vqc_v1",
        "weights": {"mom12_1": 0.4, "quantum_ml_ae": 0.6},
    },
])
# → data/experiment_log.json 에 결과 누적
```

### compute_dsr.py — Deflated Sharpe Ratio

**역할**: 다중검정 보정 Sharpe 검증 (Bailey & López de Prado, 2014).

```
PSR(SR*) = Φ[(ŜR - SR*) / σ̂(ŜR)]
SR* = μ_SR + σ_SR × E[max of N N(0,1)]  (N = 총 시도 실험 수)
```

결과가 **DSR ≥ 0.95** 이상이어야 통계적으로 유의미한 알파로 인정.

### build_alphalens_factor.py — 팩터 IC 분석 (alphalens)

**역할**: IS 기간(2014–2019) 전체 유니버스를 대상으로 16개 AE_INPUT_FEATURES의 Spearman IC를 계산하고 `data/alphalens/factor_summary.json`에 저장.

```bash
python scripts/build_alphalens_factor.py              # 전체 16 팩터
python scripts/build_alphalens_factor.py --factor mom12_1   # 단일 팩터
python scripts/build_alphalens_factor.py --periods 1,5,21   # 선행수익률 기간
```

**동작 흐름**

```
1. BacktestEngine 로드
2. IS 기간 월별 리밸런싱 날짜 생성 (68개 날짜)
3. 각 날짜 × 전 유니버스 종목에 대해 팩터값 계산 (low_good 팩터는 부호 반전)
4. alphalens get_clean_factor_and_forward_returns 시도
   → 실패 시(주파수 불일치): scipy.stats.spearmanr로 수동 IC 계산 (폴백)
5. IC(1D/5D/21D), IR, hit rate, n 집계
6. data/alphalens/factor_summary.json 저장
   data/alphalens/<factor>_tearsheet.png 저장 (alphalens 성공 시)
```

**주요 발견 (IS 2014–2019, 21D IC 기준)**

| 팩터 | IC(21D) | 해석 |
|------|---------|------|
| `insider` | +0.005 | 내부자 매수 신호 — 유일하게 양의 21D IC |
| `inst_crowding_neutral` | +0.003 | 기관 미발견 종목 프리미엄 |
| `mom1` | −0.030 | 단기 반전 효과 (학계와 일치) |
| 밸류에이션 전반 | 음수 | 2014–2019 성장주 우세 시장 반영 |

**출력 파일**

| 파일 | 내용 |
|------|------|
| `data/alphalens/factor_summary.json` | 팩터별 IC/IR/hit rate 요약 |
| `data/alphalens/<factor>_tearsheet.png` | alphalens 시각화 (alphalens 성공 시) |

---

### baseline_crossvalidate.py — 내부 일관성 검증

**역할**: 단일 팩터 월별 top-N equal-weight 백테스트로 데이터 파이프라인의 기본 동작(CAGR 양수, Sharpe 합리적)을 확인한다. BacktestEngine의 전체 전략(복합팩터+TC+레짐)과 수치를 직접 비교하지 않는다.

**중요 — GS Quant 한계**: GS Quant 오픈소스는 Marquee 계정 없이 백테스트 API를 제공하지 않는다. 실제 외부 라이브러리 교차검증이 필요하면 `bt` 또는 `vectorbt` 사용 권장.

```bash
python scripts/baseline_crossvalidate.py                    # 기본: mom12_1, top-15
python scripts/baseline_crossvalidate.py --factor insider   # 팩터 지정
python scripts/baseline_crossvalidate.py --top-n 20         # 종목 수 변경
```

**동작 흐름**

```
1. IS 기간 signal_df(월별 팩터값) + price_df(일별 가격) 빌드
2. run_simple_backtest(): 월별 top-N EW 백테스트
   - 진입가: rebal_date 다음 영업일 종가 (1-day execution lag)
   - 청산가: 다음 rebal_date 직전 종가
   - TC 없음, equal weight
   - Sharpe=sqrt(12) 연환산, CAGR=캘린더 연수 기준
3. data/baseline_crossval_result.json 저장 (timestamp 포함)
```

**출력 파일**

| 파일 | 내용 |
|------|------|
| `data/baseline_crossval_result.json` | 단일팩터 baseline 결과 (timestamp, settings 포함) |

> `gsquant_crossvalidate.py`는 deprecated — 항상 자체 구현 fallback만 실행하며 GS Quant를 실제로 호출하지 않음. `baseline_crossvalidate.py`가 대체.

---

### run_ae_ensemble_backtest.py — AE-VQC 앙상블 빠른 확인

**역할**: `engine.use_quantum_signal_ae()`를 활성화한 뒤 `quantum_ml_ae` 단일 신호로 IS/OOS 성과를 출력하는 원샷 스크립트. 앙상블 파라미터 변경 후 결과 즉시 확인용.

```bash
python scripts/run_ae_ensemble_backtest.py
```

**출력 예시**

```
=== AE-VQC 앙상블 (5 seeds, inst_crowding_neutral) ===
IS  Sharpe: 0.983    OOS Sharpe: 0.745
IS  CAGR:   13.2%    OOS CAGR:   12.4%
IS  MaxDD:  -16.0%   OOS MaxDD:  -24.4%
일반화 ratio (OOS/IS Sharpe): 0.758
```

---

### 기타 분석 스크립트

| 스크립트 | 역할 |
|---------|------|
| `analyze_feature_ic.py` | 팩터별 Spearman IC (1개월 선행수익률 상관) 계산 |
| `compute_signal_correlation.py` | 두 신호 간 OOS 상관관계 |
| `compute_dsr_crowding.py` | inst_crowding 신호 DSR 검증 |
| `analyze_subperiod.py` | IS/OOS 세부 기간 분석 |
| `audit_pit.py` | PIT 무결성 감사 |
| `ablation_study.py` | 팩터 ablation 실험 |
| `run_spa_test.py` | SPA(Superior Predictive Ability) 검정 |

---

## 8. 라이브 트레이딩

### alpaca_rebalance.py

**역할**: score_snapshot.json의 목표 비중을 읽어 Alpaca에 실제 주문을 실행한다.

**동작 흐름**

```
1. live_state.json → 목표 포트폴리오 비중 로드
2. Alpaca API → 현재 포지션 + 계좌 잔고 조회
3. 드리프트 판단: |현재비중 - 목표비중| > 임계값 OR 월 1회 강제 리밸런싱
4. 매도 먼저 실행 (현금 확보)
5. 매수 실행 (시장가)
6. data/alpaca_trade_log.json 에 거래 기록 누적
```

**환경변수 (GitHub Secrets)**

```
ALPACA_API_KEY      Alpaca API Key ID
ALPACA_SECRET_KEY   Alpaca Secret Key
ALPACA_BASE_URL     paper: https://paper-api.alpaca.markets
                    live:  https://api.alpaca.markets
```

**관련 데이터 파일**

| 파일 | 내용 |
|------|------|
| `data/live_state.json` | 현재 목표 포트폴리오 비중 |
| `data/final_holdings.json` | 마지막 확정 보유 비중 |
| `data/alpaca_trade_log.json` | 거래 이력 누적 |

---

## 9. GitHub Actions 자동화

모든 워크플로는 `invest-data/.github/workflows/`에 위치한다.

| 워크플로 파일 | 트리거 | 실행 스크립트 | 역할 |
|-------------|--------|-------------|------|
| `alpaca_daily.yml` | 매일 15:35 UTC (장 마감 후) | `alpaca_rebalance.py` | 자동 리밸런싱 |
| `build_score_snapshot.yml` | 매주 또는 수동 | `build_score_snapshot.py` | 점수 스냅샷 생성 |
| `update_live_performance.yml` | 매일 | `update_live_performance.py` | 라이브 성과 업데이트 |
| `build_sector_dist.yml` | 주기적 | `build_sector_dist.py` | 섹터 분포 업데이트 |
| `update_banners.yml` | 주기적 | `build_banners.py` | 포털 배너 갱신 |
| `update_news.yml` | 주기적 | `build_news.py` | 뉴스 피드 갱신 |
| `build_risk_snapshot.yml` | 주기적 | `build_risk_snapshot.py` | 리스크 스냅샷 |
| `build_sep_cache.yml` | 주 1회 | `build_sep_cache.py` | SEP 가격 캐시 갱신 |
| `build_sharadar_signals.yml` | 주 1회 | SF3/SF2 빌더 | 기관·내부자 신호 갱신 |
| `build_fundmentals.yml` | 주기적 | `build_fundamentals.py` | 재무지표 갱신 |

**필요한 GitHub Secrets**

```
NASDAQ_DATA_LINK_KEY   Nasdaq Data Link API 키
FINNHUB_API_KEY        Finnhub API 키
ALPACA_API_KEY         Alpaca API 키
ALPACA_SECRET_KEY      Alpaca Secret 키
TELEGRAM_BOT_TOKEN     텔레그램 알림 봇
TELEGRAM_CHAT_ID       텔레그램 채팅 ID
```

---

## 10. 데이터 파일 레퍼런스

`data/` 폴더 파일 목록 (git 미추적, 로컬에서만 관리).

### 대용량 PKL (핵심 데이터)

| 파일 | 크기 | 내용 |
|------|------|------|
| `daily_history.pkl` | ~300MB | 전 종목 일별 밸류에이션 시계열 |
| `sep_prices.pkl` | ~41MB | 전 종목 일별 조정 가격 |
| `sf2_history.pkl` | ~15MB | 내부자 Form4 거래 이력 |
| `sf3_history.pkl` | ~0.8MB | 기관 13F 보유 이력 |
| `inst_neutral_history.pkl` | ~0.4MB | 시가총액 통제 기관 크라우딩 |
| `xgb_factor_model.pkl` | ~0.2MB | XGBoost 팩터 모델 |

### 양자 모델 파라미터

| 파일 | 내용 |
|------|------|
| `quantum_vqc_ae_params.pkl` | AE-VQC 앙상블 파라미터 (list of 5) |
| `quantum_vqc_ae_norm.pkl` | 정규화 mu/std |
| `quantum_vqc_ae_meta.json` | 회로 메타 정보 |
| `quantum_vqc_params.pkl` | 구 VQC 파라미터 |
| `quantum_vqc_norm.pkl` | 구 VQC 정규화 |

### 백테스트 결과 JSON

| 파일 | 내용 |
|------|------|
| `backtest_regime_result.json` | 레짐 전략 IS/OOS 성과 (메인 전략) |
| `equity_curve_regime.json` | 레짐 전략 일별 자산 곡선 |
| `quantum_backtest_ae_result.json` | AE-VQC 단독 백테스트 결과 |
| `quantum_backtest_regime_ae_result.json` | AE-VQC + 레짐 결합 결과 |
| `experiment_log.json` | 전체 실험 누적 로그 |
| `backtest_result.json` | 기본 백테스트 결과 |
| `baseline_crossval_result.json` | 단일팩터 baseline 검증 결과 (timestamp·settings 포함) |

### 팩터 분석 (data/alphalens/)

| 파일 | 내용 |
|------|------|
| `alphalens/factor_summary.json` | 16개 팩터별 IC/IR/hit rate (IS 기간) |
| `alphalens/<factor>_tearsheet.png` | alphalens 시각화 차트 |

### 스냅샷 JSON (포털 피드)

| 파일 | 포털 경로 | 내용 |
|------|----------|------|
| `score_snapshot.json` | `public/data/` | 현재 종목 점수 + 포트폴리오 |
| `risk_snapshot.json` | `public/data/` | 리스크 지표 |
| `market_regime.json` | `public/data/` | 시장 레짐 상태 |
| `live_performance.json` | `public/data/` | 라이브 성과 |
| `banners.json` | `public/data/` | 포털 배너 데이터 |

---

## 11. IS/OOS 설계 원칙

이 시스템의 핵심 원칙: **IS 기간에서 발견한 패턴이 OOS에서도 유지되는가.**

```
IS  (In-Sample)  : 2014-05-01 ~ 2019-12-31  ← 알파 탐색, 파라미터 최적화
OOS (Out-of-Sample): 2020-01-01 ~ 오늘       ← 검증 전용, 파라미터 고정
```

**절대 금지 사항**

- OOS 기간 데이터로 파라미터를 조정하거나 신호를 재학습하는 것
- `IS_START`, `IS_END`, `OOS_START` 상수 수정 (CLAUDE.md에 고정)
- 학습 라벨에 OOS 가격 포함 (A-1 버그가 이것 — 수정 완료)

**일반화 ratio 해석**

```
일반화 ratio = OOS Sharpe / IS Sharpe
≈ 1.0   → 과적합 없음, 진짜 알파
< 0.5   → 과적합 의심
> 1.0   → IS가 보수적으로 추정됨 (좋은 신호)
```

---

## 12. 개발 규칙 요약

(CLAUDE.md 전문 참조)

### 규칙 1 — 데이터 로딩은 BacktestEngine 한 곳에서만

`sep_prices.pkl`, `daily_history.pkl` 등을 스크립트에서 직접 열지 않는다.
반드시 `BacktestEngine().load_data()`를 통해 접근.

### 규칙 2 — 새 팩터는 FACTOR_UNIVERSE 확장으로

`run_backtest_new.py`의 `FACTOR_UNIVERSE` dict에 추가 + `_raw_signal()`에 type 분기 추가.
신호 계산 로직을 별도 스크립트에 복사하지 않는다 (PIT 무결성 위협).

### 규칙 3 — 실험 결과는 반드시 JSON으로 저장

`experiment_harness.py`의 `run_experiments()`를 통해 실행.
결과는 `data/experiment_log.json`에 누적 기록.

### 추가 코딩 가이드

- **광범위한 `except Exception`** 사용 금지 → 예외 타입 명시 (`ValueError`, `KeyError` 등)
- **`assert`** 대신 `if ... raise ValueError` 사용 (python -O에서 assert는 비활성화)
- 모든 팩터 계산은 `as_of` 날짜 이전 데이터만 사용 (PIT)
- SF3 13F 데이터는 45일 filing lag 적용 필수

---

## 빠른 시작 체크리스트

```bash
# 1. 환경 설정
pip install pennylane numpy pandas scipy yfinance alpaca-trade-api alphalens-reloaded gs-quant

# 2. 데이터 캐시 빌드 (처음 한 번만)
python scripts/build_sep_cache.py
python scripts/build_daily_history.py
python scripts/build_sf3_history.py
python scripts/build_sf2_history.py
python scripts/build_sp500_membership.py
python scripts/build_sp400_membership.py
python scripts/build_sp600_membership.py
python scripts/build_inst_neutral_history.py

# 3. AE-VQC 앙상블 학습
python scripts/train_ae_vqc_ensemble.py

# 4. 백테스트 실행
python scripts/run_backtest_new.py

# 5. 스냅샷 생성
python scripts/build_score_snapshot.py

# 6. 팩터 IC 분석 (선택)
python scripts/build_alphalens_factor.py

# 7. 교차검증 (선택)
python scripts/baseline_crossvalidate.py --factor insider
```

---

*최종 수정: 2026-05-19 (inst_smart_proxy 팩터 추가: 사이즈 통제 미발견 × HHI 스마트머니 집중도 신호)*
