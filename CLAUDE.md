# 프로젝트 하네스 규칙

## 프로젝트 개요
미국 S&P1500 주식시장 양자영감 알파 백테스트 시스템.
- 엔진: `scripts/run_backtest_new.py` — BacktestEngine
- 양자 신호: `scripts/quantum_signal.py` — VQCSignal
- 양자 최적화: `scripts/quantum_optimizer.py` — QuantumPortfolioOptimizer
- 실험 하네스: `scripts/experiment_harness.py`
- 데이터: `data/` (pkl/json, git 추적 안 함)

## 코드 생성 규칙

### 규칙 1 — 데이터 로딩은 BacktestEngine 한 곳에서만
새 스크립트에서 sep_prices.pkl, daily_history.pkl 등을 직접 열지 않는다.
`BacktestEngine().load_data()` 를 통해 접근한다.

**이유**: 스크립트마다 로딩 코드가 중복되면 경로·포맷 변경 시 전부 수정해야 한다.

### 규칙 2 — 새 팩터/신호는 FACTOR_UNIVERSE 확장으로
`run_backtest_new.py`의 `FACTOR_UNIVERSE` dict에 추가하고,
`_raw_signal()` 에 type 분기를 추가한다.
별도 스크립트에 신호 계산 로직을 복사하지 않는다.

**이유**: 신호 계산 로직이 두 곳에 존재하면 PIT(Point-In-Time) 무결성 보장이 불가능하다.

### 규칙 3 — 실험은 config dict로, 결과는 반드시 JSON으로 저장
`experiment_harness.py`의 `run_experiments()` 를 통해 실행한다.
결과는 `data/experiment_log.json`에 누적 기록된다.

**이유**: 어떤 실험을 언제 어떤 파라미터로 돌렸는지 재현 가능해야 한다.

### 규칙 4 — 새 스크립트 작성 즉시 PROJECT_GUIDE.md 업데이트
새 스크립트를 만들거나 기존 스크립트에 중요한 기능을 추가하면,
해당 작업과 같은 세션 내에 `PROJECT_GUIDE.md`에 설명을 기록한다.
위치: 스크립트 성격에 맞는 섹션(데이터 빌더→5절, 분석/검증→7절, 출력 파일→10절).

**이유**: PROJECT_GUIDE.md는 처음 보는 개발자/AI 에이전트가 구조를 파악하는 유일한 문서다.
새 파일이 추가되는 순간 문서가 낡아지면 문서 자체가 신뢰를 잃는다.

### 규칙 추가 기준
새 규칙을 추가하기 전에 반드시 두 질문에 답한다:
1. 이 규칙이 없으면 실제로 어떤 문제가 생기는가? (구체적으로)
2. 이 규칙이 작업 속도를 얼마나 낮추는가?

문제가 구체적이고 작업 지연이 작으면 추가. 그렇지 않으면 기각.

## 데이터 파이프라인 실행 순서
```
1. build_sp400_membership.py   # SP400 멤버십 이벤트
2. build_sp600_membership.py   # SP600 멤버십 이벤트
3. build_score_snapshot.py     # 점수 스냅샷
4. build_daily_history.py      # SHARADAR/DAILY PIT 캐시
5. build_sf3_history.py        # SF3 13F 기관 보유 캐시
6. build_sf2_history.py        # SF2 Form4 내부자 거래 캐시
7. quantum_signal.py           # VQC 학습 → quantum_vqc_params.pkl
```

## IS/OOS 기간 (절대 수정 금지)
- IS : 2014-05-01 ~ 2019-12-31
- OOS: 2020-01-01 ~ 오늘
