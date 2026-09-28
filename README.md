# Invest Data

**미국 주식의 팩터 점수, 과거 검증, 일별 스냅샷을 연결하는 Python 퀀트 리서치 파이프라인입니다.** 양자 회로 실험보다 데이터의 시점 관리와 검증 절차를 이 저장소의 중심에 둡니다.

[시각화 포털](https://invest-portal-rust.vercel.app) · [구조·파일별 설명](PROJECT_GUIDE.md) · [자동화 워크플로](.github/workflows/)

## 데이터 흐름

`가격·재무·공시·지수 구성 자료 → 날짜별 팩터 계산 → 단면 점수·포트폴리오 → IS/OOS 백테스트·진단 → JSON 스냅샷 → invest-portal`

주요 입력은 Nasdaq Data Link의 SHARADAR 가격·재무·밸류에이션·13F·Form 4 자료, 지수 편입 이벤트, yfinance 시세, Finnhub 뉴스입니다. Alpaca 연동은 별도의 **페이퍼 주문 실험**에 쓰입니다. 일부 입력은 유료 접근권한과 로컬 캐시가 필요합니다.

## 채용 직무와 연결되는 구현

| 영역 | 저장소에서 확인할 곳 |
| --- | --- |
| 수집·가공 | `scripts/build_*`, `data/`의 날짜별 JSON 산출물 |
| 팩터·백테스트 | `scripts/run_backtest_new.py`의 모멘텀, 밸류에이션, 재무건전성, 기관·내부자 신호와 IS/OOS 분리 |
| 시점 통제 | 리밸런싱 날짜까지의 가격만 슬라이스하고, 지수 편입 이벤트를 역산하는 유니버스 함수 |
| 검증 | `scripts/build_alphalens_factor.py`의 IC, `scripts/compute_dsr_crowding.py`의 다중 탐색 보정 지표, `scripts/validation_outputs/`의 보유 구간·민감도·워크포워드 산출물 |
| 자동화 | `.github/workflows/`의 점수·리스크·재무·뉴스 스냅샷 갱신과 페이퍼 리밸런싱 |

`run_backtest_new.py`에는 2014-05~2019-12의 IS와 2020-01 이후 OOS 구간이 정의돼 있습니다. 결과 JSON과 차트는 **과거 시뮬레이션**이며, `invest-portal`의 사전등록된 Top-5 **전향 페이퍼 실험**과 분리해 해석해야 합니다. `data/live_performance.json` 같은 파일명만으로 실계좌 성과를 뜻하지 않습니다.

## 확인된 제약

과거 편입 이벤트를 역산하는 코드는 있으나 가격 로더는 현재 S&P 500/400/600 구성 종목에서 출발합니다. 이 상태만으로 과거 탈락·상장폐지 종목의 가격까지 완전하게 포함한다고 보장할 수 없어 **생존편향 제거 완료**라고 주장하지 않습니다. 13F의 45일 지연은 일부 기관 보유 신호에 적용되지만 `institutional` 신호 경로에는 같은 지연이 보이지 않습니다. 이 경로의 미래정보 사용 가능성은 별도 감사가 필요합니다. 따라서 개별 백테스트 수치는 PIT 무결성을 추가 확인하기 전까지 잠정 결과입니다.

`PROJECT_GUIDE.md`에는 현재 트리에 없는 스크립트·캐시를 전제로 한 설명도 있습니다. 재현 가능한 범위는 실제 코드와 입력 파일을 기준으로 확인해야 합니다. 또한 공개 JSON 중 SHARADAR에서 파생된 종목별 자료의 재배포 권한은 별도 확인이 필요합니다.

## 실행

Python 3.11을 사용하는 워크플로가 있습니다. 준비된 스냅샷 갱신은 해당 워크플로의 의존성·환경변수를 설정한 뒤 `python scripts/build_score_snapshot.py`로 실행합니다. 과거 백테스트는 `python scripts/run_backtest_new.py`를 사용하지만, 필요한 유료 원천 자료와 로컬 캐시가 공개 저장소에 모두 있지 않아 새 클론만으로 전체 재현은 되지 않습니다. 포털 코드는 별도 [invest-portal](https://github.com/wjdrjs09076-ops/invest-portal)에 있습니다.

**연구 원칙:** 사전 기준을 고정하고, 날짜별로 검증하며, 반례와 중단한 가설을 기록합니다.

