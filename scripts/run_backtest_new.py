#!/usr/bin/env python3
"""
run_backtest_new.py — 미국 주식시장 복제 백테스트 엔진

목적: 시장 환경을 최대한 정밀하게 복제한 실행 엔진.
      "어떤 모델이 좋은가"는 이 파일이 결정하지 않는다.
      모델(팩터 가중치)은 외부에서 주입한다.

핵심 API:
    engine = BacktestEngine()          # 데이터 1회 로드
    result = engine.run(weights)       # 가중치 넣으면 IS/OOS 성과 반환
    result.is_sharpe / result.oos_sharpe / result.is_metrics / result.oos_metrics

사용 가능한 팩터 신호 (FACTOR_UNIVERSE 참고):
    가격 계열  : mom12_1, mom9_1, mom6_1, mom3_1, mom1, rs_spy_12m, rs_spy_6m, rs_spy_3m
    밸류에이션 : evebit, evebitda, pb, pe, ps
    퀄리티     : zscore
    기관 보유  : institutional   (SF3 13F QoQ, PIT bisect)
    내부자     : insider         (SF2 Form4 롤링 12개월, PIT bisect)

IS  기간: 2014-05-01 ~ 2019-12-31  (알파 탐색 · 파라미터 최적화)
OOS 기간: 2020-01-01 ~ 오늘        (검증 전용 — 파라미터 고정 후 성과)

기존 run_backtest_regime.py 는 일절 수정하지 않음.
"""
from __future__ import annotations

import bisect
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import yfinance as yf

ROOT            = Path(__file__).resolve().parents[1]
DATA_DIR        = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"

# ─────────────────────────────────────────────────────────────
# IS / OOS 기간 (엔진 고정값)
# ─────────────────────────────────────────────────────────────
IS_START  = "2014-05-01"
IS_END    = "2019-12-31"
OOS_START = "2020-01-01"
OOS_END   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ─────────────────────────────────────────────────────────────
# FACTOR_UNIVERSE — 사용 가능한 모든 팩터 신호 정의
# (가중치 없음 — 모델이 결정)
#
# type:
#   "price"          — SEP 가격 기반 수익률 (lookback 일봉, skip 일봉 제외)
#   "price_vs_bench" — 위와 동일하되 SPY 초과수익률
#   "daily"          — SHARADAR/DAILY EV/EBIT 등 (key, cap 지정)
#   "zscore"         — SF1 Altman Z-score PIT bisect
#   "institutional"  — SF3 13F QoQ 기관 순매수 (lookback_qtrs)
#   "insider"        — SF2 Form4 내부자 순매수 비율 (lookback_days)
#
# rank: "high_good" (높을수록 좋음) | "low_good" (낮을수록 좋음)
# cap:  [min, max]  이 범위 밖의 값은 신호 없음(None) 처리
# ─────────────────────────────────────────────────────────────
FACTOR_UNIVERSE: dict[str, dict] = {
    # ── 가격 모멘텀 ───────────────────────────────────────────
    "mom12_1":  {"type": "price",          "lookback": 252, "skip": 21,  "rank": "high_good"},
    "mom9_1":   {"type": "price",          "lookback": 189, "skip": 21,  "rank": "high_good"},
    "mom6_1":   {"type": "price",          "lookback": 126, "skip": 21,  "rank": "high_good"},
    "mom3_1":   {"type": "price",          "lookback": 63,  "skip": 21,  "rank": "high_good"},
    "mom1":     {"type": "price",          "lookback": 21,  "skip": 0,   "rank": "high_good"},
    # ── 상대강도 vs SPY ───────────────────────────────────────
    "rs_spy_12m": {"type": "price_vs_bench", "lookback": 252, "skip": 21, "rank": "high_good"},
    "rs_spy_6m":  {"type": "price_vs_bench", "lookback": 126, "skip": 21, "rank": "high_good"},
    "rs_spy_3m":  {"type": "price_vs_bench", "lookback": 63,  "skip": 21, "rank": "high_good"},
    # ── 밸류에이션 (SHARADAR/DAILY PIT) ──────────────────────
    "evebit":   {"type": "daily", "key": "evebit",   "rank": "low_good",  "cap": [0.5,  150.0]},
    "evebitda": {"type": "daily", "key": "evebitda", "rank": "low_good",  "cap": [1.0,  80.0]},
    "pb":       {"type": "daily", "key": "pb",       "rank": "low_good",  "cap": [0.1,  50.0]},
    "pe":       {"type": "daily", "key": "pe",       "rank": "low_good",  "cap": [1.0,  100.0]},
    "ps":       {"type": "daily", "key": "ps",       "rank": "low_good",  "cap": [0.1,  30.0]},
    # ── 퀄리티 (SF1 Altman Z-score PIT) ──────────────────────
    "zscore":   {"type": "zscore",        "rank": "high_good"},
    # ── 기관 보유 변화 (SF3 13F QoQ, PIT bisect) ─────────────
    "institutional": {"type": "institutional", "rank": "high_good", "lookback_qtrs": 2},
    # ── 기관 크라우딩 역이용 (SF3 보유 비율 낮은 종목 매수, 45일 파일링 딜레이 보정) ──
    "inst_crowding": {"type": "inst_crowding", "rank": "low_good", "filing_lag_days": 45},
    # ── 사이즈 중립 기관 크라우딩 (log(mcap) 회귀 잔차, 낮을수록 매수) ──────────────
    "inst_crowding_neutral": {"type": "inst_crowding_neutral", "rank": "low_good"},
    # ── SF3 투자자별 상세 신호 (build_sf3_detail_history.py 필요) ─────────────────
    "inst_new_holders": {"type": "inst_detail", "key": "new_holders_rate", "rank": "high_good", "filing_lag_days": 45},
    "inst_n_holders_chg": {"type": "inst_detail", "key": "n_holders_chg",  "rank": "high_good", "filing_lag_days": 45},
    "inst_hhi":           {"type": "inst_detail", "key": "hhi",            "rank": "low_good",  "filing_lag_days": 45},
    # ── 스마트머니 집중도: 사이즈 중립 미발견 × HHI (소수 기관 집중 보유 + 전체 미발견) ──
    "inst_smart_proxy":  {"type": "inst_smart_proxy", "rank": "high_good", "filing_lag_days": 45},
    # ── 섹터+사이즈 이중 중립 크라우딩 (build_sector_neutral_history.py 필요) ─────────
    "inst_crowding_sector_neutral": {"type": "inst_crowding_sector_neutral", "rank": "low_good"},
    # ── 내부자 순매수 (SF2 Form4, 롤링 12개월, PIT bisect) ────
    "insider":  {"type": "insider",       "rank": "high_good", "lookback_days": 365},
    # ── S&P 방법론 기반 신용 점수 (build_credit_score_history.py 필요) ──────────
    # 0~100점: 높을수록 신용 우량. AAA/AA≥80, A≥65, BBB≥50, BB≥35, B≥20, CCC<20
    "credit_quality": {"type": "credit_quality", "rank": "high_good"},
    # ── 양자영감 ML (VQC AngleEmbedding, 6큐비트, 6팩터) ─────
    "quantum_ml":    {"type": "quantum_ml",    "rank": "high_good"},
    # ── 양자영감 ML (VQC AmplitudeEmbedding, 4큐비트, 16팩터) ─
    "quantum_ml_ae": {"type": "quantum_ml_ae", "rank": "high_good"},
}

# ─────────────────────────────────────────────────────────────
# 실행 파라미터 (포트폴리오 구성 · 레짐)
# ─────────────────────────────────────────────────────────────
TOP_N             = 15
HOLD_BUFFER_N     = 25
TRANSACTION_COST  = 0.0015
BENCHMARK         = "SPY"
VIX_TICKER        = "^VIX"
VIX_CRASH_THRESHOLD = 40.0
MIN_HISTORY       = 220
SECTOR_MAX_NAMES  = 3
MIN_WEIGHT        = 0.05
MAX_WEIGHT        = 0.20
VOL_FALLBACK      = 0.35
VOL_FLOOR         = 0.18
SCORE_ALPHA       = 2.5
ABS_MOM_63D_MIN   = 0.0
ABS_MOM_252D_MIN  = 0.0

USE_REGIME_FILTER   = True
REGIME_MA_WINDOW    = 200
REGIME_MOM_WINDOW   = 63
REGIME_BUFFER       = 0.005
REGIME_CONFIRM_DAYS = 2
RISK_ON_EXPOSURE    = 1.00
MID_EXPOSURE        = 0.85
RISK_OFF_EXPOSURE   = 0.40
DEFENSIVE_TICKERS   = ["TAIL", "DBMF"]
DEFENSIVE_WEIGHTS   = {"TAIL": 0.3, "DBMF": 0.7}

# ─────────────────────────────────────────────────────────────
# 파일 경로
# ─────────────────────────────────────────────────────────────
SEP_PRICES_FILE       = DATA_DIR / "sep_prices.pkl"
DAILY_HISTORY_FILE    = DATA_DIR / "daily_history.pkl"
ZSCORE_HISTORY_FILE   = DATA_DIR / "zscore_history.json"
SF3_HISTORY_FILE      = DATA_DIR / "sf3_history.pkl"
SF3_DETAIL_FILE       = DATA_DIR / "sf3_detail_history.pkl"
INST_NEUTRAL_FILE        = DATA_DIR / "inst_neutral_history.pkl"
SECTOR_NEUTRAL_FILE      = DATA_DIR / "sector_neutral_history.pkl"
CREDIT_SCORE_FILE        = DATA_DIR / "credit_score_history.pkl"
SF2_HISTORY_FILE      = DATA_DIR / "sf2_history.pkl"
SP500_SHARADAR_EVENTS = DATA_DIR / "sp500_membership_events_sharadar.json"
SP500_WIKI_EVENTS     = DATA_DIR / "sp500_membership_events.json"
SP400_EVENTS          = DATA_DIR / "sp400_membership_events.json"
SP600_EVENTS          = DATA_DIR / "sp600_membership_events.json"
SP500_CURRENT_WIKI    = DATA_DIR / "sp500_current_wiki.json"
SP400_CURRENT_WIKI    = DATA_DIR / "sp400_current_wiki.json"
SP600_CURRENT_WIKI    = DATA_DIR / "sp600_current_wiki.json"


# ═════════════════════════════════════════════════════════════
# 성과 결과 컨테이너
# ═════════════════════════════════════════════════════════════

@dataclass
class BacktestResult:
    weights:     dict[str, float]
    is_metrics:  dict[str, float]
    oos_metrics: dict[str, float]
    full_metrics: dict[str, float]
    history:     list[dict] = field(repr=False, default_factory=list)

    @property
    def is_sharpe(self)  -> float: return self.is_metrics.get("sharpe", 0.0)
    @property
    def oos_sharpe(self) -> float: return self.oos_metrics.get("sharpe", 0.0)
    @property
    def is_cagr(self)    -> float: return self.is_metrics.get("cagr", 0.0)
    @property
    def oos_cagr(self)   -> float: return self.oos_metrics.get("cagr", 0.0)
    @property
    def is_mdd(self)     -> float: return self.is_metrics.get("max_drawdown", 0.0)
    @property
    def oos_mdd(self)    -> float: return self.oos_metrics.get("max_drawdown", 0.0)

    def verdict(self) -> str:
        if self.is_sharpe <= 0:
            return "N/A"
        r = self.oos_sharpe / self.is_sharpe
        if r >= 0.70:
            return f"PASS    (OOS/IS Sharpe={r:.2f})"
        elif r >= 0.50:
            return f"MARGINAL (OOS/IS Sharpe={r:.2f})"
        else:
            return f"FAIL    (OOS/IS Sharpe={r:.2f})"

    def summary(self) -> str:
        lines = [
            f"weights : {self.weights}",
            f"IS  ({IS_START}~{IS_END}): "
            f"CAGR={self.is_cagr:+.2%}  Sharpe={self.is_sharpe:.2f}  MDD={self.is_mdd:.2%}",
            f"OOS ({OOS_START}~{OOS_END}): "
            f"CAGR={self.oos_cagr:+.2%}  Sharpe={self.oos_sharpe:.2f}  MDD={self.oos_mdd:.2%}",
            f"verdict : {self.verdict()}",
        ]
        return "\n".join(lines)


# ═════════════════════════════════════════════════════════════
# PIT 신호 룩업 (bisect 기반)
# ═════════════════════════════════════════════════════════════

class _DailyLookup:
    def __init__(self):
        self._d: dict[str, dict] = {}
        self._ok = False

    def load(self):
        if self._ok:
            return
        self._ok = True
        if not DAILY_HISTORY_FILE.exists():
            print("[WARN] daily_history.pkl 없음 — build_daily_history.py 실행 필요")
            return
        payload   = pd.read_pickle(DAILY_HISTORY_FILE)
        self._d   = payload.get("lookup", {}) if isinstance(payload, dict) else {}
        print(f"[DAILY] {len(self._d)} tickers loaded")

    def get(self, ticker: str, key: str, as_of: pd.Timestamp) -> float | None:
        rec = self._d.get(ticker)
        if not rec:
            return None
        dates = rec["dates"]
        vals  = rec.get(key, [])
        idx   = bisect.bisect_right(dates, str(as_of.date())) - 1
        if idx < 0 or idx >= len(vals):
            return None
        v = vals[idx]
        return float(v) if v is not None else None


class _ZscoreLookup:
    def __init__(self):
        self._d: dict[str, list] = {}
        self._ok = False

    def load(self):
        if self._ok:
            return
        self._ok = True
        if not ZSCORE_HISTORY_FILE.exists():
            print("[WARN] zscore_history.json 없음")
            return
        with open(ZSCORE_HISTORY_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for ticker, recs in data.get("history", {}).items():
            t = ticker.upper().replace(".", "-")
            self._d[t] = sorted(recs, key=lambda r: str(r.get("datekey", "")))
        print(f"[ZSCORE] {len(self._d)} tickers loaded")

    def get(self, ticker: str, as_of: pd.Timestamp) -> float | None:
        recs = self._d.get(ticker)
        if not recs:
            return None
        keys = [str(r.get("datekey", "")) for r in recs]
        idx  = bisect.bisect_right(keys, str(as_of.date())) - 1
        if idx < 0:
            return None
        z = recs[idx].get("zscore")
        return float(z) if z is not None else None


class _SF3Lookup:
    def __init__(self):
        self._d: dict[str, dict] = {}
        self._ok = False

    def load(self):
        if self._ok:
            return
        self._ok = True
        if not SF3_HISTORY_FILE.exists():
            print("[WARN] sf3_history.pkl 없음 — build_sf3_history.py 실행 필요")
            return
        payload = pd.read_pickle(SF3_HISTORY_FILE)
        self._d = payload.get("lookup", {}) if isinstance(payload, dict) else {}
        print(f"[SF3] {len(self._d)} tickers loaded")

    def get_qoq(self, ticker: str, as_of: pd.Timestamp, lookback_qtrs: int = 2) -> float | None:
        rec = self._d.get(ticker)
        if not rec:
            return None
        quarters = rec["quarters"]
        values   = rec["values"]
        idx_last = bisect.bisect_right(quarters, str(as_of.date())) - 1
        if idx_last < 1:
            return None
        q_n, q_n1 = values[idx_last], values[idx_last - 1]
        if q_n is None or q_n1 is None or q_n1 <= 0:
            return None
        return float((q_n - q_n1) / q_n1)

    def get_level(self, ticker: str, as_of: pd.Timestamp) -> float | None:
        """가장 최근 분기 기관 보유 총 금액 (PIT)"""
        rec = self._d.get(ticker)
        if not rec:
            return None
        quarters = rec["quarters"]
        values   = rec["values"]
        idx = bisect.bisect_right(quarters, str(as_of.date())) - 1
        if idx < 0 or values[idx] is None:
            return None
        return float(values[idx])


class _SF3DetailLookup:
    """
    sf3_detail_history.pkl 기반 투자자별 상세 신호 PIT 조회.
    key: "new_holders_rate" | "n_holders_chg" | "hhi"
    """
    def __init__(self):
        self._d: dict[str, dict] = {}
        self._ok = False

    def load(self):
        if self._ok:
            return
        self._ok = True
        if not SF3_DETAIL_FILE.exists():
            print("[WARN] sf3_detail_history.pkl 없음 — build_sf3_detail_history.py 실행 필요")
            return
        payload  = pd.read_pickle(SF3_DETAIL_FILE)
        self._d  = payload.get("lookup", {}) if isinstance(payload, dict) else {}
        print(f"[SF3_DETAIL] {len(self._d)} tickers loaded")

    def get_signal(self, ticker: str, key: str, as_of: pd.Timestamp) -> float | None:
        rec = self._d.get(ticker)
        if not rec:
            return None
        quarters = rec["quarters"]
        idx = bisect.bisect_right(quarters, str(as_of.date())) - 1
        if idx < 0:
            return None

        if key == "hhi":
            v = rec["hhi"][idx]
            return float(v) if v is not None else None

        if key == "new_holders_rate":
            # 신규 진입 기관 수 / 전체 기관 수 (비율로 정규화)
            new = rec["new_holders"][idx]
            n   = rec["n_holders"][idx]
            if new is None or n is None or n == 0:
                return None
            return float(new) / float(n)

        if key == "n_holders_chg":
            # 기관 수 QoQ 변화율
            if idx < 1:
                return None
            n_cur  = rec["n_holders"][idx]
            n_prev = rec["n_holders"][idx - 1]
            if n_cur is None or n_prev is None or n_prev == 0:
                return None
            return float(n_cur - n_prev) / float(n_prev)

        return None


class _InstNeutralLookup:
    """사이즈 중립 기관 크라우딩 잔차 룩업 (inst_neutral_history.pkl)"""
    def __init__(self):
        self._d: dict[str, dict] = {}
        self._ok = False

    def load(self):
        if self._ok:
            return
        self._ok = True
        if not INST_NEUTRAL_FILE.exists():
            print("[WARN] inst_neutral_history.pkl 없음 — build_inst_neutral_history.py 실행 필요")
            return
        payload = pd.read_pickle(INST_NEUTRAL_FILE)
        self._d = payload.get("lookup", {}) if isinstance(payload, dict) else {}
        print(f"[INST_NEUTRAL] {len(self._d)} tickers loaded")

    def get(self, ticker: str, as_of: pd.Timestamp) -> float | None:
        rec = self._d.get(ticker)
        if not rec:
            return None
        quarters  = rec["quarters"]
        residuals = rec["residuals"]
        idx = bisect.bisect_right(quarters, str(as_of.date())) - 1
        if idx < 0:
            return None
        return float(residuals[idx])


class _CreditScoreLookup:
    """S&P 방법론 기반 신용 점수 룩업 (credit_score_history.pkl)
    포맷: {ticker: {date_str: score}}  date_str = datekey (SEC 공시일)
    """
    def __init__(self):
        self._d: dict[str, dict[str, float]] = {}
        self._ok = False

    def load(self):
        if self._ok:
            return
        self._ok = True
        if not CREDIT_SCORE_FILE.exists():
            print("[WARN] credit_score_history.pkl 없음 — build_credit_score_history.py 실행 필요")
            return
        self._d = pd.read_pickle(CREDIT_SCORE_FILE)
        print(f"[CREDIT] {len(self._d)} tickers loaded")

    def get(self, ticker: str, as_of: pd.Timestamp) -> float | None:
        rec = self._d.get(ticker)
        if not rec:
            return None
        dates = sorted(rec.keys())
        as_of_str = str(as_of.date())
        idx = bisect.bisect_right(dates, as_of_str) - 1
        if idx < 0:
            return None
        return float(rec[dates[idx]])


class _SectorNeutralLookup:
    """섹터+사이즈 이중 중립 기관 크라우딩 잔차 룩업 (sector_neutral_history.pkl)"""
    def __init__(self):
        self._d: dict[str, dict] = {}
        self._ok = False

    def load(self):
        if self._ok:
            return
        self._ok = True
        if not SECTOR_NEUTRAL_FILE.exists():
            print("[WARN] sector_neutral_history.pkl 없음 — build_sector_neutral_history.py 실행 필요")
            return
        payload = pd.read_pickle(SECTOR_NEUTRAL_FILE)
        self._d = payload.get("lookup", {}) if isinstance(payload, dict) else {}
        print(f"[SECTOR_NEUTRAL] {len(self._d)} tickers loaded")

    def get(self, ticker: str, as_of: pd.Timestamp) -> float | None:
        rec = self._d.get(ticker)
        if not rec:
            return None
        quarters  = rec["quarters"]
        residuals = rec["residuals"]
        idx = bisect.bisect_right(quarters, str(as_of.date())) - 1
        if idx < 0:
            return None
        return float(residuals[idx])


class _SF2Lookup:
    def __init__(self):
        self._d: dict[str, dict] = {}
        self._ok = False

    def load(self):
        if self._ok:
            return
        self._ok = True
        if not SF2_HISTORY_FILE.exists():
            print("[WARN] sf2_history.pkl 없음 — build_sf2_history.py 실행 필요")
            return
        payload = pd.read_pickle(SF2_HISTORY_FILE)
        self._d = payload.get("lookup", {}) if isinstance(payload, dict) else {}
        print(f"[SF2] {len(self._d)} tickers loaded")

    def get_net_ratio(self, ticker: str, as_of: pd.Timestamp, lookback_days: int = 365) -> float | None:
        rec = self._d.get(ticker)
        if not rec:
            return None
        dates  = rec["dates"]
        types  = rec["types"]
        values = rec["values"]
        as_of_str  = str(as_of.date())
        cut_str    = str((as_of - pd.Timedelta(days=lookback_days)).date())
        i_end   = bisect.bisect_right(dates, as_of_str)
        i_start = bisect.bisect_left(dates, cut_str)
        if i_end <= i_start:
            return None
        buy_v = sum(values[i] for i in range(i_start, i_end) if types[i] == "P")
        sell_v = sum(values[i] for i in range(i_start, i_end) if types[i] == "S")
        total = buy_v + sell_v
        if total <= 0:
            return None
        return float((buy_v - sell_v) / total)


# ═════════════════════════════════════════════════════════════
# 백테스트 엔진
# ═════════════════════════════════════════════════════════════

class BacktestEngine:
    """
    데이터를 1회 로드한 뒤 run(weights) 를 반복 호출할 수 있는 엔진.

    사용 예:
        engine = BacktestEngine()
        result = engine.run({"mom12_1": 0.6, "evebit": 0.4})
        print(result.summary())
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._data_loaded = False

        # PIT 룩업
        self._daily        = _DailyLookup()
        self._zscore       = _ZscoreLookup()
        self._sf3          = _SF3Lookup()
        self._sf3_detail   = _SF3DetailLookup()
        self._inst_neutral   = _InstNeutralLookup()
        self._sector_neutral = _SectorNeutralLookup()
        self._credit         = _CreditScoreLookup()
        self._sf2            = _SF2Lookup()

        # 시장 데이터
        self.price_map:  dict[str, pd.Series] = {}
        self.universe:   dict[str, dict]      = {}
        self.events:     list[dict]           = []
        self.bench:      pd.Series | None     = None
        self.vix:        pd.Series | None     = None
        self.trading_dates: pd.Index | None   = None

    # ──────────────────────────────────────────────────────────
    # 데이터 로드 (최초 1회)
    # ──────────────────────────────────────────────────────────

    def load_data(self):
        if self._data_loaded:
            return
        self._data_loaded = True

        self._log("데이터 로드 시작...")
        self.universe = self._load_universe()
        self.events   = self._load_events()
        self.price_map = self._load_prices(list(self.universe.keys()))
        self.bench = self.price_map.get(BENCHMARK)
        self.vix   = self.price_map.get(VIX_TICKER)
        if self.bench is None:
            raise RuntimeError("SPY 가격 데이터 없음")

        start_ts = pd.Timestamp(IS_START)
        end_ts   = pd.Timestamp(OOS_END)
        self.trading_dates = self.bench[
            (self.bench.index >= start_ts) & (self.bench.index <= end_ts)
        ].index

        self._daily.load()
        self._zscore.load()
        self._sf3.load()
        self._sf3_detail.load()
        self._inst_neutral.load()
        self._sector_neutral.load()
        self._credit.load()
        self._sf2.load()
        self._log(f"로드 완료: {len(self.price_map)} 시리즈, {len(self.trading_dates)} 거래일")

    def _log(self, msg: str):
        if self.verbose:
            print(msg)

    # ──────────────────────────────────────────────────────────
    # 양자 모듈 연동
    # ──────────────────────────────────────────────────────────

    def use_quantum_optimizer(
        self,
        top_n:         int   = TOP_N,
        risk_aversion: float = 0.3,
        penalty:       float = 5.0,
        solver:        str   = "auto",
    ) -> "BacktestEngine":
        """
        QUBO 기반 포트폴리오 최적화기 활성화.
        이후 run() 시 _rebalance() 에서 기존 score^alpha/vol 대신
        QUBO 최적화를 사용해 종목을 선택한다.
        """
        from quantum_optimizer import QuantumPortfolioOptimizer
        self._qubo = QuantumPortfolioOptimizer(
            top_n=top_n,
            risk_aversion=risk_aversion,
            penalty=penalty,
            solver=solver,
            score_alpha=SCORE_ALPHA,
            vol_floor=VOL_FLOOR,
            vol_fallback=VOL_FALLBACK,
        )
        self._log(f"[QUBO] 포트폴리오 최적화기 활성화 (solver={self._qubo.solver_name})")
        return self

    def use_quantum_signal(self) -> "BacktestEngine":
        """
        VQC 신호 활성화 (AngleEmbedding, 6큐비트, 6팩터).
        quantum_vqc_params.pkl 이 존재해야 함 (quantum_signal.py 로 학습 필요).
        weights 에 'quantum_ml' 을 포함하면 자동으로 이 신호를 사용.
        """
        from quantum_signal import VQCSignal
        self._vqc = VQCSignal()
        if not self._vqc.load():
            self._log("[VQC] 파라미터 없음 — quantum_signal.py 로 먼저 학습하세요.")
            self._vqc = None
        return self

    def use_quantum_signal_ae(self) -> "BacktestEngine":
        """
        AE-VQC 신호 활성화 (AmplitudeEmbedding, 4큐비트, 16팩터).
        quantum_vqc_ae_params.pkl 이 존재해야 함 (quantum_signal_ae.py 로 학습 필요).
        weights 에 'quantum_ml_ae' 를 포함하면 자동으로 이 신호를 사용.
        """
        from quantum_signal_ae import AEVQCSignal
        self._vqc_ae = AEVQCSignal()
        if not self._vqc_ae.load():
            self._log("[AE-VQC] 파라미터 없음 — quantum_signal_ae.py 로 먼저 학습하세요.")
            self._vqc_ae = None
        return self

    # ──────────────────────────────────────────────────────────
    # 메인 API
    # ──────────────────────────────────────────────────────────

    def run(self, weights: dict[str, float]) -> BacktestResult:
        """
        weights: {factor_name: weight, ...}  — 합계가 1이 될 필요 없음, 내부에서 정규화
        factor_name은 FACTOR_UNIVERSE에 정의된 키여야 함.

        반환: BacktestResult (is_metrics, oos_metrics, is_sharpe, oos_sharpe 등)
        """
        self.load_data()

        # 사용할 팩터만 걸러냄
        active = {k: v for k, v in weights.items() if k in FACTOR_UNIVERSE and v > 0}
        if not active:
            raise ValueError(f"유효한 팩터가 없음. FACTOR_UNIVERSE 키: {list(FACTOR_UNIVERSE)}")
        w_total = sum(active.values())
        norm_w  = {k: v / w_total for k, v in active.items()}

        history = self._simulate(norm_w)
        df = pd.DataFrame(history)
        df["date"] = pd.to_datetime(df["date"])

        is_mask  = (df["date"] >= pd.Timestamp(IS_START))  & (df["date"] <= pd.Timestamp(IS_END))
        oos_mask = (df["date"] >= pd.Timestamp(OOS_START)) & (df["date"] <= pd.Timestamp(OOS_END))

        return BacktestResult(
            weights      = norm_w,
            is_metrics   = _calc_metrics(df[is_mask]) if is_mask.any()  else {},
            oos_metrics  = _calc_metrics(df[oos_mask]) if oos_mask.any() else {},
            full_metrics = _calc_metrics(df),
            history      = history,
        )

    def run_adaptive(self, weights_fn: "Callable[[str], dict[str, float]]") -> BacktestResult:
        """
        weights_fn: 레짐('risk_on'|'mid'|'risk_off') → 팩터 가중치 dict
        월간 리밸런싱 시점마다 당시 레짐에 맞는 가중치로 스코어링.
        """
        self.load_data()
        history = self._simulate_adaptive(weights_fn)
        df = pd.DataFrame(history)
        df["date"] = pd.to_datetime(df["date"])

        is_mask  = (df["date"] >= pd.Timestamp(IS_START))  & (df["date"] <= pd.Timestamp(IS_END))
        oos_mask = (df["date"] >= pd.Timestamp(OOS_START)) & (df["date"] <= pd.Timestamp(OOS_END))

        rep_raw = weights_fn("risk_on")
        rep_active = {k: v for k, v in rep_raw.items() if k in FACTOR_UNIVERSE and v > 0}
        rep_total  = sum(rep_active.values())
        rep_norm   = {k: v / rep_total for k, v in rep_active.items()} if rep_total > 0 else {}

        return BacktestResult(
            weights      = rep_norm,
            is_metrics   = _calc_metrics(df[is_mask]) if is_mask.any()  else {},
            oos_metrics  = _calc_metrics(df[oos_mask]) if oos_mask.any() else {},
            full_metrics = _calc_metrics(df),
            history      = history,
        )

    # ──────────────────────────────────────────────────────────
    # 시뮬레이션 루프
    # ──────────────────────────────────────────────────────────

    def _simulate(self, norm_w: dict[str, float]) -> list[dict]:
        regime_engine = _RegimeEngine(self.bench) if USE_REGIME_FILTER else None
        rebalance_set = set(_monthly_dates(self.trading_dates))

        equity, hwm = 1.0, 1.0
        history: list[dict] = []

        cur_stock:  dict[str, float] = {}
        cur_total:  dict[str, float] = {}
        cur_exp:    float            = RISK_ON_EXPOSURE
        cur_regime: str              = "risk_on"

        pend_stock:  dict[str, float] | None = None
        pend_total:  dict[str, float] | None = None
        pend_exp:    float | None            = None
        pend_regime: str | None              = None
        pend_cost:   bool                    = False
        prev_month:  str                     = ""

        n = len(self.trading_dates)
        for i, date in enumerate(self.trading_dates):
            if self.verbose and (i % 500 == 0 or i == n - 1):
                print(f"  [{date.date()}] equity={equity:.4f}  {i+1}/{n}")

            # 1-day lag 적용
            if pend_total is not None:
                if pend_cost:
                    equity *= (1 - TRANSACTION_COST)
                cur_stock  = pend_stock or {}
                cur_exp    = pend_exp if pend_exp is not None else cur_exp
                cur_total  = pend_total
                cur_regime = pend_regime or cur_regime
                pend_stock = pend_total = pend_exp = pend_regime = None
                pend_cost  = False

            # 일별 수익
            dr    = _daily_return(self.price_map, cur_total, date)
            equity *= (1 + dr)
            hwm    = max(hwm, equity)
            history.append({
                "date":           str(date.date()),
                "equity":         float(equity),
                "daily_return":   float(dr),
                "drawdown":       float(equity / hwm - 1),
                "stock_exposure": float(cur_exp),
                "regime":         cur_regime,
                "holdings":       sorted(cur_stock.keys()),
            })

            tgt_stock  = cur_stock
            tgt_exp    = cur_exp
            tgt_regime = cur_regime

            # 월간 리밸런싱
            if date in rebalance_set:
                month_key = date.strftime("%Y-%m")
                if month_key != prev_month:
                    prev_month = month_key
                    tgt_stock  = self._rebalance(date, cur_stock, norm_w)

            # 레짐 (매일)
            if regime_engine:
                tgt_regime, tgt_exp = regime_engine.update(date, self.vix)

            tgt_total, _ = _build_total(tgt_stock, tgt_exp)
            will_change  = not _eq_weights(tgt_total, cur_total)

            pend_stock  = tgt_stock
            pend_total  = tgt_total
            pend_exp    = tgt_exp
            pend_regime = tgt_regime
            pend_cost   = will_change

        return history

    def _simulate_adaptive(self, weights_fn: "Callable[[str], dict[str, float]]") -> list[dict]:
        regime_engine = _RegimeEngine(self.bench) if USE_REGIME_FILTER else None
        rebalance_set = set(_monthly_dates(self.trading_dates))

        equity, hwm = 1.0, 1.0
        history: list[dict] = []

        cur_stock:  dict[str, float] = {}
        cur_total:  dict[str, float] = {}
        cur_exp:    float            = RISK_ON_EXPOSURE
        cur_regime: str              = "risk_on"

        pend_stock:  dict[str, float] | None = None
        pend_total:  dict[str, float] | None = None
        pend_exp:    float | None            = None
        pend_regime: str | None              = None
        pend_cost:   bool                    = False
        prev_month:  str                     = ""

        n = len(self.trading_dates)
        for i, date in enumerate(self.trading_dates):
            if self.verbose and (i % 500 == 0 or i == n - 1):
                print(f"  [{date.date()}] equity={equity:.4f}  {i+1}/{n}")

            if pend_total is not None:
                if pend_cost:
                    equity *= (1 - TRANSACTION_COST)
                cur_stock  = pend_stock or {}
                cur_exp    = pend_exp if pend_exp is not None else cur_exp
                cur_total  = pend_total
                cur_regime = pend_regime or cur_regime
                pend_stock = pend_total = pend_exp = pend_regime = None
                pend_cost  = False

            dr = _daily_return(self.price_map, cur_total, date)
            equity *= (1 + dr)
            hwm = max(hwm, equity)
            history.append({
                "date":           str(date.date()),
                "equity":         float(equity),
                "daily_return":   float(dr),
                "drawdown":       float(equity / hwm - 1),
                "stock_exposure": float(cur_exp),
                "regime":         cur_regime,
                "holdings":       sorted(cur_stock.keys()),
            })

            tgt_stock  = cur_stock
            tgt_exp    = cur_exp
            tgt_regime = cur_regime

            if date in rebalance_set:
                month_key = date.strftime("%Y-%m")
                if month_key != prev_month:
                    prev_month = month_key
                    raw_w  = weights_fn(cur_regime)
                    active = {k: v for k, v in raw_w.items() if k in FACTOR_UNIVERSE and v > 0}
                    w_tot  = sum(active.values())
                    norm_w = {k: v / w_tot for k, v in active.items()} if w_tot > 0 else {}
                    if norm_w:
                        tgt_stock = self._rebalance(date, cur_stock, norm_w)

            if regime_engine:
                tgt_regime, tgt_exp = regime_engine.update(date, self.vix)

            tgt_total, _ = _build_total(tgt_stock, tgt_exp)
            will_change  = not _eq_weights(tgt_total, cur_total)

            pend_stock  = tgt_stock
            pend_total  = tgt_total
            pend_exp    = tgt_exp
            pend_regime = tgt_regime
            pend_cost   = will_change

        return history

    def _rebalance(
        self,
        date: pd.Timestamp,
        cur_holdings: dict[str, float],
        norm_w: dict[str, float],
    ) -> dict[str, float]:
        # PIT 유니버스
        members = _reconstruct_universe(date, self.universe, self.events)
        pit_bench = self.bench[self.bench.index <= date]

        # PIT 가격 슬라이스
        sliced: dict[str, pd.Series] = {}
        for t in members:
            ps = self.price_map.get(t)
            if ps is not None:
                ps2 = ps[ps.index <= date]
                if len(ps2) >= MIN_HISTORY:
                    sliced[t] = ps2

        if not sliced:
            return cur_holdings

        # 팩터 점수 계산
        scores = self._score_all(list(sliced.keys()), sliced, pit_bench, date, norm_w)

        # 포트폴리오 선택 — QUBO 활성화 시 양자영감 최적화, 아니면 기본 방식
        qubo = getattr(self, "_qubo", None)
        if qubo is not None:
            return qubo.optimize(scores, sliced)
        return _pick(scores, set(cur_holdings.keys()), pit_bench)

    def _score_all(
        self,
        tickers: list[str],
        sliced:  dict[str, pd.Series],
        bench:   pd.Series,
        as_of:   pd.Timestamp,
        norm_w:  dict[str, float],
    ) -> dict[str, dict]:
        """
        각 팩터를 단면 백분위로 변환 → 가중 기하평균 composite score 계산
        """
        # 팩터별 원시값 수집
        raw: dict[str, dict[str, float | None]] = {f: {} for f in norm_w}
        mom63_map:  dict[str, float | None] = {}
        mom252_map: dict[str, float | None] = {}
        vol20_map:  dict[str, float | None] = {}

        for t in tickers:
            ps = sliced.get(t)
            if ps is None or len(ps) < 63:
                continue
            for fname in norm_w:
                raw[fname][t] = self._raw_signal(t, fname, ps, bench, as_of)
            mom63_map[t]  = _pret(ps, 63,  0)
            mom252_map[t] = _pret(ps, 252, 21)
            vol20_map[t]  = _vol(ps, 20)

        # 팩터별 단면 백분위
        pct: dict[str, dict[str, float]] = {}
        for fname in norm_w:
            cfg     = FACTOR_UNIVERSE[fname]
            p       = _rank_pct(raw[fname])
            if cfg["rank"] == "low_good":
                p = {t: 100.0 - v for t, v in p.items()}
            pct[fname] = p

        # 가중 기하평균
        result: dict[str, dict] = {}
        for t in tickers:
            if t not in sliced or len(sliced[t]) < 63:
                continue
            num, denom = 0.0, 0.0
            fvals: dict[str, float | None] = {}
            for fname, w in norm_w.items():
                sc = pct[fname].get(t)
                fvals[fname] = sc
                if sc is not None:
                    num   += w * np.log(max(sc, 0.5))
                    denom += w
            composite = float(np.exp(num / denom)) if denom > 0 else None
            result[t] = {
                "composite": composite,
                "factors":   fvals,
                "vol20":     vol20_map.get(t),
                "mom63":     mom63_map.get(t),
                "mom252":    mom252_map.get(t),
                "sector":    self.universe.get(t, {}).get("sector", "Unknown") or "Unknown",
            }
        return result

    def _raw_signal(
        self,
        ticker: str,
        fname:  str,
        ps:     pd.Series,
        bench:  pd.Series,
        as_of:  pd.Timestamp,
    ) -> float | None:
        cfg   = FACTOR_UNIVERSE[fname]
        ftype = cfg["type"]

        if ftype == "price":
            return _pret(ps, cfg["lookback"], cfg.get("skip", 0))

        elif ftype == "price_vs_bench":
            s = _pret(ps, cfg["lookback"], cfg.get("skip", 0))
            b = _pret(bench, cfg["lookback"], cfg.get("skip", 0))
            return (s - b) if s is not None and b is not None else None

        elif ftype == "daily":
            val = self._daily.get(ticker, cfg["key"], as_of)
            cap = cfg.get("cap")
            if val is not None and cap and (val < cap[0] or val > cap[1]):
                return None
            return val

        elif ftype == "zscore":
            return self._zscore.get(ticker, as_of)

        elif ftype == "institutional":
            return self._sf3.get_qoq(ticker, as_of, cfg.get("lookback_qtrs", 2))

        elif ftype == "inst_crowding":
            # 13F 파일링 딜레이 45일 보정: 분기 말 데이터는 45일 후 공개
            filing_lag = pd.Timedelta(days=cfg.get("filing_lag_days", 45))
            inst_val = self._sf3.get_level(ticker, as_of - filing_lag)
            if inst_val is None:
                return None
            mcap = self._daily.get(ticker, "marketcap", as_of)
            if mcap is None or mcap <= 0:
                return None
            return inst_val / (mcap * 1_000_000)  # 기관 소유 비율 (0~1+)

        elif ftype == "inst_crowding_neutral":
            # 사이즈 중립 잔차: build_inst_neutral_history.py 에서 사전 계산
            return self._inst_neutral.get(ticker, as_of)

        elif ftype == "inst_crowding_sector_neutral":
            # 섹터+사이즈 이중 중립 잔차: build_sector_neutral_history.py 에서 사전 계산
            return self._sector_neutral.get(ticker, as_of)

        elif ftype == "credit_quality":
            # S&P 방법론 기반 신용 점수 (0~100): build_credit_score_history.py 에서 사전 계산
            return self._credit.get(ticker, as_of)

        elif ftype == "inst_detail":
            # 투자자별 상세 신호: build_sf3_detail_history.py 필요
            filing_lag = pd.Timedelta(days=cfg.get("filing_lag_days", 45))
            return self._sf3_detail.get_signal(ticker, cfg["key"], as_of - filing_lag)

        elif ftype == "inst_smart_proxy":
            # 스마트머니 집중도: (-사이즈중립잔차) × HHI
            # 전체 기관 보유 비율은 낮으나(미발견), 소수 기관이 집중 보유(확신) = 스마트머니 조용한 축적
            filing_lag = pd.Timedelta(days=cfg.get("filing_lag_days", 45))
            residual = self._inst_neutral.get(ticker, as_of)          # 사이즈 통제 잔차 (음수 = 미발견)
            hhi      = self._sf3_detail.get_signal(ticker, "hhi", as_of - filing_lag)
            if residual is None or hhi is None:
                return None
            return (-residual) * hhi  # 미발견(residual↓) × 집중도(HHI↑) 클수록 높음

        elif ftype == "insider":
            return self._sf2.get_net_ratio(ticker, as_of, cfg.get("lookback_days", 365))

        elif ftype == "quantum_ml":
            vqc = getattr(self, "_vqc", None)
            if vqc is None:
                return None
            from quantum_signal import INPUT_FEATURES
            raw_factors: dict[str, float | None] = {}
            for fname in INPUT_FEATURES:
                raw_factors[fname] = self._raw_signal(ticker, fname, ps, bench, as_of)
            return vqc.predict(raw_factors)

        elif ftype == "quantum_ml_ae":
            vqc_ae = getattr(self, "_vqc_ae", None)
            if vqc_ae is None:
                return None
            from quantum_signal_ae import AE_INPUT_FEATURES
            raw_factors: dict[str, float | None] = {}
            for fname in AE_INPUT_FEATURES:
                raw_factors[fname] = self._raw_signal(ticker, fname, ps, bench, as_of)
            return vqc_ae.predict(raw_factors)

        return None

    # ──────────────────────────────────────────────────────────
    # 유니버스 / 이벤트 / 가격 로더
    # ──────────────────────────────────────────────────────────

    def _load_universe(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for path in [SP500_CURRENT_WIKI, SP400_CURRENT_WIKI, SP600_CURRENT_WIKI]:
            data = _load_json(path, {})
            for item in (data.get("items", []) or []):
                if not isinstance(item, dict):
                    continue
                t = _nt(item.get("ticker", ""))
                if t and t not in out:
                    out[t] = {
                        "ticker": t,
                        "name":   item.get("name", ""),
                        "sector": item.get("sector", "Unknown") or "Unknown",
                    }
        self._log(f"[UNIVERSE] {len(out)} tickers (current)")
        return out

    def _load_events(self) -> list[dict]:
        events: list[dict] = []
        sp500_f = SP500_SHARADAR_EVENTS if SP500_SHARADAR_EVENTS.exists() else SP500_WIKI_EVENTS
        for path in [sp500_f, SP400_EVENTS, SP600_EVENTS]:
            data = _load_json(path, {})
            for ev in data.get("events", []):
                d = str(ev.get("date", "")).strip()
                if not d:
                    continue
                events.append({
                    "date":    d,
                    "added":   [_nt(x) for x in ev.get("added",   []) if str(x).strip()],
                    "removed": [_nt(x) for x in ev.get("removed", []) if str(x).strip()],
                })
        events.sort(key=lambda x: x["date"])
        self._log(f"[EVENTS] {len(events)} membership events")
        return events

    def _load_prices(self, tickers: list[str]) -> dict[str, pd.Series]:
        all_t = sorted(set(tickers + [BENCHMARK, VIX_TICKER] + DEFENSIVE_TICKERS))
        pm    = _load_sep(all_t, IS_START)

        # 갭 보완
        if pm:
            last_date = max(s.index[-1] for s in pm.values())
            today     = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
            if last_date < today - pd.Timedelta(days=1):
                gap_start = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                self._log(f"[GAP] yfinance {gap_start} ~ today ({len(pm)} tickers)...")
                _yf_fill(pm, list(pm.keys()), gap_start)

        missing = [t for t in all_t if t not in pm]
        if missing:
            self._log(f"[yfinance] {len(missing)} tickers (SEP 외)...")
            _yf_fill(pm, missing, IS_START)

        self._log(f"[PRICES] {len(pm)} series ready")
        return pm


# ═════════════════════════════════════════════════════════════
# 레짐 엔진
# ═════════════════════════════════════════════════════════════

class _RegimeEngine:
    def __init__(self, bench: pd.Series):
        ma   = bench.rolling(REGIME_MA_WINDOW).mean()
        mom  = bench.pct_change(REGIME_MOM_WINDOW)
        self.sig = pd.DataFrame({
            "price": bench,
            "ma":    ma,
            "mom":   mom,
            "upper": ma * (1 + REGIME_BUFFER),
            "lower": ma * (1 - REGIME_BUFFER),
        })
        self._prev = "risk_on"

    def _candidate(self, date: pd.Timestamp) -> str:
        if date not in self.sig.index:
            return "risk_on"
        r = self.sig.loc[date]
        if pd.isna(r["ma"]) or pd.isna(r["mom"]):
            return "risk_on"
        if r["price"] >= r["upper"] and r["mom"] >= 0:
            return "risk_on"
        if r["price"] <= r["lower"] and r["mom"] < 0:
            return "risk_off"
        return "mid"

    def update(self, date: pd.Timestamp, vix: pd.Series | None = None) -> tuple[str, float]:
        cand = self._candidate(date)
        if cand in ("risk_on", "risk_off") and REGIME_CONFIRM_DAYS > 1:
            hist = self.sig.index[self.sig.index <= date][-REGIME_CONFIRM_DAYS:]
            if len(hist) >= REGIME_CONFIRM_DAYS and all(self._candidate(d) == cand for d in hist):
                bucket = cand
            else:
                bucket = self._prev
        elif cand in ("risk_on", "risk_off"):
            bucket = cand
        else:
            bucket = self._prev

        # VIX 크래시 오버라이드
        if vix is not None and bucket != "risk_off":
            hv = vix[vix.index <= date]
            if not hv.empty and float(hv.iloc[-1]) > VIX_CRASH_THRESHOLD:
                r = self.sig.loc[date] if date in self.sig.index else None
                if r is not None and not pd.isna(r["ma"]) and r["price"] < r["ma"]:
                    bucket = "risk_off"

        self._prev = bucket
        exp = {
            "risk_on":  RISK_ON_EXPOSURE,
            "risk_off": RISK_OFF_EXPOSURE,
            "mid":      MID_EXPOSURE,
        }.get(bucket, RISK_ON_EXPOSURE)
        return bucket, exp


# ═════════════════════════════════════════════════════════════
# 순수 함수 헬퍼
# ═════════════════════════════════════════════════════════════

def _nt(t: str) -> str:
    return str(t).strip().upper().replace(".", "-")

def _load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def _load_sep(tickers: list[str], start: str) -> dict[str, pd.Series]:
    if not SEP_PRICES_FILE.exists():
        return {}
    try:
        payload = pd.read_pickle(SEP_PRICES_FILE)
        df: pd.DataFrame = payload.get("prices") if isinstance(payload, dict) else payload
        if df is None or df.empty:
            return {}
        df.index = pd.to_datetime(df.index)
        df = df[df.index >= pd.Timestamp(start)]
        return {t: df[t].dropna() for t in tickers if t in df.columns and len(df[t].dropna()) >= 60}
    except Exception as e:
        print(f"[WARN] SEP 로드 오류: {e}")
        return {}

def _yf_fill(pm: dict[str, pd.Series], tickers: list[str], start: str) -> None:
    for i in range(0, len(tickers), 200):
        batch = tickers[i : i + 200]
        try:
            df = yf.download(batch, start=start, auto_adjust=True,
                             progress=False, group_by="ticker", threads=True)
            if df is None or df.empty:
                continue
            if isinstance(df.columns, pd.MultiIndex):
                for t in batch:
                    try:
                        if t not in df.columns.get_level_values(0):
                            continue
                        s = df[t]["Close"].dropna()
                        s.index = pd.to_datetime(s.index)
                        if t in pm:
                            c = pd.concat([pm[t], s])
                            pm[t] = c[~c.index.duplicated(keep="last")].sort_index()
                        else:
                            pm[t] = s
                    except Exception:
                        pass
            elif "Close" in df.columns and len(batch) == 1:
                t = batch[0]
                s = df["Close"].dropna()
                s.index = pd.to_datetime(s.index)
                pm[t] = pd.concat([pm[t], s]) if t in pm else s
        except Exception as e:
            print(f"[WARN] yfinance 오류: {e}")

def _reconstruct_universe(
    as_of: pd.Timestamp, current: dict[str, dict], events: list[dict]
) -> set[str]:
    members = set(current.keys())
    for ev in sorted(events, key=lambda x: x["date"], reverse=True):
        if pd.to_datetime(ev["date"]) > as_of:
            for t in ev.get("added", []):
                members.discard(t)
            for t in ev.get("removed", []):
                members.add(t)
    return members

def _pret(s: pd.Series, lb: int, skip: int = 0) -> float | None:
    n = len(s)
    if n < lb + 1:
        return None
    end = n - 1 - skip
    start = end - lb
    if start < 0:
        return None
    p0, p1 = float(s.iloc[start]), float(s.iloc[end])
    return (p1 / p0 - 1.0) if p0 > 0 else None

def _vol(s: pd.Series, w: int = 20) -> float | None:
    if len(s) < w + 1:
        return None
    r = s.pct_change().dropna().tail(w)
    v = float(r.std()) * 252 ** 0.5
    return v if v > 0 else None

def _rank_pct(vals: dict[str, float | None]) -> dict[str, float]:
    valid = {t: v for t, v in vals.items() if v is not None}
    if not valid:
        return {}
    tks = list(valid.keys())
    arr = np.array([valid[t] for t in tks], dtype=float)
    pct = pd.Series(arr).rank(pct=True, method="average") * 100
    return {t: float(pct.iloc[i]) for i, t in enumerate(tks)}

def _pick(scores: dict[str, dict], held: set[str], bench: pd.Series) -> dict[str, float]:
    def absmom_ok(t: str) -> bool:
        s = scores.get(t, {})
        m63, m252 = s.get("mom63"), s.get("mom252")
        if m63 is None or m252 is None:
            return False
        return m63 > ABS_MOM_63D_MIN and m252 > ABS_MOM_252D_MIN

    ranked = sorted(
        [t for t in scores if scores[t].get("composite") is not None],
        key=lambda t: scores[t]["composite"], reverse=True,
    )
    filtered = [t for t in ranked if absmom_ok(t)] or ranked
    spy_v    = _vol(bench, 20)
    dyn_floor = max(VOL_FLOOR, spy_v) if spy_v else VOL_FLOOR

    selected: list[str] = []
    sec_cnt:  dict[str, int] = {}

    def try_add(t: str) -> bool:
        sec = scores[t].get("sector", "Unknown") or "Unknown"
        if SECTOR_MAX_NAMES > 0 and sec_cnt.get(sec, 0) >= SECTOR_MAX_NAMES:
            return False
        selected.append(t)
        sec_cnt[sec] = sec_cnt.get(sec, 0) + 1
        return True

    for i, t in enumerate(filtered):
        if t in held and i < HOLD_BUFFER_N and len(selected) < TOP_N:
            try_add(t)
    for t in filtered:
        if len(selected) >= TOP_N:
            break
        if t not in selected:
            try_add(t)

    if not selected:
        return {}

    # score^alpha / vol 가중치
    raw_w: dict[str, float] = {}
    for t in selected:
        sc = max(scores[t].get("composite") or 1.0, 0.5)
        vl = max(scores[t].get("vol20") or VOL_FALLBACK, dyn_floor)
        raw_w[t] = (sc ** SCORE_ALPHA) / vl

    total = sum(raw_w.values())
    if total <= 0:
        n = len(selected)
        return {t: 1.0 / n for t in selected}
    w = {t: v / total for t, v in raw_w.items()}
    for _ in range(2):
        w = {t: max(MIN_WEIGHT, min(MAX_WEIGHT, v)) for t, v in w.items()}
        s = sum(w.values())
        w = {t: v / s for t, v in w.items()}
    return w

def _build_total(stock_w: dict[str, float], exp: float) -> tuple[dict[str, float], dict[str, float]]:
    exp = max(0.0, min(1.0, exp))
    def_w = 1.0 - exp
    total: dict[str, float] = {}
    def _norm(d: dict[str, float]) -> dict[str, float]:
        s = sum(v for v in d.values() if v > 0)
        return {k: v / s for k, v in d.items() if v > 0} if s > 0 else {}
    for t, w in _norm(stock_w).items():
        total[t] = total.get(t, 0.0) + w * exp
    defensive: dict[str, float] = {}
    for t, w in _norm(DEFENSIVE_WEIGHTS).items():
        dw = w * def_w
        total[t] = total.get(t, 0.0) + dw
        defensive[t] = dw
    return _norm(total), defensive

def _eq_weights(a: dict[str, float], b: dict[str, float], tol: float = 1e-10) -> bool:
    for k in set(a) | set(b):
        if abs(float(a.get(k, 0)) - float(b.get(k, 0))) > tol:
            return False
    return True

def _monthly_dates(idx: pd.Index) -> list[pd.Timestamp]:
    out, seen = [], set()
    for d in idx:
        k = d.strftime("%Y-%m")
        if k not in seen:
            out.append(d)
            seen.add(k)
    return out

def _daily_return(pm: dict[str, pd.Series], holdings: dict[str, float], date: pd.Timestamp) -> float:
    wr, uw = 0.0, 0.0
    for t, w in holdings.items():
        s = pm.get(t)
        if s is None or date not in s.index:
            continue
        idx = s.index.get_loc(date)
        if idx == 0:
            continue
        r = float(s.iloc[idx] / s.iloc[idx - 1] - 1.0)
        wr += w * r
        uw += w
    return float(wr / uw) if uw > 0 else 0.0

def _calc_metrics(df: pd.DataFrame) -> dict[str, float]:
    if df.empty:
        return {}
    eq    = df["equity"].copy()
    eq    = eq / eq.iloc[0]
    dr    = df["daily_return"].values
    years = len(df) / 252
    total = float(eq.iloc[-1] - 1.0)
    cagr  = float(eq.iloc[-1] ** (1 / years) - 1) if years > 0 else 0.0
    vol   = float(np.std(dr) * 252 ** 0.5)
    sharpe = cagr / vol if vol > 0 else 0.0
    mdd   = float((eq / eq.cummax() - 1).min())
    return {
        "total_return": round(total,  6),
        "cagr":         round(cagr,   6),
        "volatility":   round(vol,    6),
        "sharpe":       round(sharpe, 6),
        "max_drawdown": round(mdd,    6),
    }


# ═════════════════════════════════════════════════════════════
# 편의 함수 — 스크립트로 직접 실행할 때 샘플 모델로 동작
# ═════════════════════════════════════════════════════════════

def run_backtest(weights: dict[str, float], engine: BacktestEngine | None = None) -> BacktestResult:
    """
    편의 래퍼. 엔진 인스턴스가 없으면 새로 만들어 실행.
    반복 실험 시에는 engine을 외부에서 만들어 재사용할 것.
    """
    e = engine or BacktestEngine()
    return e.run(weights)


if __name__ == "__main__":
    # 직접 실행 시 — 예시 모델로 IS/OOS 검증
    # 실제 모델 개발 시 이 부분을 수정하거나 별도 스크립트에서 import해 사용
    sample_weights = {
        "mom12_1": 0.35,
        "mom6_1":  0.15,
        "mom3_1":  0.10,
        "rs_spy_6m": 0.10,
        "evebit":  0.12,
        "pb":      0.08,
        "zscore":  0.10,
    }
    engine = BacktestEngine()
    result = engine.run(sample_weights)
    print(result.summary())
