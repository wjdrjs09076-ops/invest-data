from __future__ import annotations

import json
import math
import shutil
import re # [NEW] 정규식 사용
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"

UNIVERSE_FILE = DATA_DIR / "universe.json"
SP500_FILE = DATA_DIR / "sp500_tickers.json"
SP400_FILE = DATA_DIR / "sp400_current_wiki.json"
SP600_FILE = DATA_DIR / "sp600_current_wiki.json"
QUALITY_FILE        = DATA_DIR / "quality_snapshot.json"
INSTITUTIONAL_FILE  = DATA_DIR / "institutional_signal.json"
INSIDER_FILE        = DATA_DIR / "insider_signal.json"
VALUATION_FILE      = DATA_DIR / "daily_valuation.json"
OUT_FILE            = DATA_DIR / "score_snapshot.json"

# [NEW] 뉴스 데이터 경로 설정
NEWS_DIR = DATA_DIR / "news"

# -----------------------------
# Config
# -----------------------------
BENCHMARK_TICKER = "SPY" # [업그레이드] 벤치마크 티커 추가

GROUP_LABELS = {
    "sp500": "S&P 500",
    "sp400": "S&P 400 MidCap",
    "sp600": "S&P 600 SmallCap",
    "nasdaq100": "NASDAQ-100",
    "dow30": "Dow 30",
}

GROUP_DESCRIPTIONS = {
    "sp500": (
        "Ranked by geometric-based composite score "
        "(Momentum + RS + Quality + Sector strength + Risk), "
        "then filtered by relaxed absolute momentum and sector cap."
    ),
    "sp400": (
        "Ranked by geometric-based composite score "
        "(Momentum + RS + Quality + Sector strength + Risk), "
        "then filtered by relaxed absolute momentum and sector cap."
    ),
    "sp600": (
        "Ranked by geometric-based composite score "
        "(Momentum + RS + Quality + Sector strength + Risk), "
        "then filtered by relaxed absolute momentum and sector cap."
    ),
    "nasdaq100": (
        "Ranked by geometric-based composite score "
        "(Momentum + RS + Quality + Sector strength + Risk), "
        "then filtered by relaxed absolute momentum and sector cap."
    ),
    "dow30": (
        "Ranked by geometric-based composite score "
        "(Momentum + RS + Quality + Sector strength + Risk), "
        "then filtered by relaxed absolute momentum and sector cap."
    ),
}

BATCH_SIZE = 50
LOOKBACK_PERIOD = "18mo"
TOP_N = 3

# Momentum weights (sum = 1.00 inside stock score)
WEIGHT_MOM_21D = 0.05
WEIGHT_MOM_63D = 0.20   # [업그레이드] RS 지표에 비중 양보
WEIGHT_RS_63D = 0.25    # [업그레이드] 상대적 강도(Relative Strength) 신규 편입
WEIGHT_MOM_126D = 0.30
WEIGHT_MOM_252D = 0.00
WEIGHT_MOM_12_1 = 0.20

# Final score block weights
WEIGHT_QUALITY        = 0.15  # Sharadar SF1 퀄리티 (build_quality_snapshot)
WEIGHT_SECTOR         = 0.05
WEIGHT_RISK           = 0.05
WEIGHT_INSTITUTIONAL  = 0.08  # Sharadar SF3 기관 순매수 (build_institutional_signal)
WEIGHT_INSIDER        = 0.04  # Sharadar SF2 내부자 순매수 (build_insider_signal)
WEIGHT_VALUATION      = 0.08  # Sharadar DAILY EV/EBIT + P/B (build_daily_valuation)

# Risk internal weights (sum = 1.00 inside risk score)
WEIGHT_VOL = 0.40
WEIGHT_DOWNSIDE = 0.30
WEIGHT_DD = 0.30

# -----------------------------
# Portfolio construction
# -----------------------------
PORTFOLIO_WEIGHT_METHOD = "score_x_inverse_vol"
WEIGHT_ALPHA_SCORE = 2.5
MIN_WEIGHT = 0.05
MAX_WEIGHT = 0.20
VOL_FALLBACK = 0.35
VOL_WEIGHT_FLOOR = 0.18

# -----------------------------
# Absolute momentum / diversification
# -----------------------------
ABS_MOM_63D_MIN = 0
ABS_MOM_252D_MIN = 0
SECTOR_MAX_NAMES = 3

# -----------------------------
# Sector robustness / smoothing
# -----------------------------
SECTOR_MIN_COUNT_FULL = 3
SECTOR_SHRINK_MAP = {
    1: 0.30,
    2: 0.60,
}
SECTOR_DEFAULT_SHRINK = 1.00

# -----------------------------
# [최종 고도화] 기관급 킬 스위치(Kill-Switch) 뉴스 필터
# -----------------------------
import re
from collections import Counter

NEWS_SCAN_COUNT = 50
NEWS_LOOKBACK_ITEMS = 10

# -----------------------------
# Tier 1: 즉시 퇴출 (정말 치명적인 것만)
# -----------------------------
HARD_KILL_KEYWORDS = [
    "BANKRUPTCY", "CHAPTER 11", "DEFAULT", "DELISTING",
    "GOING CONCERN", "AUDITOR RESIGNATION", "TECHNICAL DEFAULT"
]

# -----------------------------
# Tier 2: 강한 패널티 (즉시 퇴출까지는 아님)
# -----------------------------
SEVERE_NEG_KEYWORDS = [
    "FRAUD", "GUILTY", "RESTATEMENT", "LIQUIDITY CRUNCH",
    "MATERIAL WEAKNESS", "DELAYED FILING", "ABRUPT RESIGNATION",
    "INDICTMENT", "SUBPOENA", "CLAWBACK"
]

# -----------------------------
# Tier 3: 일반 악재 (빈도 기반 감점)
# -----------------------------
MILD_NEG_KEYWORDS = [
    "LAWSUIT", "INVESTIGATION", "SEC", "PROBE", "CYBERATTACK",
    "RECALL", "MISCONDUCT", "SHORT SELLER", "DOWNGRADE",
    "WHISTLEBLOWER", "CLASS ACTION", "HOSTILE TAKEOVER"
]

# -----------------------------
# Tier 4: 호재
# -----------------------------
POS_KEYWORDS = {
    "UPGRADE": 2,
    "RAISED TARGET": 2,
    "BEAT": 2,
    "APPROVAL": 3,
    "PARTNERSHIP": 3,
    "BREAKTHROUGH": 3,
    "ACQUISITION": 2,
    "BUY RATING": 2,
}

HARD_KILL_REGEX = re.compile(r'\b(?:' + '|'.join(map(re.escape, HARD_KILL_KEYWORDS)) + r')\b', re.IGNORECASE)
SEVERE_NEG_REGEX = re.compile(r'\b(?:' + '|'.join(map(re.escape, SEVERE_NEG_KEYWORDS)) + r')\b', re.IGNORECASE)
MILD_NEG_REGEX = re.compile(r'\b(?:' + '|'.join(map(re.escape, MILD_NEG_KEYWORDS)) + r')\b', re.IGNORECASE)
POS_REGEX = re.compile(r'\b(?:' + '|'.join(map(re.escape, POS_KEYWORDS.keys())) + r')\b', re.IGNORECASE)


def detect_keyword_counts(news_items: list[str]) -> dict:
    """
    news_items: 최근 뉴스 제목/요약 문자열 리스트
    """
    counts = Counter()
    total_items = len(news_items)

    for text in news_items:
        text_upper = text.upper()

        if HARD_KILL_REGEX.search(text_upper):
            counts["hard_kill"] += 1

        severe_matches = SEVERE_NEG_REGEX.findall(text_upper)
        mild_matches = MILD_NEG_REGEX.findall(text_upper)
        pos_matches = POS_REGEX.findall(text_upper)

        counts["severe_neg"] += len(severe_matches)
        counts["mild_neg"] += len(mild_matches)

        for m in pos_matches:
            counts[f"pos::{m.upper()}"] += 1

    counts["total_items"] = total_items
    return counts


def trend_relief_factor(ret_20d: float, ret_63d: float, above_ma50: bool) -> float:
    """
    가격 추세가 강할수록 일반 악재 패널티를 줄인다.
    0.5 = 패널티 50%만 반영
    1.0 = 패널티 100% 반영
    """
    if ret_20d > 0.05 and ret_63d > 0.10 and above_ma50:
        return 0.5
    if ret_20d > 0.02 and ret_63d > 0.05:
        return 0.75
    return 1.0


def compute_news_score(
    news_items: list[str],
    ret_20d: float,
    ret_63d: float,
    above_ma50: bool,
) -> tuple[float, bool, dict]:
    """
    Returns:
      news_score_adjustment: 기존 종합 점수에 더할 값
      hard_kill: 즉시 퇴출 여부
      debug_info: 디버깅용 상세 정보
    """
    counts = detect_keyword_counts(news_items)

    # 1) 진짜 치명적 악재는 즉시 퇴출
    if counts["hard_kill"] > 0:
        return -999.0, True, {
            "reason": "hard_kill",
            "counts": dict(counts),
        }

    score = 0.0

    # 2) 중대 악재: 누적 강한 감점
    severe_count = counts["severe_neg"]
    if severe_count == 1:
        score -= 8
    elif severe_count == 2:
        score -= 14
    elif severe_count >= 3:
        score -= 20

    # 3) 일반 악재: 빈도 기반 감점 + 추세 강하면 패널티 완화
    mild_count = counts["mild_neg"]
    mild_penalty = 0.0
    if mild_count == 1:
        mild_penalty = -3
    elif mild_count == 2:
        mild_penalty = -6
    elif mild_count >= 3:
        mild_penalty = -10

    relief = trend_relief_factor(ret_20d, ret_63d, above_ma50)
    score += mild_penalty * relief

    # 4) 뉴스량 편향 보정: 기사 수가 많으면 부정 신호 비율도 함께 고려
    total_items = max(counts["total_items"], 1)
    neg_density = (severe_count + mild_count) / total_items

    if neg_density >= 0.5:
        score -= 4
    elif neg_density <= 0.1 and mild_count > 0:
        score += 1  # 기사 많은 종목에 대한 과도한 감점 완화

    # 5) 긍정 뉴스 가점
    pos_score = 0
    for key, weight in POS_KEYWORDS.items():
        pos_score += counts.get(f"pos::{key.upper()}", 0) * weight

    # 호재는 과도하게 커지지 않도록 상한
    pos_score = min(pos_score, 6)
    score += pos_score

    return score, False, {
        "reason": "scored",
        "counts": dict(counts),
        "mild_penalty_before_relief": mild_penalty,
        "trend_relief_factor": relief,
        "neg_density": neg_density,
        "pos_score_capped": pos_score,
    }


@dataclass
class StockMetric:
    ticker: str
    sector: str
    close: float | None

    ret5d: float | None
    ret20d: float | None
    ret21d: float | None
    ret63d: float | None
    rs_63d: float | None # [업그레이드] 상대적 강도 변수 추가
    ret126d: float | None
    ret252d: float | None
    mom12_1: float | None

    rsi14: float | None

    vol20: float | None
    downside20: float | None
    dd60: float | None

    stock_score_raw: float | None = None
    quality_score: float | None = None
    sector_strength_63d: float | None = None
    sector_score: float | None = None
    risk_score: float | None = None
    final_score_raw: float | None = None
    final_score_100: int | None = None
    signal: str | None = None
    portfolio_weight: float | None = None
    
    # [NEW] 뉴스 필터 사유 저장
    news_reason: str | None = None 


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def copy_to_public(src: Path) -> None:
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst = PUBLIC_DATA_DIR / src.name
    shutil.copy2(src, dst)
    print(f"[OK] Copied -> {dst}")


def clip(x: float, low: float, high: float) -> float:
    return max(low, min(high, x))


def safe_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    except Exception:
        return None


def normalize_ticker(t: str) -> str:
    return str(t).strip().upper().replace(".", "-")


def chunked(items: list[str], n: int) -> list[list[str]]:
    return [items[i:i + n] for i in range(0, len(items), n)]


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    if series is None or len(series) < period + 1:
        return pd.Series(index=series.index if series is not None else [])
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    roll_up = up.ewm(alpha=1 / period, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / period, adjust=False).mean()

    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def momentum_12_1(series: pd.Series) -> float | None:
    if series is None or len(series) < 260:
        return None

    p12 = safe_float(series.iloc[-252])
    p1 = safe_float(series.iloc[-21])

    if p12 is None or p1 is None or p12 == 0:
        return None

    return (p1 / p12 - 1.0) * 100.0


def downside_volatility(series: pd.Series, lookback: int = 20) -> float | None:
    if series is None or len(series) <= lookback:
        return None

    rets = series.pct_change().dropna()
    if len(rets) < lookback:
        return None

    window = rets.iloc[-lookback:]
    downside = window[window < 0]

    if downside.empty:
        return 0.0

    vol = float(downside.std() * np.sqrt(252))
    return vol


def pct_return(series: pd.Series, lookback: int) -> float | None:
    if series is None or len(series) <= lookback:
        return None

    last_val = safe_float(series.iloc[-1])
    prev_val = safe_float(series.iloc[-(lookback + 1)])

    if last_val is None or prev_val is None or prev_val == 0:
        return None

    return (last_val / prev_val - 1.0) * 100.0


def annualized_volatility(series: pd.Series, lookback: int = 20) -> float | None:
    if series is None or len(series) <= lookback:
        return None

    rets = series.pct_change().dropna()
    if len(rets) < lookback:
        return None

    window = rets.iloc[-lookback:]
    vol = float(window.std() * np.sqrt(252))
    return vol


def max_drawdown(series: pd.Series, lookback: int = 60) -> float | None:
    if series is None or len(series) < 10:
        return None

    window = series.iloc[-lookback:] if len(series) > lookback else series
    if window.empty:
        return None

    roll_max = window.cummax()
    dd = (window / roll_max) - 1.0
    return float(dd.min())


def raw_score_to_100(raw: float) -> int:
    score = round(clip(raw, 0.0, 1.0) * 100.0)
    return int(score)


def signal_from_score(score100: int) -> str:
    if score100 >= 70:
        return "BUY"
    if score100 >= 55:
        return "WATCH"
    if score100 >= 40:
        return "HOLD"
    return "AVOID"


def _add_group_flag(meta: dict[str, Any], group: str) -> None:
    flags = meta.setdefault("index_flags", [])
    if group not in flags:
        flags.append(group)


def parse_universe() -> tuple[dict[str, dict[str, Any]], dict[str, list[str]]]:
    universe = load_json(UNIVERSE_FILE, default=None)

    by_ticker: dict[str, dict[str, Any]] = {}
    groups: dict[str, list[str]] = {
        "sp500": [],
        "sp400": [],
        "sp600": [],
        "nasdaq100": [],
        "dow30": [],
    }

    def ensure_meta(ticker: str) -> dict[str, Any]:
        t = normalize_ticker(ticker)
        meta = by_ticker.setdefault(
            t,
            {
                "ticker": t,
                "sector": "Unknown",
                "index_flags": [],
            },
        )
        return meta

    if universe is None:
        tickers = load_json(SP500_FILE, default=[])
        for t in tickers:
            t2 = normalize_ticker(t)
            ensure_meta(t2)
            _add_group_flag(by_ticker[t2], "sp500")
            groups["sp500"].append(t2)
        groups["sp500"] = sorted(set(groups["sp500"]))
        return by_ticker, groups

    if isinstance(universe, list):
        for item in universe:
            if not isinstance(item, dict):
                continue

            ticker = normalize_ticker(item.get("ticker") or item.get("symbol") or "")
            if not ticker:
                continue

            meta = ensure_meta(ticker)
            sector = item.get("sector") or item.get("gics_sector") or item.get("gicsSector")
            if sector:
                meta["sector"] = str(sector).strip()

            flags = item.get("indexFlags") or item.get("index_flags") or item.get("indices") or item.get("indexes") or []
            if isinstance(flags, str):
                flags = [flags]
            if isinstance(flags, list):
                for g in flags:
                    g_norm = str(g).strip().lower()
                    if g_norm in groups:
                        _add_group_flag(meta, g_norm)
                        groups[g_norm].append(ticker)

            if item.get("sp500") is True:
                _add_group_flag(meta, "sp500")
                groups["sp500"].append(ticker)
            if item.get("sp400") is True:
                _add_group_flag(meta, "sp400")
                groups["sp400"].append(ticker)
            if item.get("sp600") is True:
                _add_group_flag(meta, "sp600")
                groups["sp600"].append(ticker)
            if item.get("nasdaq100") is True:
                _add_group_flag(meta, "nasdaq100")
                groups["nasdaq100"].append(ticker)
            if item.get("dow30") is True:
                _add_group_flag(meta, "dow30")
                groups["dow30"].append(ticker)

    elif isinstance(universe, dict):
        if isinstance(universe.get("items"), list):
            for item in universe["items"]:
                if not isinstance(item, dict):
                    continue

                ticker = normalize_ticker(item.get("ticker") or item.get("symbol") or "")
                if not ticker:
                    continue

                meta = ensure_meta(ticker)
                sector = item.get("sector") or item.get("gics_sector") or item.get("gicsSector")
                if sector:
                    meta["sector"] = str(sector).strip()

                flags = item.get("indexFlags") or item.get("index_flags") or item.get("indices") or []
                if isinstance(flags, str):
                    flags = [flags]
                if isinstance(flags, list):
                    for g in flags:
                        g_norm = str(g).strip().lower()
                        if g_norm in groups:
                            _add_group_flag(meta, g_norm)
                            groups[g_norm].append(ticker)

                if item.get("sp500") is True:
                    _add_group_flag(meta, "sp500")
                    groups["sp500"].append(ticker)
                if item.get("sp400") is True:
                    _add_group_flag(meta, "sp400")
                    groups["sp400"].append(ticker)
                if item.get("sp600") is True:
                    _add_group_flag(meta, "sp600")
                    groups["sp600"].append(ticker)
                if item.get("nasdaq100") is True:
                    _add_group_flag(meta, "nasdaq100")
                    groups["nasdaq100"].append(ticker)
                if item.get("dow30") is True:
                    _add_group_flag(meta, "dow30")
                    groups["dow30"].append(ticker)

        for group_key in ["sp500", "sp400", "sp600", "nasdaq100", "dow30"]:
            arr = universe.get(group_key)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, str):
                        ticker = normalize_ticker(item)
                        if not ticker:
                            continue
                        meta = ensure_meta(ticker)
                        _add_group_flag(meta, group_key)
                        groups[group_key].append(ticker)
                    elif isinstance(item, dict):
                        ticker = normalize_ticker(item.get("ticker") or item.get("symbol") or "")
                        if not ticker:
                            continue
                        meta = ensure_meta(ticker)
                        sector = item.get("sector") or item.get("gics_sector") or item.get("gicsSector")
                        if sector:
                            meta["sector"] = str(sector).strip()
                        _add_group_flag(meta, group_key)
                        groups[group_key].append(ticker)

        for k, v in universe.items():
            if k in {"items", "sp500", "sp400", "sp600", "nasdaq100", "dow30"}:
                continue
            if not isinstance(v, dict):
                continue

            maybe_ticker = normalize_ticker(v.get("ticker") or v.get("symbol") or k)
            if not maybe_ticker:
                continue

            meta = ensure_meta(maybe_ticker)
            sector = v.get("sector") or v.get("gics_sector") or v.get("gicsSector")
            if sector:
                meta["sector"] = str(sector).strip()

            flags = v.get("indexFlags") or v.get("index_flags") or v.get("indices") or []
            if isinstance(flags, str):
                flags = [flags]
            if isinstance(flags, list):
                for g in flags:
                    g_norm = str(g).strip().lower()
                    if g_norm in groups:
                        _add_group_flag(meta, g_norm)
                        groups[g_norm].append(maybe_ticker)

            if v.get("sp500") is True:
                _add_group_flag(meta, "sp500")
                groups["sp500"].append(maybe_ticker)
            if v.get("sp400") is True:
                _add_group_flag(meta, "sp400")
                groups["sp400"].append(maybe_ticker)
            if v.get("sp600") is True:
                _add_group_flag(meta, "sp600")
                groups["sp600"].append(maybe_ticker)
            if v.get("nasdaq100") is True:
                _add_group_flag(meta, "nasdaq100")
                groups["nasdaq100"].append(maybe_ticker)
            if v.get("dow30") is True:
                _add_group_flag(meta, "dow30")
                groups["dow30"].append(maybe_ticker)

    sp500_fallback = load_json(SP500_FILE, default=[])
    for t in sp500_fallback:
        t2 = normalize_ticker(t)
        if not t2:
            continue
        ensure_meta(t2)
        _add_group_flag(by_ticker[t2], "sp500")
        groups["sp500"].append(t2)

    for grp_key, wiki_file in [("sp400", SP400_FILE), ("sp600", SP600_FILE)]:
        wiki_data = load_json(wiki_file, default=[])
        if isinstance(wiki_data, list):
            tickers_from_wiki = wiki_data
        elif isinstance(wiki_data, dict):
            tickers_from_wiki = wiki_data.get("tickers", wiki_data.get("items", []))
        else:
            tickers_from_wiki = []
        for t in tickers_from_wiki:
            t2 = normalize_ticker(t if isinstance(t, str) else t.get("ticker", ""))
            if not t2:
                continue
            ensure_meta(t2)
            _add_group_flag(by_ticker[t2], grp_key)
            groups[grp_key].append(t2)

    for g in groups:
        groups[g] = sorted(set(groups[g]))

    return by_ticker, groups


def load_quality_score_map() -> dict[str, float]:
    payload = load_json(QUALITY_FILE, default={})
    items = payload.get("items", [])

    out: dict[str, float] = {}
    if not isinstance(items, list):
        return out

    for item in items:
        if not isinstance(item, dict):
            continue

        ticker = normalize_ticker(item.get("ticker") or "")
        if not ticker:
            continue

        raw = safe_float(item.get("quality_score_raw"))
        if raw is None:
            raw100 = safe_float(item.get("quality_score_100"))
            if raw100 is not None:
                raw = raw100 / 100.0

        if raw is None:
            continue

        out[ticker] = clip(raw, 0.0, 1.0)

    return out

def load_signal_map(path: Path, key: str = "signals") -> dict[str, float]:
    """Sharadar 신호 파일에서 {ticker: score(0-100)} 로드"""
    payload = load_json(path, default={})
    raw = payload.get(key, {})
    return {normalize_ticker(t): float(v) for t, v in raw.items() if v is not None}


def load_institutional_score_map() -> dict[str, float]:
    return load_signal_map(INSTITUTIONAL_FILE, key="signals")


def load_insider_score_map() -> dict[str, float]:
    return load_signal_map(INSIDER_FILE, key="signals")


def load_valuation_score_map() -> dict[str, float]:
    return load_signal_map(VALUATION_FILE, key="valuation_score")


# [업그레이드] 다운로드 에러(NoneType) 완벽 방어 로직 적용
def download_close_map(tickers: list[str]) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}
    all_tickers = sorted(set([normalize_ticker(t) for t in tickers if t] + [BENCHMARK_TICKER]))
    
    if not all_tickers:
        return out

    for batch in chunked(all_tickers, BATCH_SIZE):
        try:
            df = yf.download(
                tickers=batch,
                period=LOOKBACK_PERIOD,
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
            
            if df is None or df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                level0 = set(df.columns.get_level_values(0))
                for ticker in batch:
                    if ticker not in level0:
                        continue
                    try:
                        sub_df = df[ticker]
                        # 데이터 존재 여부 강력 확인
                        if sub_df is not None and not sub_df.empty and "Close" in sub_df.columns:
                            close = sub_df["Close"].dropna()
                            if not close.empty:
                                out[ticker] = close
                    except Exception:
                        continue
            else:
                if len(batch) == 1 and "Close" in df.columns:
                    close = df["Close"].dropna()
                    if not close.empty:
                        out[batch[0]] = close
        except Exception as e:
            print(f"[WARN] Batch download failed for {batch[:3]}... : {e}")
            continue

    return out


def build_metrics_for_group(
    group_tickers: list[str],
    by_ticker: dict[str, dict[str, Any]],
    close_map: dict[str, pd.Series],
) -> list[StockMetric]:
    rows: list[StockMetric] = []
    
    spy_close = close_map.get(BENCHMARK_TICKER)
    spy_ret63d = pct_return(spy_close, 63) if spy_close is not None else 0.0

    for ticker in group_tickers:
        close = close_map.get(ticker)
        sector = by_ticker.get(ticker, {}).get("sector", "Unknown") or "Unknown"

        if close is None or close.empty:
            rows.append(
                StockMetric(
                    ticker=ticker,
                    sector=sector,
                    close=None,
                    ret5d=None,
                    ret20d=None,
                    ret21d=None,
                    ret63d=None,
                    rs_63d=None,
                    ret126d=None,
                    ret252d=None,
                    mom12_1=None,
                    rsi14=None,
                    vol20=None,
                    downside20=None,
                    dd60=None,
                )
            )
            continue

        rsi_series = compute_rsi(close, 14)
        stock_ret63d = pct_return(close, 63)
        rs_63d = (stock_ret63d - spy_ret63d) if stock_ret63d is not None and spy_ret63d is not None else None

        rows.append(
            StockMetric(
                ticker=ticker,
                sector=sector,
                close=safe_float(close.iloc[-1]),
                ret5d=pct_return(close, 5),
                ret20d=pct_return(close, 20),
                ret21d=pct_return(close, 21),
                ret63d=stock_ret63d,
                rs_63d=rs_63d,
                ret126d=pct_return(close, 126),
                ret252d=pct_return(close, 252),
                mom12_1=momentum_12_1(close),
                rsi14=safe_float(rsi_series.iloc[-1]) if not rsi_series.empty else None,
                vol20=annualized_volatility(close, 20),
                downside20=downside_volatility(close, 20),
                dd60=max_drawdown(close, 60),
            )
        )

    return rows


def compute_sector_strength(rows: list[StockMetric]) -> tuple[dict[str, float], float | None]:
    valid = [r for r in rows if r.ret63d is not None]
    if not valid:
        return {}, None

    market_avg = float(np.mean([r.ret63d for r in valid]))

    bucket: dict[str, list[float]] = {}
    for r in valid:
        sector = r.sector or "Unknown"
        bucket.setdefault(sector, []).append(float(r.ret63d))

    sector_strength_map: dict[str, float] = {}

    for sector, vals in bucket.items():
        sector_avg = float(np.mean(vals))
        raw_strength = sector_avg - market_avg

        count = len(vals)
        if count >= SECTOR_MIN_COUNT_FULL:
            shrink = SECTOR_DEFAULT_SHRINK
        else:
            shrink = SECTOR_SHRINK_MAP.get(count, SECTOR_DEFAULT_SHRINK)

        adjusted_strength = raw_strength * shrink
        sector_strength_map[sector] = adjusted_strength

    return sector_strength_map, market_avg


def percentile_score_map(
    rows: list[StockMetric],
    value_getter,
    *,
    higher_is_better: bool = True,
) -> dict[str, float]:
    pairs: list[tuple[str, float]] = []
    for r in rows:
        v = value_getter(r)
        if v is None:
            continue
        pairs.append((r.ticker, float(v)))

    if not pairs:
        return {}

    df = pd.DataFrame(pairs, columns=["ticker", "value"])
    df["rank"] = df["value"].rank(method="average", ascending=True)

    n = len(df)
    if n == 1:
        df["score01"] = 0.5
    else:
        df["score01"] = (df["rank"] - 1.0) / (n - 1.0)

    if not higher_is_better:
        df["score01"] = 1.0 - df["score01"]

    return dict(zip(df["ticker"], df["score01"]))


def passes_absolute_momentum(row: StockMetric) -> bool:
    if row.ret63d is None or row.ret252d is None:
        return False
    return row.ret63d > ABS_MOM_63D_MIN and row.ret252d > ABS_MOM_252D_MIN


def apply_sector_name_cap(rows: list[StockMetric], max_names: int) -> list[StockMetric]:
    if max_names <= 0:
        return rows

    out: list[StockMetric] = []
    sector_counts: dict[str, int] = {}

    for r in rows:
        sector = r.sector or "Unknown"
        current = sector_counts.get(sector, 0)
        if current >= max_names:
            continue
        out.append(r)
        sector_counts[sector] = current + 1

    return out


def select_snapshot_rows(
    scored_rows: list[StockMetric],
    *,
    top_n: int = TOP_N,
    sector_max_names: int = SECTOR_MAX_NAMES,
) -> list[StockMetric]:
    ranked = [r for r in scored_rows if r.final_score_100 is not None]
    ranked.sort(key=lambda x: (x.final_score_100 or -999), reverse=True)

    filtered = [r for r in ranked if passes_absolute_momentum(r)]
    if not filtered:
        filtered = ranked

    capped = apply_sector_name_cap(filtered, sector_max_names)
    return capped[:top_n]


def safe_log_score(val: float | None) -> float:
    if val is None or val <= 0:
        return math.log(0.05) 
    return math.log(clip(val, 0.05, 1.0))


def score_group(
    rows: list[StockMetric],
    quality_score_map: dict[str, float] | None = None,
    institutional_score_map: dict[str, float] | None = None,
    insider_score_map: dict[str, float] | None = None,
    valuation_score_map: dict[str, float] | None = None,
) -> list[StockMetric]:
    sector_strength_map, _market_avg = compute_sector_strength(rows)

    for r in rows:
        r.sector_strength_63d = sector_strength_map.get(r.sector, None)

    mom21_map = percentile_score_map(rows, lambda r: r.ret21d, higher_is_better=True)
    mom63_map = percentile_score_map(rows, lambda r: r.ret63d, higher_is_better=True)
    rs63_map = percentile_score_map(rows, lambda r: r.rs_63d, higher_is_better=True) 
    mom126_map = percentile_score_map(rows, lambda r: r.ret126d, higher_is_better=True)
    mom252_map = percentile_score_map(rows, lambda r: r.ret252d, higher_is_better=True)
    mom121_map = percentile_score_map(rows, lambda r: r.mom12_1, higher_is_better=True)

    sector_map = percentile_score_map(rows, lambda r: r.sector_strength_63d, higher_is_better=True)

    vol_map = percentile_score_map(rows, lambda r: r.vol20, higher_is_better=False)
    down_map = percentile_score_map(rows, lambda r: r.downside20, higher_is_better=False)
    dd_map = percentile_score_map(rows, lambda r: r.dd60, higher_is_better=False)

    for r in rows:
        if r.close is None:
            continue

        mom21 = mom21_map.get(r.ticker)
        mom63 = mom63_map.get(r.ticker)
        rs63 = rs63_map.get(r.ticker) 
        mom126 = mom126_map.get(r.ticker)
        mom252 = mom252_map.get(r.ticker)
        mom121 = mom121_map.get(r.ticker)

        sector_score = sector_map.get(r.ticker)

        vol_s = vol_map.get(r.ticker)
        down_s = down_map.get(r.ticker)
        dd_s = dd_map.get(r.ticker)

        momentum_parts: list[tuple[float, float]] = []
        if mom21 is not None and WEIGHT_MOM_21D > 0:
            momentum_parts.append((WEIGHT_MOM_21D, mom21))
        if mom63 is not None and WEIGHT_MOM_63D > 0:
            momentum_parts.append((WEIGHT_MOM_63D, mom63))
        if rs63 is not None and WEIGHT_RS_63D > 0:
            momentum_parts.append((WEIGHT_RS_63D, rs63))
        if mom126 is not None and WEIGHT_MOM_126D > 0:
            momentum_parts.append((WEIGHT_MOM_126D, mom126))
        if mom252 is not None and WEIGHT_MOM_252D > 0:
            momentum_parts.append((WEIGHT_MOM_252D, mom252))
        if mom121 is not None and WEIGHT_MOM_12_1 > 0:
            momentum_parts.append((WEIGHT_MOM_12_1, mom121))

        stock_score_raw = None
        if momentum_parts:
            mom_log_sum = sum(w * safe_log_score(s) for w, s in momentum_parts)
            mom_weight_sum = sum(w for w, _ in momentum_parts)
            stock_score_raw = math.exp(mom_log_sum / mom_weight_sum) if mom_weight_sum > 0 else None

        risk_parts: list[tuple[float, float]] = []
        if vol_s is not None:
            risk_parts.append((WEIGHT_VOL, vol_s))
        if down_s is not None:
            risk_parts.append((WEIGHT_DOWNSIDE, down_s))
        if dd_s is not None:
            risk_parts.append((WEIGHT_DD, dd_s))

        risk_score = None
        if risk_parts:
            risk_log_sum = sum(w * safe_log_score(s) for w, s in risk_parts)
            risk_weight_sum = sum(w for w, _ in risk_parts)
            risk_score = math.exp(risk_log_sum / risk_weight_sum) if risk_weight_sum > 0 else None

        quality_score = None
        if quality_score_map is not None:
            quality_score = quality_score_map.get(r.ticker)

        institutional_score = None
        if institutional_score_map is not None:
            institutional_score = institutional_score_map.get(r.ticker)

        insider_score = None
        if insider_score_map is not None:
            insider_score = insider_score_map.get(r.ticker)

        valuation_score = None
        if valuation_score_map is not None:
            valuation_score = valuation_score_map.get(r.ticker)

        final_parts: list[tuple[float, float]] = []
        stock_block_weight = (
            WEIGHT_MOM_21D + WEIGHT_MOM_63D + WEIGHT_RS_63D +
            WEIGHT_MOM_126D + WEIGHT_MOM_252D + WEIGHT_MOM_12_1
        )

        if stock_score_raw is not None and stock_block_weight > 0:
            final_parts.append((stock_block_weight, stock_score_raw))
        if quality_score is not None and WEIGHT_QUALITY > 0:
            final_parts.append((WEIGHT_QUALITY, quality_score))
        if sector_score is not None and WEIGHT_SECTOR > 0:
            final_parts.append((WEIGHT_SECTOR, sector_score))
        if risk_score is not None and WEIGHT_RISK > 0:
            final_parts.append((WEIGHT_RISK, risk_score))
        if institutional_score is not None and WEIGHT_INSTITUTIONAL > 0:
            final_parts.append((WEIGHT_INSTITUTIONAL, institutional_score))
        if insider_score is not None and WEIGHT_INSIDER > 0:
            final_parts.append((WEIGHT_INSIDER, insider_score))
        if valuation_score is not None and WEIGHT_VALUATION > 0:
            final_parts.append((WEIGHT_VALUATION, valuation_score))

        if not final_parts:
            continue

        final_log_sum = sum(w * safe_log_score(s) for w, s in final_parts)
        final_weight_sum = sum(w for w, _ in final_parts)
        final_raw = math.exp(final_log_sum / final_weight_sum) if final_weight_sum > 0 else None

        r.stock_score_raw = stock_score_raw
        r.quality_score = quality_score
        r.sector_score = sector_score
        r.risk_score = risk_score
        r.final_score_raw = final_raw
        r.final_score_100 = raw_score_to_100(final_raw) if final_raw is not None else None
        r.signal = signal_from_score(r.final_score_100) if r.final_score_100 is not None else None

    scored = [r for r in rows if r.final_score_100 is not None]
    scored.sort(key=lambda x: (x.final_score_100 or -999), reverse=True)
    return scored


def _iterative_clip_and_normalize(
    weights: pd.Series,
    min_w: float,
    max_w: float,
    max_iter: int = 20,
) -> pd.Series:
    if weights.empty:
        return weights

    w = weights.copy().astype(float)

    if w.sum() <= 0:
        w[:] = 1.0 / len(w)
        return w

    w = w / w.sum()

    for _ in range(max_iter):
        prev = w.copy()
        w = w.clip(lower=min_w, upper=max_w)
        total = float(w.sum())

        if total <= 0:
            w[:] = 1.0 / len(w)
            break

        w = w / total

        if np.allclose(prev.values, w.values, atol=1e-8):
            break

    w = w / w.sum()
    return w


def compute_portfolio_weights(
    selected: list[StockMetric],
    *,
    method: str = PORTFOLIO_WEIGHT_METHOD,
    alpha_score: float = WEIGHT_ALPHA_SCORE,
    min_w: float = MIN_WEIGHT,
    max_w: float = MAX_WEIGHT,
    vol_fallback: float = VOL_FALLBACK,
    vol_floor: float = VOL_WEIGHT_FLOOR,
    dynamic_vol_floor: float | None = None, 
) -> list[StockMetric]:
    if not selected:
        return selected

    if len(selected) == 1:
        selected[0].portfolio_weight = 1.0
        return selected

    tickers = [r.ticker for r in selected]
    scores = pd.Series(
        [float(r.final_score_100 or 1) for r in selected],
        index=tickers,
        dtype=float,
    )
    vols = pd.Series(
        [
            float(r.vol20) if (r.vol20 is not None and r.vol20 > 0) else np.nan
            for r in selected
        ],
        index=tickers,
        dtype=float,
    )

    if vols.notna().any():
        vols = vols.fillna(float(vols.dropna().median()))
    else:
        vols[:] = vol_fallback

    vols = vols.replace(0, np.nan).fillna(vol_fallback)
    
    active_floor = dynamic_vol_floor if dynamic_vol_floor is not None else vol_floor
    effective_vols = vols.clip(lower=active_floor)

    if method == "equal_weight":
        raw = pd.Series(1.0, index=tickers, dtype=float)
    elif method == "inverse_vol":
        raw = 1.0 / effective_vols
    else:
        raw = (scores.clip(lower=1.0) ** alpha_score) / effective_vols

    if not np.isfinite(raw).all() or raw.sum() <= 0:
        raw = pd.Series(1.0, index=tickers, dtype=float)

    w = raw / raw.sum()

    n = len(selected)
    feasible_min = min_w * n <= 1.0
    feasible_max = max_w * n >= 1.0

    if feasible_min and feasible_max:
        w = _iterative_clip_and_normalize(w, min_w=min_w, max_w=max_w)

    w = w / w.sum()

    for r in selected:
        r.portfolio_weight = float(w.loc[r.ticker])

    return selected


def stock_metric_to_card(r: StockMetric) -> dict[str, Any]:
    return {
        "ticker": r.ticker,
        "signal": r.signal,
        "score": r.final_score_100,
        "label": r.signal,
        "sector": r.sector,
        "close": round(r.close, 2) if r.close is not None else None,
        "rsi": round(r.rsi14, 1) if r.rsi14 is not None else None,
        "ret5d": round(r.ret5d, 1) if r.ret5d is not None else None,
        "ret20d": round(r.ret20d, 1) if r.ret20d is not None else None,
        "ret21d": round(r.ret21d, 1) if r.ret21d is not None else None,
        "ret63d": round(r.ret63d, 1) if r.ret63d is not None else None,
        "rs_63d": round(r.rs_63d, 2) if r.rs_63d is not None else None, 
        "ret126d": round(r.ret126d, 1) if r.ret126d is not None else None,
        "ret252d": round(r.ret252d, 1) if r.ret252d is not None else None,
        "mom12_1": round(r.mom12_1, 1) if r.mom12_1 is not None else None,
        "vol20": round(r.vol20, 4) if r.vol20 is not None else None,
        "downside20": round(r.downside20, 4) if r.downside20 is not None else None,
        "dd60": round(r.dd60, 4) if r.dd60 is not None else None,
        "stock_score_raw": round(r.stock_score_raw, 4) if r.stock_score_raw is not None else None,
        "quality_score": round(r.quality_score, 4) if r.quality_score is not None else None,
        "sector_strength_20d": round(r.sector_strength_63d, 2) if r.sector_strength_63d is not None else None,
        "sector_strength_63d": round(r.sector_strength_63d, 2) if r.sector_strength_63d is not None else None,
        "sector_score": round(r.sector_score, 4) if r.sector_score is not None else None,
        "risk_score": round(r.risk_score, 4) if r.risk_score is not None else None,
        "final_score_raw": round(r.final_score_raw, 4) if r.final_score_raw is not None else None,
        "weight": round(r.portfolio_weight, 4) if r.portfolio_weight is not None else None,
        "weight_pct": round((r.portfolio_weight or 0.0) * 100.0, 1) if r.portfolio_weight is not None else None,
        "passes_absolute_momentum": passes_absolute_momentum(r),
        "news_reason": r.news_reason, 
    }


# -----------------------------
# [고도화] 로컬 뉴스 파일 분석 로직
# -----------------------------
def analyze_news_from_local(ticker: str) -> tuple[int, bool, str]:
    """로컬 GDELT JSON을 읽어 중복을 제거한 정규식 기반 점수/플래그를 반환합니다."""
    news_file = NEWS_DIR / f"{ticker.upper()}.json"
    
    if not news_file.exists():
        return 0, False, ""

    try:
        with open(news_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        items = data.get("items", [])
        if not items:
            return 0, False, ""

        found_red_flags = set()
        found_mild_neg = set()
        found_pos = set()

        for item in items[:NEWS_LOOKBACK_ITEMS]:
            title = item.get("title", "").upper()
            
            for match in RED_FLAG_REGEX.finditer(title):
                found_red_flags.add(match.group(0))
            for match in MILD_NEG_REGEX.finditer(title):
                found_mild_neg.add(match.group(0))
            for match in POS_REGEX.finditer(title):
                found_pos.add(match.group(0))

        is_red_flag = len(found_red_flags) > 0
        penalty = len(found_mild_neg) * -15  # 중복 제거 후 1건당 -15점
        bonus = len(found_pos) * 1          # 중복 제거 후 1건당 +1점

        total_adjustment = penalty + bonus
        
        reasons = []
        if found_red_flags: reasons.append(f"[KILL SWITCH] {', '.join(found_red_flags)}")
        if found_mild_neg: reasons.append(f"[-15pt] {', '.join(found_mild_neg)}")
        if found_pos: reasons.append(f"[+1pt] {', '.join(found_pos)}")
        reason_str = " | ".join(reasons)

        return total_adjustment, is_red_flag, reason_str
    
    except Exception as e:
        print(f"[WARN] Failed to process local news for {ticker}: {e}")
        return 0, False, ""


def build_snapshot() -> dict[str, Any]:
    by_ticker, groups = parse_universe()
    quality_score_map      = load_quality_score_map()
    institutional_score_map = load_institutional_score_map()
    insider_score_map       = load_insider_score_map()
    valuation_score_map     = load_valuation_score_map()

    all_needed_tickers = sorted(
        set(groups["sp500"])
        | set(groups.get("sp400", []))
        | set(groups.get("sp600", []))
        | set(groups["nasdaq100"])
        | set(groups["dow30"])
    )

    print(f"[INFO] Total unique tickers to fetch: {len(all_needed_tickers)}")
    print(f"[INFO] Loaded quality scores: {len(quality_score_map)}")
    print(f"[INFO] Loaded institutional scores: {len(institutional_score_map)}")
    print(f"[INFO] Loaded insider scores: {len(insider_score_map)}")
    print(f"[INFO] Loaded valuation scores: {len(valuation_score_map)}")
    close_map = download_close_map(all_needed_tickers)

    spy_close = close_map.get(BENCHMARK_TICKER)
    spy_vol20 = annualized_volatility(spy_close, 20) if spy_close is not None else None
    dynamic_floor = max(VOL_WEIGHT_FLOOR, spy_vol20) if spy_vol20 else VOL_WEIGHT_FLOOR

    snapshot_groups: list[dict[str, Any]] = []

    for group_key in ["sp500", "sp400", "sp600", "nasdaq100", "dow30"]:
        tickers = groups.get(group_key, [])
        metrics = build_metrics_for_group(tickers, by_ticker, close_map)
        scored = score_group(
            metrics,
            quality_score_map=quality_score_map,
            institutional_score_map=institutional_score_map,
            insider_score_map=insider_score_map,
            valuation_score_map=valuation_score_map,
        )

        # -----------------------------
        # [고도화] 뉴스 필터 및 레드플래그 퇴출 적용
        # -----------------------------
        print(f"[INFO] Applying Local News Filter to {group_key} top {NEWS_SCAN_COUNT} candidates...")
        candidates = scored[:NEWS_SCAN_COUNT]
        valid_scored = scored[NEWS_SCAN_COUNT:] # 상위 50개 밖의 종목들은 일단 그대로 보존
        
        for r in candidates:
            if r.final_score_100 is not None:
                adj_score, is_red_flag, reason = analyze_news_from_local(r.ticker)
                
                # 치명적 악재 발견 시 완전히 리스트에서 배제
                if is_red_flag:
                    print(f"  > [DROP] {r.ticker} excluded due to Red Flag: {reason}")
                    r.news_reason = reason 
                    continue 

                if adj_score != 0:
                    print(f"  > [ADJ] {r.ticker}: {adj_score} pts ({reason})")
                    r.final_score_100 = clip(r.final_score_100 + adj_score, 0, 100)
                    r.signal = signal_from_score(r.final_score_100)
                
                r.news_reason = reason
                valid_scored.append(r) 

        # 다시 최종 점수 기준으로 정렬
        valid_scored.sort(key=lambda x: (x.final_score_100 or -999), reverse=True)
        # -----------------------------

        selected = select_snapshot_rows(
            valid_scored,
            top_n=TOP_N,
            sector_max_names=SECTOR_MAX_NAMES,
        )
        selected = compute_portfolio_weights(
            selected,
            method=PORTFOLIO_WEIGHT_METHOD,
            alpha_score=WEIGHT_ALPHA_SCORE,
            min_w=MIN_WEIGHT,
            max_w=MAX_WEIGHT,
            vol_fallback=VOL_FALLBACK,
            vol_floor=VOL_WEIGHT_FLOOR,
            dynamic_vol_floor=dynamic_floor, 
        )

        top_cards = [stock_metric_to_card(r) for r in selected]
        
        # 필터 통과 종목 개수 산정 시 valid_scored 사용
        filtered_count = sum(1 for r in valid_scored if passes_absolute_momentum(r))

        snapshot_groups.append(
            {
                "key": group_key,
                "label": GROUP_LABELS[group_key],
                "description": GROUP_DESCRIPTIONS[group_key],
                "top3": top_cards,
                "count": len(valid_scored),
                "filtered_count_absolute_momentum": filtered_count,
                "quality_count": sum(1 for r in valid_scored if r.quality_score is not None),
                "portfolio_construction": {
                    "method": PORTFOLIO_WEIGHT_METHOD,
                    "top_n": TOP_N,
                    "alpha_score": WEIGHT_ALPHA_SCORE,
                    "min_weight": MIN_WEIGHT,
                    "max_weight": MAX_WEIGHT,
                    "vol_floor": VOL_WEIGHT_FLOOR,
                    "dynamic_vol_floor": round(dynamic_floor, 4), 
                    "absolute_momentum_63d_min": ABS_MOM_63D_MIN,
                    "absolute_momentum_252d_min": ABS_MOM_252D_MIN,
                    "sector_max_names": SECTOR_MAX_NAMES,
                },
            }
        )

    flat_map = {g["key"]: g["top3"] for g in snapshot_groups}

    result = {
        "generated_at": utc_now_iso(),
        "market_context": {
            "benchmark": BENCHMARK_TICKER,
            "spy_vol20": round(spy_vol20, 4) if spy_vol20 else None,
            "dynamic_vol_floor_used": round(dynamic_floor, 4)
        },
        "weights": {
            "momentum_21d": WEIGHT_MOM_21D,
            "momentum_63d": WEIGHT_MOM_63D,
            "rs_63d": WEIGHT_RS_63D, 
            "momentum_126d": WEIGHT_MOM_126D,
            "momentum_252d": WEIGHT_MOM_252D,
            "momentum_12_1": WEIGHT_MOM_12_1,
            "quality": WEIGHT_QUALITY,
            "sector": WEIGHT_SECTOR,
            "risk": WEIGHT_RISK,
            "risk_vol": WEIGHT_VOL,
            "risk_downside": WEIGHT_DOWNSIDE,
            "risk_drawdown": WEIGHT_DD,
        },
        "portfolio_construction": {
            "method": PORTFOLIO_WEIGHT_METHOD,
            "top_n": TOP_N,
            "score_alpha": WEIGHT_ALPHA_SCORE,
            "min_weight": MIN_WEIGHT,
            "max_weight": MAX_WEIGHT,
            "vol_fallback": VOL_FALLBACK,
            "vol_floor_base": VOL_WEIGHT_FLOOR,
            "absolute_momentum_63d_min": ABS_MOM_63D_MIN,
            "absolute_momentum_252d_min": ABS_MOM_252D_MIN,
            "sector_max_names": SECTOR_MAX_NAMES,
            "summary": (
                "Top-N names are selected by geometric composite score, then filtered by relaxed absolute momentum "
                "and sector cap, and finally allocated using score × inverse dynamic volatility weighting."
            ),
            "formula": "weight_i ∝ (score_i ^ alpha) / max(vol20_i, max(vol_floor, spy_vol20))",
        },
        "sector_robustness": {
            "min_count_full": SECTOR_MIN_COUNT_FULL,
            "shrink_map": SECTOR_SHRINK_MAP,
            "default_shrink": SECTOR_DEFAULT_SHRINK,
            "summary": "Sector strength is shrunk toward market average when sector sample count is small.",
        },
        "method": {
            "summary": (
                "Geometric (Multiplicative) percentile-based composite score using mid-horizon momentum, relative strength, quality, "
                "sector strength, and risk, with relaxed absolute momentum filter and sector cap."
            ),
            "notes": [
                "Momentum is ranked cross-sectionally using 21D, 63D, 126D, 12-1 momentum, AND 63D Relative Strength vs SPY.",
                "Scoring uses a multiplicative (geometric) approach: low score in any one factor severely penalizes the final score.",
                "Volatility targeting: Portfolio allocation uses a dynamic volatility floor tied to SPY's recent 20D volatility.",
                "Asymmetric News Filter: Exact word matching via regex. Drops red flags entirely, mild negs get -15, positives get +1 (tie-breaker). Unique per ticker to prevent over-counting.", 
                "252D momentum weight is set to zero in the main scoring configuration.",
                "21D momentum weight is reduced to minimize short-term noise.",
                "Quality score is loaded from quality_snapshot.json and merged by ticker.",
                "Sector strength = sector average 63D return minus group average 63D return.",
                "Small-sample sectors are shrunk toward market average before sector ranking.",
                "Risk score uses inverse percentile of 20D annualized volatility, 20D downside volatility, and 60D max drawdown.",
                "Final score is mapped from raw [0, 1] scale to [0, 100].",
                "Relaxed absolute momentum filter requires 63D return > -5% and 252D return > -10%.",
                "Sector cap limits the number of selected names per sector.",
                "RSI, 5D, and 20D values are retained for display but excluded from scoring.",
                "Quality is applied to snapshot scoring only; historical backtest integration requires point-in-time quality data.",
            ],
        },
        "groups": snapshot_groups,
        "sp500": flat_map.get("sp500", []),
        "sp400": flat_map.get("sp400", []),
        "sp600": flat_map.get("sp600", []),
        "nasdaq100": flat_map.get("nasdaq100", []),
        "dow30": flat_map.get("dow30", []),
    }

    return result


def main():
    snapshot = build_snapshot()
    save_json(OUT_FILE, snapshot)
    print(f"[OK] Saved -> {OUT_FILE}")
    copy_to_public(OUT_FILE)


if __name__ == "__main__":
    main()