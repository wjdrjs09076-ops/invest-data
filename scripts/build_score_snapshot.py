# scripts/build_score_snapshot.py
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

UNIVERSE_FILE = DATA_DIR / "universe.json"
SP500_FILE = DATA_DIR / "sp500_tickers.json"
OUT_FILE = DATA_DIR / "score_snapshot.json"

# -----------------------------
# Config
# -----------------------------
BATCH_SIZE = 50
LOOKBACK_PERIOD = "8mo"   # RSI/20D/60D 계산 여유
TOP_N = 3

# 가중치 (합계 1.00)
WEIGHT_MOM_20D = 0.30
WEIGHT_MOM_5D = 0.15
WEIGHT_RSI = 0.15
WEIGHT_SECTOR = 0.15
WEIGHT_RISK = 0.20

# risk 내부 가중치
WEIGHT_VOL = 0.60
WEIGHT_DD = 0.40

GROUP_LABELS = {
    "sp500": "S&P 500",
    "nasdaq100": "NASDAQ-100",
    "dow30": "Dow 30",
}

GROUP_DESCRIPTIONS = {
    "sp500": "Ranked by composite score (Momentum + RSI + Sector strength + Risk).",
    "nasdaq100": "Ranked by composite score (Momentum + RSI + Sector strength + Risk).",
    "dow30": "Ranked by composite score (Momentum + RSI + Sector strength + Risk).",
}


@dataclass
class StockMetric:
    ticker: str
    sector: str
    close: float | None
    ret5d: float | None
    ret20d: float | None
    rsi14: float | None
    vol20: float | None
    dd60: float | None
    stock_score_raw: float | None = None
    sector_strength_20d: float | None = None
    sector_score: float | None = None
    risk_score: float | None = None
    final_score_raw: float | None = None
    final_score_100: int | None = None
    signal: str | None = None


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


def rsi_score_fn(rsi: float | None) -> float:
    if rsi is None:
        return 0.0

    # 55 근처 최적, 과매수/과매도 과도하면 점수 감소
    score = 1.0 - abs(rsi - 55.0) / 25.0
    return clip(score, -1.0, 1.0)


def momentum_20d_score_fn(ret20d: float | None) -> float:
    if ret20d is None:
        return 0.0
    # +15%면 상단 근처, -15%면 하단 근처
    return clip(ret20d / 15.0, -1.0, 1.0)


def momentum_5d_score_fn(ret5d: float | None) -> float:
    if ret5d is None:
        return 0.0
    # +8%면 상단 근처, -8%면 하단 근처
    return clip(ret5d / 8.0, -1.0, 1.0)


def volatility_score_fn(vol20: float | None) -> float:
    if vol20 is None:
        return 0.0
    # 연율 변동성 15% 이하 좋음, 55% 이상 매우 나쁨
    # 0.15 -> 1.0 / 0.55 -> -1.0
    score = 1.0 - ((vol20 - 0.15) / 0.20)
    return clip(score, -1.0, 1.0)


def drawdown_score_fn(dd60: float | None) -> float:
    if dd60 is None:
        return 0.0
    # dd60는 음수 (예: -0.08)
    # -5% 이내 좋음, -30% 이하 매우 나쁨
    score = 1.0 - (abs(dd60) / 0.15)
    return clip(score, -1.0, 1.0)


def sector_score_fn(sector_strength_pct: float | None) -> float:
    if sector_strength_pct is None:
        return 0.0
    # 시장 대비 +5% 이상 강하면 거의 +1
    return clip(sector_strength_pct / 10.0, -1.0, 1.0)


def raw_score_to_100(raw: float) -> int:
    # raw [-1, 1] -> [0, 100]
    score = round((clip(raw, -1.0, 1.0) + 1.0) * 50.0)
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
    """
    최대한 다양한 universe.json 구조를 받아서
    by_ticker 메타데이터 + 그룹별 티커 목록으로 정규화
    """
    universe = load_json(UNIVERSE_FILE, default=None)

    by_ticker: dict[str, dict[str, Any]] = {}
    groups: dict[str, list[str]] = {
        "sp500": [],
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
        # fallback: sp500_tickers.json만 있는 경우
        tickers = load_json(SP500_FILE, default=[])
        for t in tickers:
            t2 = normalize_ticker(t)
            ensure_meta(t2)
            _add_group_flag(by_ticker[t2], "sp500")
            groups["sp500"].append(t2)
        groups["sp500"] = sorted(set(groups["sp500"]))
        return by_ticker, groups

    # case A: universe가 list[dict]
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

            # boolean membership도 허용
            if item.get("sp500") is True:
                _add_group_flag(meta, "sp500")
                groups["sp500"].append(ticker)
            if item.get("nasdaq100") is True:
                _add_group_flag(meta, "nasdaq100")
                groups["nasdaq100"].append(ticker)
            if item.get("dow30") is True:
                _add_group_flag(meta, "dow30")
                groups["dow30"].append(ticker)

    # case B: universe가 dict
    elif isinstance(universe, dict):
        # B-1) {"items":[...]} 형태
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
                if item.get("nasdaq100") is True:
                    _add_group_flag(meta, "nasdaq100")
                    groups["nasdaq100"].append(ticker)
                if item.get("dow30") is True:
                    _add_group_flag(meta, "dow30")
                    groups["dow30"].append(ticker)

        # B-2) 그룹별 배열 직접 포함
        for group_key in ["sp500", "nasdaq100", "dow30"]:
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

        # B-3) {"AAPL": {...}} map 형태
        # ticker-like key를 meta map으로 해석
        for k, v in universe.items():
            if k in {"items", "sp500", "nasdaq100", "dow30"}:
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
            if v.get("nasdaq100") is True:
                _add_group_flag(meta, "nasdaq100")
                groups["nasdaq100"].append(maybe_ticker)
            if v.get("dow30") is True:
                _add_group_flag(meta, "dow30")
                groups["dow30"].append(maybe_ticker)

    # fallback: sp500_tickers.json은 무조건 보정
    sp500_fallback = load_json(SP500_FILE, default=[])
    for t in sp500_fallback:
        t2 = normalize_ticker(t)
        if not t2:
            continue
        ensure_meta(t2)
        _add_group_flag(by_ticker[t2], "sp500")
        groups["sp500"].append(t2)

    # dedupe + sort
    for g in groups:
        groups[g] = sorted(set(groups[g]))

    return by_ticker, groups


def download_close_map(tickers: list[str]) -> dict[str, pd.Series]:
    """
    yfinance로 티커별 종가 시계열 획득
    """
    out: dict[str, pd.Series] = {}
    all_tickers = sorted(set(normalize_ticker(t) for t in tickers if t))
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
        except Exception as e:
            print(f"[WARN] download batch failed: {batch[:5]}... -> {e}")
            continue

        if df is None or df.empty:
            continue

        # multi-index 형태
        if isinstance(df.columns, pd.MultiIndex):
            for ticker in batch:
                if ticker not in df.columns.get_level_values(0):
                    continue
                sub = df[ticker]
                if "Close" not in sub.columns:
                    continue
                close = sub["Close"].dropna()
                if not close.empty:
                    out[ticker] = close
        else:
            # 단일 ticker 내려오는 경우
            if len(batch) == 1 and "Close" in df.columns:
                close = df["Close"].dropna()
                if not close.empty:
                    out[batch[0]] = close

    return out


def build_metrics_for_group(
    group_tickers: list[str],
    by_ticker: dict[str, dict[str, Any]],
    close_map: dict[str, pd.Series],
) -> list[StockMetric]:
    rows: list[StockMetric] = []

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
                    rsi14=None,
                    vol20=None,
                    dd60=None,
                )
            )
            continue

        rsi_series = compute_rsi(close, 14)

        rows.append(
            StockMetric(
                ticker=ticker,
                sector=sector,
                close=safe_float(close.iloc[-1]),
                ret5d=pct_return(close, 5),
                ret20d=pct_return(close, 20),
                rsi14=safe_float(rsi_series.iloc[-1]) if not rsi_series.empty else None,
                vol20=annualized_volatility(close, 20),
                dd60=max_drawdown(close, 60),
            )
        )

    return rows


def compute_sector_strength(rows: list[StockMetric]) -> tuple[dict[str, float], float | None]:
    valid = [r for r in rows if r.ret20d is not None]
    if not valid:
        return {}, None

    market_avg = float(np.mean([r.ret20d for r in valid if r.ret20d is not None]))

    bucket: dict[str, list[float]] = {}
    for r in valid:
        sector = r.sector or "Unknown"
        if sector not in bucket:
            bucket[sector] = []
        bucket[sector].append(float(r.ret20d))

    sector_strength_map: dict[str, float] = {}
    for sector, vals in bucket.items():
        sector_avg = float(np.mean(vals))
        sector_strength_map[sector] = sector_avg - market_avg

    return sector_strength_map, market_avg


def score_group(rows: list[StockMetric]) -> list[StockMetric]:
    sector_strength_map, _market_avg = compute_sector_strength(rows)

    for r in rows:
        if r.close is None:
            continue

        mom20 = momentum_20d_score_fn(r.ret20d)
        mom5 = momentum_5d_score_fn(r.ret5d)
        rsi_s = rsi_score_fn(r.rsi14)

        vol_s = volatility_score_fn(r.vol20)
        dd_s = drawdown_score_fn(r.dd60)
        risk_score = (WEIGHT_VOL * vol_s) + (WEIGHT_DD * dd_s)

        sector_strength = sector_strength_map.get(r.sector, 0.0)
        sector_score = sector_score_fn(sector_strength)

        stock_score_raw = (
            WEIGHT_MOM_20D * mom20
            + WEIGHT_MOM_5D * mom5
            + WEIGHT_RSI * rsi_s
        )

        final_raw = (
            stock_score_raw
            + WEIGHT_SECTOR * sector_score
            + WEIGHT_RISK * risk_score
        )

        r.stock_score_raw = stock_score_raw
        r.sector_strength_20d = sector_strength
        r.sector_score = sector_score
        r.risk_score = risk_score
        r.final_score_raw = final_raw
        r.final_score_100 = raw_score_to_100(final_raw)
        r.signal = signal_from_score(r.final_score_100)

    scored = [r for r in rows if r.final_score_100 is not None]
    scored.sort(key=lambda x: (x.final_score_100 or -999), reverse=True)
    return scored


def stock_metric_to_card(r: StockMetric) -> dict[str, Any]:
    return {
        "ticker": r.ticker,
        "signal": r.signal,
        "score": r.final_score_100,
        "label": r.signal,  # 프론트 호환용
        "sector": r.sector,
        "close": round(r.close, 2) if r.close is not None else None,
        "rsi": round(r.rsi14, 1) if r.rsi14 is not None else None,
        "ret5d": round(r.ret5d, 1) if r.ret5d is not None else None,
        "ret20d": round(r.ret20d, 1) if r.ret20d is not None else None,
        "vol20": round(r.vol20, 4) if r.vol20 is not None else None,
        "dd60": round(r.dd60, 4) if r.dd60 is not None else None,
        "stock_score_raw": round(r.stock_score_raw, 4) if r.stock_score_raw is not None else None,
        "sector_strength_20d": round(r.sector_strength_20d, 2) if r.sector_strength_20d is not None else None,
        "sector_score": round(r.sector_score, 4) if r.sector_score is not None else None,
        "risk_score": round(r.risk_score, 4) if r.risk_score is not None else None,
        "final_score_raw": round(r.final_score_raw, 4) if r.final_score_raw is not None else None,
    }


def build_snapshot() -> dict[str, Any]:
    by_ticker, groups = parse_universe()

    all_needed_tickers = sorted(
        set(groups["sp500"]) | set(groups["nasdaq100"]) | set(groups["dow30"])
    )

    print(f"[INFO] Total unique tickers to fetch: {len(all_needed_tickers)}")
    close_map = download_close_map(all_needed_tickers)

    snapshot_groups: list[dict[str, Any]] = []

    for group_key in ["sp500", "nasdaq100", "dow30"]:
        tickers = groups.get(group_key, [])
        metrics = build_metrics_for_group(tickers, by_ticker, close_map)
        scored = score_group(metrics)
        top_cards = [stock_metric_to_card(r) for r in scored[:TOP_N]]

        snapshot_groups.append(
            {
                "key": group_key,
                "label": GROUP_LABELS[group_key],
                "description": GROUP_DESCRIPTIONS[group_key],
                "top3": top_cards,
                "count": len(scored),
            }
        )

    # legacy-friendly flattened keys도 같이 저장
    flat_map = {
        g["key"]: g["top3"]
        for g in snapshot_groups
    }

    result = {
        "generated_at": utc_now_iso(),
        "weights": {
            "momentum_20d": WEIGHT_MOM_20D,
            "momentum_5d": WEIGHT_MOM_5D,
            "rsi": WEIGHT_RSI,
            "sector": WEIGHT_SECTOR,
            "risk": WEIGHT_RISK,
            "risk_vol": WEIGHT_VOL,
            "risk_drawdown": WEIGHT_DD,
        },
        "method": {
            "summary": "Composite score using momentum, RSI, sector strength, and risk.",
            "notes": [
                "Sector strength = sector average 20D return minus group average 20D return.",
                "Risk score combines annualized 20D volatility and 60D max drawdown.",
                "Final score is mapped from raw [-1, 1] scale to [0, 100].",
            ],
        },
        "groups": snapshot_groups,
        "sp500": flat_map.get("sp500", []),
        "nasdaq100": flat_map.get("nasdaq100", []),
        "dow30": flat_map.get("dow30", []),
    }

    return result


def main():
    snapshot = build_snapshot()
    save_json(OUT_FILE, snapshot)
    print(f"[OK] Saved -> {OUT_FILE}")


if __name__ == "__main__":
    main()
