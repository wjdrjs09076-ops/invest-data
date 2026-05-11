from __future__ import annotations

import json
import os
import shutil
import pickle
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

from build_score_snapshot import (
    build_metrics_for_group,
    score_group,
    compute_portfolio_weights,
    PORTFOLIO_WEIGHT_METHOD,
    WEIGHT_ALPHA_SCORE,
    MIN_WEIGHT,
    MAX_WEIGHT,
    VOL_FALLBACK,
    VOL_WEIGHT_FLOOR,
    ABS_MOM_63D_MIN,
    ABS_MOM_252D_MIN,
    SECTOR_MAX_NAMES,
    annualized_volatility, # [업그레이드 반영] 동적 변동성 계산을 위해 추가 임포트
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"
CACHE_DIR = DATA_DIR / "cache"
PRICE_CACHE_DIR = CACHE_DIR / "prices"
SNAPSHOT_CACHE_DIR = CACHE_DIR / "rebalance_snapshots"
CACHE_ENABLED = True


# ----------------------------
# CONFIG
# ----------------------------

BACKTEST_YEARS = 10

REBALANCE = "monthly"   # "monthly" or "weekly"
SELECTION = "TOP"
TOP_N = 15
HOLD_BUFFER_N = 25  # [NEW] 버퍼 랭킹: 이 순위 안쪽에 있으면 기존 보유 종목 유지

TRANSACTION_COST = 0.0015
BENCHMARK = "SPY"

# ── Altman Z-score filter (experimental) ─────────────────────────────────────
USE_ZSCORE_FILTER = False          # override to True in run_backtest_zscore.py
ZSCORE_DISTRESS_SET: set[str] = set()   # populated at runtime if filter enabled
ZSCORE_DATE_LOOKUP = None          # callable(date) -> set[str]; enables point-in-time mode

# [VIX 시스템 붕괴 필터 설정]
VIX_TICKER = "^VIX"
VIX_CRASH_THRESHOLD = 40.0 # 진짜 패닉장에서만 발동

MIN_HISTORY = 220

CURRENT_UNIVERSE_FILES = [
    DATA_DIR / "sp500_current_wiki.json",
    DATA_DIR / "sp400_current_wiki.json",
    DATA_DIR / "sp600_current_wiki.json",
]

# Sharadar SP500 이벤트를 우선 사용, 없으면 Wikipedia fallback
_SP500_SHARADAR_EVENTS = DATA_DIR / "sp500_membership_events_sharadar.json"
_SP500_WIKI_EVENTS     = DATA_DIR / "sp500_membership_events.json"

MEMBERSHIP_EVENTS_FILES = [
    _SP500_SHARADAR_EVENTS if _SP500_SHARADAR_EVENTS.exists() else _SP500_WIKI_EVENTS,
    DATA_DIR / "sp400_membership_events.json",
    DATA_DIR / "sp600_membership_events.json",
]

SEP_PRICES_FILE = DATA_DIR / "sep_prices.pkl"

# Regime / exposure
REGIME_MA_WINDOW = 200
REGIME_MOM_WINDOW = 63

RISK_ON_EXPOSURE = 1.00
MID_EXPOSURE = 0.85
RISK_OFF_EXPOSURE = 0.40

# Buffer + smoothing
REGIME_BUFFER = 0.005       # 0.5%
REGIME_CONFIRM_DAYS = 2      # candidate signal must persist for N days before switching

# Defensive sleeve (TAIL 30%, DBMF 70% 반영)
DEFENSIVE_TICKERS = ["TAIL","DBMF"]
RISK_OFF_DEFENSIVE_WEIGHTS = {
    "TAIL": 0.3,
    "DBMF": 0.7,
}

# Exposure rebalance frequency: "daily" / "weekly" / "monthly"
EXPOSURE_REBALANCE = "daily"

# [실험용] None 이면 자동 계산, 날짜 문자열("2015-05-26") 이면 그 날짜로 고정
START_DATE_OVERRIDE: str | None = None

# ----------------------------


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def copy_to_public(src: Path) -> None:
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst = PUBLIC_DATA_DIR / src.name
    shutil.copy2(src, dst)
    print(f"Copied -> {dst}")


def normalize_ticker(t: str) -> str:
    return str(t).strip().upper().replace(".", "-")


def get_start_date() -> str:
    if START_DATE_OVERRIDE:
        return START_DATE_OVERRIDE
    today = datetime.now(timezone.utc)
    start = today - timedelta(days=365 * BACKTEST_YEARS + 320)
    return start.strftime("%Y-%m-%d")


def load_current_universe() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    for file in CURRENT_UNIVERSE_FILES:
        payload = load_json(file, default={})
        items = payload.get("items", [])

        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            ticker = normalize_ticker(item.get("ticker", ""))
            if not ticker:
                continue

            existing_flags = out.get(ticker, {}).get("indexFlags", [])
            new_flags = item.get("indexFlags", [])

            if isinstance(existing_flags, str):
                existing_flags = [existing_flags]
            if isinstance(new_flags, str):
                new_flags = [new_flags]

            merged_flags = sorted(set(existing_flags) | set(new_flags))

            out[ticker] = {
                "ticker": ticker,
                "name": item.get("name", out.get(ticker, {}).get("name", "")),
                "sector": item.get("sector", out.get(ticker, {}).get("sector", "Unknown")) or "Unknown",
                "subIndustry": item.get("subIndustry", out.get(ticker, {}).get("subIndustry", "")),
                "headquarters": item.get("headquarters", out.get(ticker, {}).get("headquarters", "")),
                "indexFlags": merged_flags,
            }

    if not out:
        raise RuntimeError("Failed to load current universe from S&P1500 wiki files")

    return out


def load_membership_events() -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []

    for file in MEMBERSHIP_EVENTS_FILES:
        payload = load_json(file, default={})
        events = payload.get("events", [])

        if not isinstance(events, list):
            continue

        for ev in events:
            if not isinstance(ev, dict):
                continue

            date_str = str(ev.get("date", "")).strip()
            added = [normalize_ticker(x) for x in ev.get("added", []) if str(x).strip()]
            removed = [normalize_ticker(x) for x in ev.get("removed", []) if str(x).strip()]

            if not date_str:
                continue

            cleaned.append(
                {
                    "date": date_str,
                    "added": added,
                    "removed": removed,
                }
            )

    if not cleaned:
        raise RuntimeError("Failed to load membership events from S&P1500 wiki event files")

    cleaned.sort(key=lambda x: x["date"])
    return cleaned


def reconstruct_membership_as_of(
    as_of: pd.Timestamp,
    current_universe: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
) -> set[str]:
    members = set(current_universe.keys())

    for ev in sorted(events, key=lambda x: x["date"], reverse=True):
        ev_date = pd.to_datetime(ev["date"])
        if ev_date > as_of:
            for t in ev.get("added", []):
                members.discard(t)
            for t in ev.get("removed", []):
                members.add(t)

    return members


def load_sep_prices(tickers: list[str], start_date: str) -> dict[str, pd.Series]:
    """SHARADAR/SEP 가격 캐시에서 조정 종가를 로드한다."""
    if not SEP_PRICES_FILE.exists():
        return {}
    try:
        payload = pd.read_pickle(SEP_PRICES_FILE)
        df: pd.DataFrame = payload.get("prices") if isinstance(payload, dict) else payload
        if df is None or df.empty:
            return {}
        df.index = pd.to_datetime(df.index)
        df = df[df.index >= pd.Timestamp(start_date)]
        result: dict[str, pd.Series] = {}
        for t in tickers:
            if t in df.columns:
                s = df[t].dropna()
                if len(s) >= MIN_HISTORY:
                    result[t] = s
        print(f"[SEP CACHE] Loaded {len(result)} tickers from {SEP_PRICES_FILE.name}")
        return result
    except Exception as e:
        print(f"[WARN] SEP cache load error: {e}")
        return {}


def _fill_price_gap(price_map: dict[str, pd.Series], gap_tickers: list[str], gap_start: str) -> None:
    """SEP 캐시 마지막 날짜 이후의 갭을 yfinance로 보완한다. 200개씩 청크."""
    if not gap_tickers:
        return

    chunk_size = 200
    chunks = [gap_tickers[i : i + chunk_size] for i in range(0, len(gap_tickers), chunk_size)]

    for chunk in chunks:
        try:
            df = yf.download(
                tickers=chunk,
                start=gap_start,
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
            if df is None or df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                level0 = set(df.columns.get_level_values(0))
                for t in chunk:
                    try:
                        if t not in level0:
                            continue
                        gap_s = df[t]["Close"].dropna()
                        if gap_s.empty:
                            continue
                        gap_s.index = pd.to_datetime(gap_s.index)
                        if t in price_map:
                            combined = pd.concat([price_map[t], gap_s])
                            price_map[t] = combined[~combined.index.duplicated(keep="last")].sort_index()
                        else:
                            price_map[t] = gap_s
                    except Exception:
                        continue
            elif "Close" in df.columns and len(chunk) == 1:
                t = chunk[0]
                gap_s = df["Close"].dropna()
                gap_s.index = pd.to_datetime(gap_s.index)
                if t in price_map:
                    combined = pd.concat([price_map[t], gap_s])
                    price_map[t] = combined[~combined.index.duplicated(keep="last")].sort_index()
                else:
                    price_map[t] = gap_s
        except Exception as e:
            print(f"[WARN] Gap fill chunk error: {e}")


def download_prices_raw(tickers: list[str]) -> dict[str, pd.Series]:
    all_tickers = sorted(set(tickers + [BENCHMARK, VIX_TICKER] + DEFENSIVE_TICKERS))
    start_date  = get_start_date()

    # 1) SEP 캐시에서 먼저 로드 (Sharadar 조정가, 생존 편향 없음)
    price_map = load_sep_prices(all_tickers, start_date)

    # 1b) SEP 캐시와 오늘 사이 갭이 있으면 yfinance로 보완
    if price_map:
        sep_last = max(s.index[-1] for s in price_map.values() if len(s) > 0)
        today    = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
        if sep_last < today - pd.Timedelta(days=1):
            gap_start = (sep_last + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            gap_tickers = list(price_map.keys())
            print(f"[GAP FILL] Fetching {gap_start} ~ today for {len(gap_tickers)} tickers via yfinance...")
            _fill_price_gap(price_map, gap_tickers, gap_start)
            after_last = max(s.index[-1] for s in price_map.values() if len(s) > 0)
            print(f"[GAP FILL] Updated last date: {after_last.date()}")

    # 2) SEP에 없는 종목 (^VIX 등) 은 yfinance fallback
    missing = [t for t in all_tickers if t not in price_map]
    if missing:
        print(f"Downloading {len(missing)} tickers via yfinance (not in SEP cache)...")
        df = yf.download(
            tickers=missing,
            start=start_date,
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            threads=True,
        )

        if isinstance(df.columns, pd.MultiIndex):
            level0 = set(df.columns.get_level_values(0))
            for t in missing:
                try:
                    if t not in level0:
                        continue
                    if "Close" not in df[t].columns:
                        continue
                    close = df[t]["Close"].dropna()
                    if len(close) >= MIN_HISTORY:
                        price_map[t] = close
                except Exception:
                    continue
        elif "Close" in df.columns and len(missing) == 1:
            close = df["Close"].dropna()
            if len(close) >= MIN_HISTORY:
                price_map[missing[0]] = close

    print(f"Total price series: {len(price_map)}")
    return price_map


def _cache_file_stamp(path: Path) -> str:
    if not path.exists():
        return f"missing:{path.name}"
    stat = path.stat()
    return f"{path.name}:{int(stat.st_mtime)}:{stat.st_size}"


def get_cache_signature() -> str:
    parts = [
        f"backtest_years={BACKTEST_YEARS}",
        f"start_date={get_start_date()}",
        f"benchmark={BENCHMARK}",
        f"vix={VIX_TICKER}",
        f"min_history={MIN_HISTORY}",
        f"vol_floor={VOL_WEIGHT_FLOOR}",
        _cache_file_stamp(ROOT / "build_score_snapshot.py"),
    ]
    for p in CURRENT_UNIVERSE_FILES + MEMBERSHIP_EVENTS_FILES:
        parts.append(_cache_file_stamp(p))
    payload = "|".join(parts)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()[:16]


def ensure_cache_dirs() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PRICE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _series_dict_to_frame(price_map: dict[str, pd.Series]) -> pd.DataFrame:
    if not price_map:
        return pd.DataFrame()
    df = pd.concat(price_map, axis=1)
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def _frame_to_series_dict(df: pd.DataFrame) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}
    if df is None or df.empty:
        return out
    if isinstance(df.columns, pd.MultiIndex):
        for t in df.columns.get_level_values(0).unique():
            s = df[t].iloc[:, 0] if isinstance(df[t], pd.DataFrame) else df[t]
            s = pd.Series(s).dropna()
            s.index = pd.to_datetime(s.index)
            out[str(t)] = s
    else:
        for t in df.columns:
            s = pd.Series(df[t]).dropna()
            s.index = pd.to_datetime(s.index)
            out[str(t)] = s
    return out


def get_price_cache_path(signature: str) -> Path:
    ensure_cache_dirs()
    return PRICE_CACHE_DIR / f"prices_{signature}.pkl"


def load_price_cache(expected_signature: str) -> dict[str, pd.Series] | None:
    cache_path = get_price_cache_path(expected_signature)
    if not cache_path.exists():
        return None
    try:
        payload = pd.read_pickle(cache_path)
        if not isinstance(payload, dict):
            return None
        if payload.get("signature") != expected_signature:
            return None
        frame = payload.get("frame")
        return _frame_to_series_dict(frame)
    except Exception as e:
        print(f"[WARN] Failed to load price cache: {e}")
        return None


def save_price_cache(price_map: dict[str, pd.Series], signature: str) -> None:
    cache_path = get_price_cache_path(signature)
    payload = {
        "signature": signature,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "frame": _series_dict_to_frame(price_map),
    }
    pd.to_pickle(payload, cache_path)


def load_or_download_prices(tickers: list[str], use_cache: bool = True) -> dict[str, pd.Series]:
    signature = get_cache_signature()
    cache_path = get_price_cache_path(signature)

    if use_cache:
        cached = load_price_cache(signature)
        if cached:
            print(f"[CACHE HIT] price_map <- {cache_path}")
            return cached

    price_map = download_prices_raw(tickers)

    if use_cache:
        save_price_cache(price_map, signature)
        print(f"[CACHE SAVE] price_map -> {cache_path}")

    return price_map


def get_snapshot_cache_path(date: pd.Timestamp, signature: str | None = None) -> Path:
    ensure_cache_dirs()
    sig = signature or get_cache_signature()
    return SNAPSHOT_CACHE_DIR / f"snapshot_{str(pd.Timestamp(date).date())}_{sig}.pkl"


def build_rebalance_snapshot(
    price_map: dict[str, pd.Series],
    date: pd.Timestamp,
    current_universe: dict[str, dict[str, Any]],
    membership_events: list[dict[str, Any]],
) -> dict[str, Any]:
    sliced = slice_price_map(price_map, date)
    membership = reconstruct_membership_as_of(
        as_of=date,
        current_universe=current_universe,
        events=membership_events,
    )
    eligible_prices = filter_price_map_by_membership(sliced, membership)
    if BENCHMARK in sliced:
        eligible_prices[BENCHMARK] = sliced[BENCHMARK]

    by_ticker_for_date = {
        t: current_universe.get(
            t,
            {
                "ticker": t,
                "sector": "Unknown",
                "index_flags": ["sp500", "sp400", "sp600"],
            },
        )
        for t in eligible_prices.keys()
    }

    metrics = build_metrics_for_group(
        list(eligible_prices.keys()),
        by_ticker_for_date,
        eligible_prices,
    )
    scored = score_group(
        metrics,
        quality_score_map=None,
    )

    spy_historical_series = sliced.get(BENCHMARK)
    if spy_historical_series is not None and len(spy_historical_series) >= 20:
        spy_vol20 = annualized_volatility(spy_historical_series, 20)
        if spy_vol20 is None:
            spy_vol20 = VOL_WEIGHT_FLOOR
    else:
        spy_vol20 = VOL_WEIGHT_FLOOR
    dynamic_floor = max(VOL_WEIGHT_FLOOR, spy_vol20)

    return {
        "date": str(pd.Timestamp(date).date()),
        "signature": get_cache_signature(),
        "scored": scored,
        "dynamic_floor": float(dynamic_floor),
        "membership_count": len(membership),
        "eligible_count": len(eligible_prices),
    }


def get_rebalance_snapshot(
    price_map: dict[str, pd.Series],
    date: pd.Timestamp,
    current_universe: dict[str, dict[str, Any]],
    membership_events: list[dict[str, Any]],
    use_cache: bool = True,
) -> dict[str, Any]:
    signature = get_cache_signature()
    cache_path = get_snapshot_cache_path(date, signature)
    if use_cache and cache_path.exists():
        try:
            payload = pd.read_pickle(cache_path)
            if isinstance(payload, dict) and payload.get("signature") == signature:
                return payload
        except Exception as e:
            print(f"[WARN] Failed to load snapshot cache for {date.date()}: {e}")

    payload = build_rebalance_snapshot(price_map, date, current_universe, membership_events)
    if use_cache:
        try:
            pd.to_pickle(payload, cache_path)
        except Exception as e:
            print(f"[WARN] Failed to save snapshot cache for {date.date()}: {e}")
    return payload


def precompute_rebalance_snapshots(
    price_map: dict[str, pd.Series],
    rebalance_dates: list[pd.Timestamp],
    current_universe: dict[str, dict[str, Any]],
    membership_events: list[dict[str, Any]],
    use_cache: bool = True,
) -> None:
    total = len(rebalance_dates)
    max_workers = min(os.cpu_count() or 4, 8)

    def _compute(dt: pd.Timestamp) -> pd.Timestamp:
        get_rebalance_snapshot(price_map, dt, current_universe, membership_events, use_cache=use_cache)
        return dt

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_compute, dt): dt for dt in rebalance_dates}
        for future in as_completed(futures):
            completed += 1
            dt = future.result()
            if completed == 1 or completed == total or completed % 12 == 0:
                print(f"[SNAPSHOT CACHE] {completed}/{total} ready ({dt.date()})")


def monthly_rebalance_dates(dates: pd.Index) -> list[pd.Timestamp]:
    out: list[pd.Timestamp] = []
    last = None
    for d in dates:
        key = d.strftime("%Y-%m")
        if key != last:
            out.append(d)
            last = key
    return out


def weekly_rebalance_dates(dates: pd.Index) -> list[pd.Timestamp]:
    out: list[pd.Timestamp] = []
    last = None
    for d in dates:
        y, w, _ = d.isocalendar()
        key = f"{y}-{w}"
        if key != last:
            out.append(d)
            last = key
    return out


def slice_price_map(
    price_map: dict[str, pd.Series],
    date: pd.Timestamp,
) -> dict[str, pd.Series]:
    out: dict[str, pd.Series] = {}

    for t, s in price_map.items():
        s2 = s[s.index <= date]
        if len(s2) > 60:
            out[t] = s2
            
    return out


def filter_price_map_by_membership(
    sliced_price_map: dict[str, pd.Series],
    membership: set[str],
) -> dict[str, pd.Series]:
    return {t: s for t, s in sliced_price_map.items() if t in membership}


def passes_absolute_momentum(row) -> bool:
    if row.ret63d is None or row.ret252d is None:
        return False
    return row.ret63d > ABS_MOM_63D_MIN and row.ret252d > ABS_MOM_252D_MIN


# [NEW] 버퍼 랭킹(Hysteresis)이 반영된 포트폴리오 픽 로직
def pick_portfolio(rows, current_holdings: set[str], dynamic_floor: float | None = None) -> list:
    ranked = [r for r in rows if r.final_score_100 is not None]
    ranked.sort(key=lambda x: x.final_score_100, reverse=True)

    filtered = [r for r in ranked if passes_absolute_momentum(r)]
    if not filtered:
        filtered = ranked

    # Altman Z-score distress filter (experimental)
    if USE_ZSCORE_FILTER and ZSCORE_DISTRESS_SET:
        non_distress = [r for r in filtered if r.ticker not in ZSCORE_DISTRESS_SET]
        if non_distress:
            filtered = non_distress

    if SELECTION == "BUY":
        selected = [r for r in filtered if r.signal == "BUY"]
        if not selected:
            selected = filtered
    else:
        selected = filtered

    out = []
    sector_counts: dict[str, int] = {}

    def add_to_out(r) -> bool:
        sector = r.sector or "Unknown"
        if SECTOR_MAX_NAMES <= 0 or sector_counts.get(sector, 0) < SECTOR_MAX_NAMES:
            out.append(r)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            return True
        return False

    # 1순위: 기존에 보유하고 있던 종목 중, 순위가 HOLD_BUFFER_N(25위) 이내로 선방 중인 종목을 우선 배정
    held_eligible = []
    for i, r in enumerate(selected):
        rank = i + 1
        if r.ticker in current_holdings and rank <= HOLD_BUFFER_N:
            held_eligible.append(r)
            
    for r in held_eligible:
        if len(out) < TOP_N:
            add_to_out(r)

    # 2순위: 남은 빈자리(TOP_N - 1순위 갯수)를 최상위 랭킹 순서대로 신규 편입
    for r in selected:
        if len(out) >= TOP_N:
            break
        # 이미 1순위에서 추가된 종목은 건너뜀
        if any(existing.ticker == r.ticker for existing in out):
            continue
        add_to_out(r)

    selected_final = out

    if not selected_final:
        return []

    selected_final = compute_portfolio_weights(
        selected_final,
        method=PORTFOLIO_WEIGHT_METHOD,
        alpha_score=WEIGHT_ALPHA_SCORE,
        min_w=MIN_WEIGHT,
        max_w=MAX_WEIGHT,
        vol_fallback=VOL_FALLBACK,
        vol_floor=VOL_WEIGHT_FLOOR,
        dynamic_vol_floor=dynamic_floor,
    )
    return selected_final


def holdings_to_weight_map(selected_rows) -> dict[str, float]:
    if not selected_rows:
        return {}

    weights: dict[str, float] = {}
    for r in selected_rows:
        w = float(r.portfolio_weight or 0.0)
        if w > 0:
            weights[r.ticker] = w

    total = sum(weights.values())
    if total <= 0:
        return {}

    return {k: v / total for k, v in weights.items()}


def normalize_weight_map(weights: dict[str, float]) -> dict[str, float]:
    clean = {k: float(v) for k, v in weights.items() if v is not None and float(v) > 0}
    total = sum(clean.values())
    if total <= 0:
        return {}
    return {k: v / total for k, v in clean.items()}


def compute_daily_return(
    price_map: dict[str, pd.Series],
    holdings: dict[str, float],
    date: pd.Timestamp,
) -> float:
    if not holdings:
        return 0.0

    weighted_ret = 0.0
    used_weight = 0.0

    for t, w in holdings.items():
        s = price_map.get(t)
        if s is None:
            continue
        if date not in s.index:
            continue

        idx = s.index.get_loc(date)
        if idx == 0:
            continue

        r = float(s.iloc[idx] / s.iloc[idx - 1] - 1.0)
        weighted_ret += w * r
        used_weight += w

    if used_weight <= 0:
        return 0.0

    return float(weighted_ret / used_weight)


def calc_metrics(df: pd.DataFrame) -> dict[str, float]:
    equity = float(df["equity"].iloc[-1])

    total_return = equity - 1.0
    years = len(df) / 252

    cagr = equity ** (1 / years) - 1 if years > 0 else 0.0
    vol = float(df["daily_return"].std() * np.sqrt(252))
    sharpe = cagr / vol if vol > 0 else 0.0
    # MDD 계산 Step 3: drawdown 컬럼(매 거래일의 고점 대비 낙폭)에서 최솟값을 꺼낸다.
    # 가장 작은 값 = 가장 큰 낙폭 = Max Drawdown
    # 예) df["drawdown"].min() == -0.366  →  MDD -36.6%
    # 주의: 부호가 음수이므로 절댓값이 클수록 더 큰 손실을 의미한다.
    mdd = float(df["drawdown"].min())

    return {
        "total_return": float(total_return),
        "cagr": float(cagr),
        "volatility": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(mdd),
    }


def build_benchmark_df(benchmark_series: pd.Series) -> pd.DataFrame:
    bench_equity = benchmark_series / benchmark_series.iloc[0]
    bench_returns = benchmark_series.pct_change().fillna(0.0)

    bench_df = pd.DataFrame(
        {
            "date": [str(d.date()) for d in benchmark_series.index],
            "equity": bench_equity.values,
            "daily_return": bench_returns.values,
        }
    )

    bench_df["drawdown"] = bench_df["equity"] / bench_df["equity"].cummax() - 1.0
    return bench_df


def compute_subperiod_metrics(df: pd.DataFrame, windows_years: list[int]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if df.empty:
        return out

    for years in windows_years:
        n = years * 252
        if len(df) < max(60, n // 2):
            continue

        sub = df.tail(n).copy() if len(df) > n else df.copy()

        start_equity = float(sub["equity"].iloc[0])
        if start_equity <= 0:
            continue

        sub["equity"] = sub["equity"] / start_equity
        # 서브기간(3Y/5Y/10Y)용 drawdown: 해당 구간 시작점을 1.0으로 리베이스한 뒤
        # cummax()로 구간 내 최고점을 추적하고, 현재값 / 최고점 - 1 로 낙폭을 계산한다.
        sub["drawdown"] = sub["equity"] / sub["equity"].cummax() - 1.0

        metrics = calc_metrics(sub)
        out.append(
            {
                "label": f"{years}y",
                "metrics": metrics,
            }
        )

    return out


def trailing_return(series: pd.Series, date: pd.Timestamp, lookback: int) -> float | None:
    hist = series[series.index <= date]
    if len(hist) <= lookback:
        return None

    last = float(hist.iloc[-1])
    prev = float(hist.iloc[-(lookback + 1)])
    if prev == 0:
        return None

    return last / prev - 1.0


def rolling_ma(series: pd.Series, date: pd.Timestamp, window: int) -> float | None:
    hist = series[series.index <= date]
    if len(hist) < window:
        return None

    ma = hist.rolling(window).mean().iloc[-1]
    if pd.isna(ma):
        return None

    return float(ma)


def precompute_regime_signals(
    benchmark_series: pd.Series,
    buffer: float = REGIME_BUFFER,
) -> pd.DataFrame:
    """MA200·모멘텀63을 전체 기간에 대해 한 번만 계산해 반환한다.
    이후 regime_candidate 호출 시 날짜 조회(O(1))로 대체된다."""
    ma200 = benchmark_series.rolling(REGIME_MA_WINDOW).mean()
    mom63 = benchmark_series.pct_change(REGIME_MOM_WINDOW)
    return pd.DataFrame(
        {
            "price": benchmark_series,
            "ma200": ma200,
            "mom63": mom63,
            "upper_band": ma200 * (1.0 + buffer),
            "lower_band": ma200 * (1.0 - buffer),
        }
    )


def regime_candidate(
    benchmark_series: pd.Series,
    date: pd.Timestamp,
    *,
    buffer: float = REGIME_BUFFER,
    precomputed_signals: pd.DataFrame | None = None,
) -> dict[str, Any]:
    if precomputed_signals is not None and date in precomputed_signals.index:
        row = precomputed_signals.loc[date]
        ma200 = None if pd.isna(row["ma200"]) else float(row["ma200"])
        mom63 = None if pd.isna(row["mom63"]) else float(row["mom63"])
        last = float(row["price"])
        upper_pre = None if ma200 is None else float(row["upper_band"])
        lower_pre = None if ma200 is None else float(row["lower_band"])
    else:
        ma200 = rolling_ma(benchmark_series, date, REGIME_MA_WINDOW)
        mom63 = trailing_return(benchmark_series, date, REGIME_MOM_WINDOW)
        hist = benchmark_series[benchmark_series.index <= date]
        last = float(hist.iloc[-1]) if not hist.empty else 0.0
        upper_pre = None
        lower_pre = None

    if ma200 is None or mom63 is None:
        return {
            "trend_ok": True,
            "momentum_ok": True,
            "candidate_bucket": "risk_on",
            "last_price": last,
            "ma200": ma200,
            "mom63": mom63,
        }

    upper = upper_pre if upper_pre is not None else ma200 * (1.0 + buffer)
    lower = lower_pre if lower_pre is not None else ma200 * (1.0 - buffer)

    trend_up = last >= upper
    trend_down = last <= lower
    momentum_ok = mom63 >= 0.0

    if trend_up and momentum_ok:
        candidate_bucket = "risk_on"
    elif trend_down and (not momentum_ok):
        candidate_bucket = "risk_off"
    else:
        candidate_bucket = "mid"

    return {
        "trend_ok": bool(last >= ma200),
        "momentum_ok": bool(momentum_ok),
        "candidate_bucket": candidate_bucket,
        "last_price": last,
        "ma200": float(ma200),
        "mom63": float(mom63),
        "upper_band": float(upper),
        "lower_band": float(lower),
    }


def confirmed_regime_bucket(
    benchmark_series: pd.Series,
    date: pd.Timestamp,
    prev_bucket: str,
    *,
    confirm_days: int = REGIME_CONFIRM_DAYS,
    buffer: float = REGIME_BUFFER,
    precomputed_signals: pd.DataFrame | None = None,
) -> dict[str, Any]:
    hist_dates = benchmark_series.index[benchmark_series.index <= date]
    if len(hist_dates) == 0:
        return {
            "regime_bucket": prev_bucket,
            "trend_ok": True,
            "momentum_ok": True,
            "candidate_bucket": prev_bucket,
            "confirmed": False,
        }

    current_meta = regime_candidate(benchmark_series, date, buffer=buffer, precomputed_signals=precomputed_signals)
    candidate_bucket = current_meta["candidate_bucket"]

    if candidate_bucket in {"risk_on", "risk_off"} and confirm_days > 1:
        recent_dates = hist_dates[-confirm_days:]
        if len(recent_dates) < confirm_days:
            regime_bucket = prev_bucket
            confirmed = False
        else:
            recent_candidates = [
                regime_candidate(benchmark_series, d, buffer=buffer, precomputed_signals=precomputed_signals)["candidate_bucket"]
                for d in recent_dates
            ]
            if all(x == candidate_bucket for x in recent_candidates):
                regime_bucket = candidate_bucket
                confirmed = True
            else:
                regime_bucket = prev_bucket
                confirmed = False
    elif candidate_bucket in {"risk_on", "risk_off"}:
        regime_bucket = candidate_bucket
        confirmed = True
    else:
        regime_bucket = prev_bucket
        confirmed = False

    return {
        **current_meta,
        "regime_bucket": regime_bucket,
        "confirmed": confirmed,
    }


def regime_exposure_with_vix(
    benchmark_series: pd.Series,
    vix_series: pd.Series | None,
    date: pd.Timestamp,
    prev_bucket: str,
    precomputed_signals: pd.DataFrame | None = None,
) -> tuple[float, dict[str, Any]]:

    meta = confirmed_regime_bucket(
        benchmark_series=benchmark_series,
        date=date,
        prev_bucket=prev_bucket,
        confirm_days=REGIME_CONFIRM_DAYS,
        buffer=REGIME_BUFFER,
        precomputed_signals=precomputed_signals,
    )

    base_bucket = meta["regime_bucket"]
    final_bucket = base_bucket
    is_vix_crash = False

    if vix_series is not None:
        hist_vix = vix_series[vix_series.index <= date]
        if not hist_vix.empty:
            last_vix = float(hist_vix.iloc[-1])
            if not meta["trend_ok"] and last_vix > VIX_CRASH_THRESHOLD:
                final_bucket = "risk_off"
                is_vix_crash = True

    if final_bucket == "risk_on":
        exposure = RISK_ON_EXPOSURE
    elif final_bucket == "risk_off":
        exposure = RISK_OFF_EXPOSURE
    else:
        exposure = MID_EXPOSURE

    meta.update({
        "regime_bucket": final_bucket,
        "is_vix_crash": is_vix_crash
    })

    return exposure, meta


def build_defensive_sleeve(total_defensive_weight: float) -> dict[str, float]:
    if total_defensive_weight <= 0:
        return {}

    base = normalize_weight_map(RISK_OFF_DEFENSIVE_WEIGHTS)
    if not base:
        return {}

    return {ticker: weight * total_defensive_weight for ticker, weight in base.items()}


def build_total_holdings(
    stock_holdings: dict[str, float],
    stock_exposure: float,
) -> tuple[dict[str, float], dict[str, float]]:
    stock_exposure = float(max(0.0, min(1.0, stock_exposure)))
    defensive_weight = 1.0 - stock_exposure

    total: dict[str, float] = {}

    if stock_holdings and stock_exposure > 0:
        norm_stock = normalize_weight_map(stock_holdings)
        for ticker, weight in norm_stock.items():
            total[ticker] = total.get(ticker, 0.0) + weight * stock_exposure

    defensive = build_defensive_sleeve(defensive_weight)
    for ticker, weight in defensive.items():
        total[ticker] = total.get(ticker, 0.0) + weight

    return normalize_weight_map(total), defensive


def weight_maps_equal(
    a: dict[str, float],
    b: dict[str, float],
    tol: float = 1e-10,
) -> bool:
    keys = set(a.keys()) | set(b.keys())
    for k in keys:
        if abs(float(a.get(k, 0.0)) - float(b.get(k, 0.0))) > tol:
            return False
    return True


def run_backtest() -> None:
    current_universe = load_current_universe()
    membership_events = load_membership_events()

    tickers = list(current_universe.keys())
    price_map = load_or_download_prices(tickers, use_cache=CACHE_ENABLED)

    if BENCHMARK not in price_map or VIX_TICKER not in price_map:
        raise RuntimeError("Benchmark or VIX data missing")

    benchmark_series = price_map[BENCHMARK]
    vix_series = price_map.get(VIX_TICKER)
    trading_dates = benchmark_series.index

    if REBALANCE == "monthly":
        stock_rebalance_dates = set(monthly_rebalance_dates(trading_dates))
    else:
        stock_rebalance_dates = set(weekly_rebalance_dates(trading_dates))

    if EXPOSURE_REBALANCE == "daily":
        exposure_rebalance_dates = set(trading_dates)
    elif EXPOSURE_REBALANCE == "weekly":
        exposure_rebalance_dates = set(weekly_rebalance_dates(trading_dates))
    else:
        exposure_rebalance_dates = set(monthly_rebalance_dates(trading_dates))

    precompute_rebalance_snapshots(price_map, sorted(stock_rebalance_dates), current_universe, membership_events, use_cache=CACHE_ENABLED)

    # [최적화] MA200·모멘텀63을 전체 기간에 대해 한 번만 벡터 계산 → 매일 재계산 O(n²) 제거
    print("[OPTIM] Pre-computing regime signals (MA200 + Mom63)...")
    regime_signals = precompute_regime_signals(benchmark_series)

    current_stock_holdings: dict[str, float] = {}
    current_stock_holdings_list: list[str] = []
    current_stock_exposure: float = RISK_ON_EXPOSURE
    current_defensive_holdings: dict[str, float] = {}
    current_total_holdings: dict[str, float] = {}

    pending_stock_holdings: dict[str, float] | None = None
    pending_stock_holdings_list: list[str] | None = None
    pending_stock_exposure: float | None = None
    pending_total_holdings: dict[str, float] | None = None
    pending_defensive_holdings: dict[str, float] | None = None
    pending_regime_meta: dict[str, Any] | None = None
    pending_cost: bool = False

    last_regime_meta: dict[str, Any] = {
        "trend_ok": True,
        "momentum_ok": True,
        "regime_bucket": "risk_on",
        "candidate_bucket": "risk_on",
        "confirmed": True,
    }

    equity = 1.0
    high = 1.0
    history: list[dict] = []

    for date in trading_dates:
        if pending_total_holdings is not None:
            if pending_cost:
                equity *= (1 - TRANSACTION_COST)

            current_stock_holdings = pending_stock_holdings or {}
            current_stock_holdings_list = pending_stock_holdings_list or []
            current_stock_exposure = float(
                pending_stock_exposure if pending_stock_exposure is not None else current_stock_exposure
            )
            current_total_holdings = pending_total_holdings
            current_defensive_holdings = pending_defensive_holdings or {}

            if pending_regime_meta is not None:
                last_regime_meta = pending_regime_meta

            pending_stock_holdings = None
            pending_stock_holdings_list = None
            pending_stock_exposure = None
            pending_total_holdings = None
            pending_defensive_holdings = None
            pending_regime_meta = None
            pending_cost = False

        daily_ret = compute_daily_return(price_map, current_total_holdings, date)
        equity *= (1 + daily_ret)
        # MDD 계산 Step 1: 오늘까지의 최고 자산 가치(high)를 갱신한다.
        # high 는 백테스트 시작(equity=1.0)부터 현재까지 기록한 최고점이다.
        high = max(high, equity)
        # MDD 계산 Step 2: 현재 자산이 최고점 대비 얼마나 내려왔는지 비율로 계산한다.
        # dd = (현재값 / 최고점) - 1  →  0 이하의 음수, 예) -0.20 = 최고점에서 20% 하락
        # 이 값들 중 전체 기간 최솟값(가장 큰 하락)이 최종 MDD가 된다. (calc_metrics 참고)
        dd = equity / high - 1.0

        history.append(
            {
                "date": str(date.date()),
                "equity": float(equity),
                "daily_return": float(daily_ret),
                "drawdown": float(dd),
                "stock_exposure": float(current_stock_exposure),
                "defensive_exposure": float(1.0 - current_stock_exposure),
                "trend_ok": bool(last_regime_meta.get("trend_ok", True)),
                "momentum_ok": bool(last_regime_meta.get("momentum_ok", True)),
                "candidate_bucket": str(last_regime_meta.get("candidate_bucket", "risk_on")),
                "regime_bucket": str(last_regime_meta.get("regime_bucket", "risk_on")),
                "regime_confirmed": bool(last_regime_meta.get("confirmed", True)),
                "vix_crash_active": bool(last_regime_meta.get("is_vix_crash", False)), 
                "holdings_count": len(current_stock_holdings),
                "holdings": current_stock_holdings_list,
                "stock_weights": {k: round(v, 6) for k, v in current_stock_holdings.items()},
                "defensive_weights": {k: round(v, 6) for k, v in current_defensive_holdings.items()},
                "total_weights": {k: round(v, 6) for k, v in current_total_holdings.items()},
            }
        )

        target_stock_holdings = current_stock_holdings
        target_stock_holdings_list = current_stock_holdings_list
        target_stock_exposure = current_stock_exposure
        target_regime_meta = last_regime_meta

        if date in stock_rebalance_dates:
            # point-in-time Z-score 업데이트 (look-ahead bias 없는 방식)
            if USE_ZSCORE_FILTER and ZSCORE_DATE_LOOKUP is not None:
                import run_backtest_regime as _self
                _self.ZSCORE_DISTRESS_SET = ZSCORE_DATE_LOOKUP(date)

            snapshot = get_rebalance_snapshot(
                price_map=price_map,
                date=date,
                current_universe=current_universe,
                membership_events=membership_events,
                use_cache=CACHE_ENABLED,
            )
            scored = snapshot["scored"]
            dynamic_floor = float(snapshot["dynamic_floor"])

            # [수정] pick_portfolio에 현재 보유 중인 종목 세트를 넘겨 버퍼 로직 작동
            selected_rows = pick_portfolio(
                scored, 
                current_holdings=set(current_stock_holdings.keys()), 
                dynamic_floor=dynamic_floor
            )
            
            target_stock_holdings = holdings_to_weight_map(selected_rows)
            target_stock_holdings_list = sorted(target_stock_holdings.keys())

        if date in exposure_rebalance_dates:
            prev_bucket = str(last_regime_meta.get("regime_bucket", "risk_on"))
            target_stock_exposure, target_regime_meta = regime_exposure_with_vix(
                benchmark_series=benchmark_series,
                vix_series=vix_series,
                date=date,
                prev_bucket=prev_bucket,
                precomputed_signals=regime_signals,
            )

        target_total_holdings, target_defensive_holdings = build_total_holdings(
            stock_holdings=target_stock_holdings,
            stock_exposure=target_stock_exposure,
        )

        will_change = not weight_maps_equal(target_total_holdings, current_total_holdings)

        pending_stock_holdings = target_stock_holdings
        pending_stock_holdings_list = target_stock_holdings_list
        pending_stock_exposure = target_stock_exposure
        pending_total_holdings = target_total_holdings
        pending_defensive_holdings = target_defensive_holdings
        pending_regime_meta = target_regime_meta
        pending_cost = will_change

    df = pd.DataFrame(history)
    strategy_metrics = calc_metrics(df)
    subperiods = compute_subperiod_metrics(df, windows_years=[3, 5, 10])

    bench_df = build_benchmark_df(benchmark_series)
    benchmark_metrics = calc_metrics(bench_df)

    curve_df = df[
        ["date", "equity", "stock_exposure", "defensive_exposure", "regime_bucket"]
    ].copy()
    curve_df = curve_df.rename(columns={"equity": "strategy"})

    bench_curve = bench_df[["date", "equity"]].copy()
    bench_curve = bench_curve.rename(columns={"equity": "benchmark"})

    merged_curve = pd.merge(curve_df, bench_curve, on="date", how="inner")
    equity_curve = merged_curve.to_dict(orient="records")

    result = {
        "strategy": {
            "rebalance": REBALANCE,
            "selection": SELECTION,
            "top_n": TOP_N,
            "hold_buffer_n": HOLD_BUFFER_N, # [NEW] JSON 출력에 기록
            "transaction_cost": TRANSACTION_COST,
            "period_years": BACKTEST_YEARS,
            "execution_lag_days": 1,
            "universe_method": "point_in_time_sp1500_membership_from_wikipedia_events",
            "portfolio_construction": {
                "method": PORTFOLIO_WEIGHT_METHOD,
                "score_alpha": WEIGHT_ALPHA_SCORE,
                "min_weight": MIN_WEIGHT,
                "max_weight": MAX_WEIGHT,
                "vol_fallback": VOL_FALLBACK,
                "vol_floor": VOL_WEIGHT_FLOOR,
                "dynamic_vol_targeting": True, 
                "absolute_momentum_63d_min": ABS_MOM_63D_MIN,
                "absolute_momentum_252d_min": ABS_MOM_252D_MIN,
                "sector_max_names": SECTOR_MAX_NAMES,
                "formula": "weight_i ∝ (score_i ^ alpha) / max(vol20_i, max(vol_floor, spy_vol20))",
            },
            "regime_filter": {
                "benchmark": BENCHMARK,
                "ma_window": REGIME_MA_WINDOW,
                "momentum_window": REGIME_MOM_WINDOW,
                "buffer": REGIME_BUFFER,
                "confirm_days": REGIME_CONFIRM_DAYS,
                "stock_rebalance": REBALANCE,
                "exposure_rebalance": EXPOSURE_REBALANCE,
                "risk_on_exposure": RISK_ON_EXPOSURE,
                "mid_exposure": MID_EXPOSURE,
                "risk_off_exposure": RISK_OFF_EXPOSURE,
                "defensive_tickers": DEFENSIVE_TICKERS,
                "summary": (
                    f"Exposure is updated on a {EXPOSURE_REBALANCE} basis using a buffered "
                    f"3-state regime model with {REGIME_MA_WINDOW}DMA, {REGIME_MOM_WINDOW}D momentum, "
                    f"{REGIME_BUFFER:.2%} hysteresis band, and {REGIME_CONFIRM_DAYS}-day confirmation. "
                    f"Risk-off sleeve is {', '.join(DEFENSIVE_TICKERS)}."
                ),
                "vix_crash_filter": f"Active ONLY if Price < MA200 AND VIX > {VIX_CRASH_THRESHOLD}"
            },
        },
        "metrics": strategy_metrics,
        "subperiods": subperiods,
        "benchmark": {
            "ticker": BENCHMARK,
            "metrics": benchmark_metrics,
        },
        "notes": [
            "Universe is reconstructed point-in-time using current S&P1500 membership plus Wikipedia change events.",
            "Current universe combines S&P500, S&P400, and S&P600.",
            "Ranking uses historical slice through rebalance date close.",
            f"Buffer Ranking Applied: Buy top {TOP_N}, Hold existing if ranked <= {HOLD_BUFFER_N}.", # [NEW] 노트 추가
            "Geometric multiplicative scoring and RS (Relative Strength) vs SPY are applied to stock selection.", 
            "Dynamic Volatility Targeting is applied: minimum weight denominator floors at recent SPY 20D Volatility.", 
            f"Asymmetric Systemic Crash Filter: Triggers Risk-Off if Price < MA200 AND VIX > {VIX_CRASH_THRESHOLD}. Ignored in uptrends.",
            "Selected stock portfolio is applied with a 1-day execution lag.",
            "Exposure signal is applied with a 1-day execution lag.",
            "Transaction cost is applied when total portfolio weights change on execution day.",
            f"Stock selection is {REBALANCE}, while exposure adjustment is {EXPOSURE_REBALANCE}.",
            f"Absolute momentum filter requires 63D return > {ABS_MOM_63D_MIN} and 252D return > {ABS_MOM_252D_MIN}.",
            "Sector cap limits selected names to reduce concentration risk.",
            f"Regime filter uses a {REGIME_BUFFER:.2%} buffer around the moving average and "
            f"{REGIME_CONFIRM_DAYS}-day confirmation to reduce whipsaw.",
            f"Risk-off sleeve uses {', '.join(DEFENSIVE_TICKERS)} only.",
            "Quality is not yet applied in backtest to avoid look-ahead bias.",
        ],
    }

    result_path = DATA_DIR / "backtest_regime_result.json"
    curve_path = DATA_DIR / "equity_curve_regime.json"

    save_json(result_path, result)
    save_json(curve_path, equity_curve)

    print("\n===== BUFFERED / SMOOTHED REGIME RESULT =====\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print(f"\nSaved -> {result_path}")
    print(f"Saved -> {curve_path}")

    copy_to_public(result_path)
    copy_to_public(curve_path)

    # [수정 완료] 실전 매매 연동을 위한 백테스트 최종 보유 종목 저장 로직
    # 변수명을 current_stock_holdings 로 교정하였고, 위치를 함수 내부로 옮겼습니다.
    final_stocks = [t for t in current_stock_holdings.keys() if t not in ["TAIL", "DBMF"]]
    with open(DATA_DIR / "final_holdings.json", "w", encoding="utf-8") as f:
        json.dump(final_stocks, f)
    print(f"Saved -> {DATA_DIR / 'final_holdings.json'} (Live Execution Ready)")

    # 실전 추적용 — 마지막 리밸런싱 포트폴리오 가중치 저장
    live_portfolio_weights = {
        t: round(w, 6)
        for t, w in current_stock_holdings.items()
        if t not in ["TAIL", "DBMF"]
    }
    live_state = {
        "last_updated":       datetime.now(timezone.utc).isoformat(),
        "last_rebalance_date": str(trading_dates[-1].date()),
        "portfolio_weights":  live_portfolio_weights,
        "defensive_weights":  {k: round(v, 6) for k, v in current_defensive_holdings.items()},
        "stock_exposure":     float(current_stock_exposure),
        "regime_bucket":      str(last_regime_meta.get("regime_bucket", "risk_on")),
    }
    save_json(DATA_DIR / "live_state.json", live_state)
    print(f"Saved -> {DATA_DIR / 'live_state.json'} ({len(live_portfolio_weights)} holdings)")


if __name__ == "__main__":
    run_backtest()