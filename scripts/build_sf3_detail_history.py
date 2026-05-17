#!/usr/bin/env python3
"""
build_sf3_detail_history.py — SF3 투자자별 상세 기관 신호 빌더

기존 sf3_history.pkl은 총 보유금액(aggregate)만 저장.
이 스크립트는 investorname 수준으로 다운로드해 분기별로 아래 3개 지표를 계산한다:

  n_holders      : 보유 기관 수
  new_holders    : 신규 진입 기관 수 (직전 분기 없었다가 이번 분기 진입)
  hhi            : 기관 보유 집중도 HHI = Σ(v_i/total)²

출력: data/sf3_detail_history.pkl
  {"lookup": {ticker: {"quarters": [...], "n_holders": [...], "new_holders": [...], "hhi": [...]}}}

실행:
  python scripts/build_sf3_detail_history.py
"""
from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import nasdaqdatalink
import pandas as pd
import numpy as np

nasdaqdatalink.ApiConfig.api_key = os.environ.get("NASDAQ_DATA_LINK_KEY", "NHr5446JR6sysBKtTBp1")

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SP500_FILE = DATA_DIR / "sp500_current_wiki.json"
SP400_FILE = DATA_DIR / "sp400_current_wiki.json"
SP600_FILE = DATA_DIR / "sp600_current_wiki.json"
OUT_FILE   = DATA_DIR / "sf3_detail_history.pkl"

START_DATE  = "2013-01-01"
CHUNK_SIZE  = 3   # investorname 포함 시 행 수가 많아 청크 더 작게
MAX_WORKERS = 8


def load_json(path: Path, default=None):
    if not path.exists():
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def collect_tickers() -> list[str]:
    tickers: set[str] = set()
    for path in [SP500_FILE, SP400_FILE, SP600_FILE]:
        data  = load_json(path, {})
        items = data.get("items", []) if isinstance(data, dict) else data
        for item in (items or []):
            t = item if isinstance(item, str) else item.get("ticker", "")
            t = str(t).strip().upper().replace(".", "-")
            if t:
                tickers.add(t)
    return sorted(tickers)


def fetch_sf3_detail_chunk(tickers: list[str]) -> pd.DataFrame:
    """투자자별 SF3 다운로드 (investorname 포함)"""
    for use_filter in [True, False]:
        try:
            kwargs = dict(
                ticker=tickers,
                securitytype="SHR",
                qopts={"columns": ["ticker", "calendardate", "investorname", "value"]},
                paginate=True,
            )
            if use_filter:
                kwargs["calendardate"] = {"gte": START_DATE}
            df = nasdaqdatalink.get_table("SHARADAR/SF3", **kwargs)
            if df is None or df.empty:
                return pd.DataFrame()
            df["calendardate"] = pd.to_datetime(df["calendardate"])
            df = df[df["calendardate"] >= pd.Timestamp(START_DATE)]
            df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)
            df["ticker"] = df["ticker"].str.upper().str.replace(".", "-", regex=False)
            return df[["ticker", "calendardate", "investorname", "value"]]
        except Exception as e:
            err = str(e)
            if use_filter and ("filter" in err.lower() or "exceed" in err.lower()):
                continue
            print(f"  [WARN] SF3 detail chunk error: {err[:120]}")
            return pd.DataFrame()
    return pd.DataFrame()


def compute_detail_signals(df: pd.DataFrame) -> dict[str, dict]:
    """
    ticker별 분기별 신호 계산:
      n_holders   : 보유 기관 수
      new_holders : 신규 진입 기관 수 (직전 분기 대비)
      hhi         : Σ(v_i/total)² 집중도
    """
    lookup: dict[str, dict] = {}

    for ticker, tdf in df.groupby("ticker"):
        tdf = tdf.sort_values("calendardate")
        quarters_seen = sorted(tdf["calendardate"].unique())

        quarters_str  = []
        n_holders_lst = []
        new_holders_lst = []
        hhi_lst       = []

        prev_investors: set[str] = set()

        for q in quarters_seen:
            qdf = tdf[tdf["calendardate"] == q]
            investors = set(qdf["investorname"].dropna().str.strip())
            total_val = float(qdf["value"].sum())

            n = len(investors)
            new = len(investors - prev_investors) if prev_investors else None

            # HHI
            if total_val > 0 and n > 0:
                shares = qdf.groupby("investorname")["value"].sum()
                hhi = float((shares / total_val).pow(2).sum())
            else:
                hhi = None

            quarters_str.append(str(q.date()))
            n_holders_lst.append(n)
            new_holders_lst.append(new)
            hhi_lst.append(hhi)

            prev_investors = investors

        lookup[str(ticker)] = {
            "quarters":    quarters_str,
            "n_holders":   n_holders_lst,
            "new_holders": new_holders_lst,
            "hhi":         hhi_lst,
        }

    return lookup


def main():
    tickers  = collect_tickers()
    total    = len(tickers)
    chunks   = [tickers[i : i + CHUNK_SIZE] for i in range(0, total, CHUNK_SIZE)]
    n_chunks = len(chunks)
    print(f"[INFO] SF3 detail history: {total} tickers (investorname 포함)")
    print(f"[INFO] {n_chunks} chunks × {CHUNK_SIZE} tickers, {MAX_WORKERS} workers")

    all_frames: list[pd.DataFrame] = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_sf3_detail_chunk, c): c for c in chunks}
        for future in as_completed(futures):
            completed += 1
            result = future.result()
            if not result.empty:
                all_frames.append(result)
            if completed % 50 == 0 or completed == n_chunks:
                rows = sum(len(f) for f in all_frames)
                print(f"  {completed}/{n_chunks} chunks done, {rows:,} rows")

    if not all_frames:
        print("[ERROR] No SF3 detail data downloaded")
        return

    combined = pd.concat(all_frames, ignore_index=True)
    print(f"\n[INFO] Total rows: {len(combined):,}")

    print("[INFO] Computing per-quarter signals (n_holders, new_holders, hhi)...")
    lookup    = compute_detail_signals(combined)
    n_tickers = len(lookup)
    print(f"[INFO] Tickers in lookup: {n_tickers}")

    # 통계 출력
    sample = list(lookup.values())[:5]
    for rec in sample:
        q_last = rec["quarters"][-1] if rec["quarters"] else "-"
        n_last = rec["n_holders"][-1] if rec["n_holders"] else 0
        print(f"  sample: last_quarter={q_last}, n_holders={n_last}")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source":       "SHARADAR/SF3",
        "start_date":   START_DATE,
        "n_tickers":    n_tickers,
        "signals":      ["n_holders", "new_holders", "hhi"],
        "lookup":       lookup,
    }
    pd.to_pickle(payload, OUT_FILE)
    print(f"\n[OK] Saved -> {OUT_FILE}")


if __name__ == "__main__":
    main()
