#!/usr/bin/env python3
"""
build_credit_score_history.py — S&P 방법론 기반 기업 신용 점수 PIT 히스토리 생성

S&P Corporate Methodology (2013) 에서 정량 가능한 5개 지표 복제:
  1. Debt/EBITDA          (레버리지,   weight=30%)
  2. EBITDA/Interest      (이자보상,   weight=25%)
  3. FCF/Debt             (현금창출,   weight=20%)
  4. EBITDA Margin        (수익성,     weight=15%)
  5. Current Ratio        (유동성,     weight=10%)

각 지표를 S&P 등급 기준으로 0~100점으로 변환 → 가중합 composite score.

등급 매핑:
  80-100 → AAA/AA  (Minimal)
  65-80  → A       (Modest)
  50-65  → BBB     (Intermediate)
  35-50  → BB      (Significant)
  20-35  → B       (Aggressive)
  < 20   → CCC/D   (Highly Leveraged / Distress)

Point-in-Time: datekey (SEC 공시일) 기준 — look-ahead bias 없음.

출력:
  data/credit_score_history.pkl   — {ticker: {date_str: score}} 백테스트용
  data/credit_score_snapshot.json — 최신 점수 스냅샷

실행:
  python scripts/build_credit_score_history.py
"""
from __future__ import annotations

import json
import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import nasdaqdatalink
import numpy as np
import pandas as pd

nasdaqdatalink.ApiConfig.api_key = os.environ.get("NASDAQ_DATA_LINK_KEY", "NHr5446JR6sysBKtTBp1")

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUT_PKL  = DATA_DIR / "credit_score_history.pkl"
OUT_JSON = DATA_DIR / "credit_score_snapshot.json"

DIMENSION   = "ARQ"          # Annual Restated Quarterly — PIT 공시일 기준
START_DATE  = "2010-01-01"
MAX_WORKERS = 8

# ── 팩터 가중치 (S&P Financial Risk Profile 비율 근사) ────────────────────────
WEIGHTS = {
    "debt_ebitda":    0.30,
    "coverage":       0.25,
    "fcf_debt":       0.20,
    "ebitda_margin":  0.15,
    "current_ratio":  0.10,
}

# ── 등급 경계 (composite score → S&P 유사 등급) ─────────────────────────────
GRADE_MAP = [
    (80, "AAA/AA"),
    (65, "A"),
    (50, "BBB"),
    (35, "BB"),
    (20, "B"),
    (0,  "CCC/D"),
]

DISTRESS_THRESHOLD = 25.0   # 이 미만 → 하드 필터 대상


def score_debt_ebitda(ratio: float | None) -> float:
    """낮을수록 좋음. S&P Minimal(<2x)~Highly Leveraged(>5x)"""
    if ratio is None or ratio < 0:
        return 50.0     # 정보 없음 → 중립
    if ratio <= 1.5:  return 100.0
    if ratio <= 2.0:  return 85.0
    if ratio <= 3.0:  return 70.0
    if ratio <= 4.0:  return 50.0
    if ratio <= 5.0:  return 30.0
    if ratio <= 7.0:  return 15.0
    return 0.0


def score_coverage(ratio: float | None) -> float:
    """높을수록 좋음. EBITDA/Interest. S&P: >13x=Minimal, <2x=Highly Leveraged"""
    if ratio is None:
        return 50.0
    if ratio <= 0:    return 0.0
    if ratio >= 15:   return 100.0
    if ratio >= 10:   return 85.0
    if ratio >= 6:    return 70.0
    if ratio >= 4:    return 50.0
    if ratio >= 2:    return 30.0
    if ratio >= 1:    return 15.0
    return 0.0


def score_fcf_debt(ratio: float | None) -> float:
    """FCF/Debt (%). >30%=Minimal, <5%=Highly Leveraged"""
    if ratio is None:
        return 50.0
    pct = ratio * 100
    if pct >= 40:   return 100.0
    if pct >= 25:   return 85.0
    if pct >= 15:   return 70.0
    if pct >= 8:    return 50.0
    if pct >= 3:    return 30.0
    if pct >= 0:    return 15.0
    return 0.0      # 음수 FCF


def score_ebitda_margin(margin: float | None) -> float:
    """EBITDA/Revenue (%). >25%=우수, <3%=열위"""
    if margin is None:
        return 50.0
    pct = margin * 100
    if pct >= 30:   return 100.0
    if pct >= 20:   return 85.0
    if pct >= 12:   return 70.0
    if pct >= 6:    return 50.0
    if pct >= 2:    return 30.0
    if pct >= 0:    return 15.0
    return 0.0


def score_current_ratio(ratio: float | None) -> float:
    """유동비율. >2.0=우수, <1.0=위험"""
    if ratio is None:
        return 50.0
    if ratio >= 2.5:  return 100.0
    if ratio >= 2.0:  return 85.0
    if ratio >= 1.5:  return 70.0
    if ratio >= 1.2:  return 50.0
    if ratio >= 1.0:  return 30.0
    if ratio >= 0.8:  return 15.0
    return 0.0


def composite_score(row: dict) -> float | None:
    ebitda  = row.get("ebitda")
    debt    = row.get("debt")
    intexp  = row.get("intexp")
    ncfo    = row.get("ncfo")
    capex   = row.get("capex")
    revenue = row.get("revenue")
    assetsc = row.get("assetsc")
    liabsc  = row.get("liabilitiesc")

    # 최소 데이터 요건: EBITDA, Revenue 중 하나 이상
    if ebitda is None and revenue is None:
        return None

    # 1. Debt/EBITDA
    d_eb = None
    if debt is not None and ebitda is not None and ebitda > 0:
        d_eb = debt / ebitda
    elif debt is not None and debt == 0:
        d_eb = 0.0

    # 2. Coverage (EBITDA/Interest)
    cov = None
    if ebitda is not None and intexp is not None:
        if intexp > 0:
            cov = ebitda / intexp
        elif intexp == 0:
            cov = 999.0     # 무차입 → 최고점

    # 3. FCF/Debt
    fcf_d = None
    if ncfo is not None and capex is not None and debt is not None and debt > 0:
        fcf = ncfo - abs(capex)
        fcf_d = fcf / debt
    elif debt is not None and debt == 0:
        fcf_d = 1.0         # 무차입

    # 4. EBITDA Margin
    margin = None
    if ebitda is not None and revenue is not None and revenue > 0:
        margin = ebitda / revenue

    # 5. Current Ratio
    cur_r = None
    if assetsc is not None and liabsc is not None and liabsc > 0:
        cur_r = assetsc / liabsc

    s = (
        WEIGHTS["debt_ebitda"]   * score_debt_ebitda(d_eb)  +
        WEIGHTS["coverage"]      * score_coverage(cov)       +
        WEIGHTS["fcf_debt"]      * score_fcf_debt(fcf_d)     +
        WEIGHTS["ebitda_margin"] * score_ebitda_margin(margin) +
        WEIGHTS["current_ratio"] * score_current_ratio(cur_r)
    )
    return round(s, 2)


def grade(score: float) -> str:
    for threshold, label in GRADE_MAP:
        if score >= threshold:
            return label
    return "CCC/D"


def fetch_ticker(ticker: str) -> tuple[str, list[dict]]:
    try:
        df = nasdaqdatalink.get_table(
            "SHARADAR/SF1",
            ticker=ticker,
            dimension=DIMENSION,
            datekey={"gte": START_DATE},
            qopts={"columns": [
                "ticker", "datekey", "reportperiod",
                "ebitda", "debt", "intexp", "ncfo", "capex",
                "revenue", "assetsc", "liabilitiesc",
            ]},
            paginate=True,
        )
    except Exception:
        return ticker, []

    if df is None or df.empty:
        return ticker, []

    df = df.sort_values("datekey").reset_index(drop=True)

    records = []
    for _, r in df.iterrows():
        def safe(col):
            v = r.get(col)
            return float(v) if pd.notna(v) else None

        row = {
            "ebitda":       safe("ebitda"),
            "debt":         safe("debt"),
            "intexp":       safe("intexp"),
            "ncfo":         safe("ncfo"),
            "capex":        safe("capex"),
            "revenue":      safe("revenue"),
            "assetsc":      safe("assetsc"),
            "liabilitiesc": safe("liabilitiesc"),
        }
        sc = composite_score(row)
        if sc is None:
            continue
        records.append({
            "datekey":      str(r["datekey"])[:10],
            "reportperiod": str(r["reportperiod"])[:10],
            "score":        sc,
            "grade":        grade(sc),
        })

    return ticker, records


def load_universe() -> list[str]:
    tickers: set[str] = set()
    for fname in ["sp500_current_wiki.json", "sp400_current_wiki.json", "sp600_current_wiki.json"]:
        p = DATA_DIR / fname
        if not p.exists():
            continue
        d = json.loads(p.read_text(encoding="utf-8"))
        items = d.get("items", d) if isinstance(d, dict) else d
        for item in items:
            if isinstance(item, dict):
                t = str(item.get("ticker") or "").strip().upper()
                if t:
                    tickers.add(t)
            elif isinstance(item, str) and item:
                tickers.add(item.upper())
    return sorted(tickers)


def main():
    tickers = load_universe()
    print(f"[INFO] Universe: {len(tickers)} tickers")
    print(f"[INFO] Fetching SF1 ({DIMENSION}) from {START_DATE}...")

    history: dict[str, dict[str, float]] = {}
    snapshot: dict[str, dict] = {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_ticker, t): t for t in tickers}
        for fut in as_completed(futures):
            ticker, records = fut.result()
            done += 1
            if done % 100 == 0:
                print(f"  [{done}/{len(tickers)}]")

            if not records:
                continue

            # PIT 히스토리 저장
            history[ticker] = {rec["datekey"]: rec["score"] for rec in records}

            # 최신 스냅샷 (as-of today)
            latest = max((rec for rec in records if rec["datekey"] <= today),
                         key=lambda x: x["datekey"], default=None)
            if latest:
                snapshot[ticker] = {
                    "score":        latest["score"],
                    "grade":        latest["grade"],
                    "datekey":      latest["datekey"],
                    "reportperiod": latest["reportperiod"],
                    "is_distress":  latest["score"] < DISTRESS_THRESHOLD,
                }

    # ── 저장 ─────────────────────────────────────────────────────────────────
    with open(OUT_PKL, "wb") as f:
        pickle.dump(history, f)
    print(f"[OK] Saved PKL -> {OUT_PKL}  ({len(history)} tickers)")

    # 스냅샷 JSON
    scored_vals = [v["score"] for v in snapshot.values()]
    grade_counts: dict[str, int] = {}
    distress_count = 0
    for v in snapshot.values():
        grade_counts[v["grade"]] = grade_counts.get(v["grade"], 0) + 1
        if v["is_distress"]:
            distress_count += 1

    top10 = sorted(snapshot.items(), key=lambda x: x[1]["score"], reverse=True)[:10]
    bot10 = sorted(snapshot.items(), key=lambda x: x[1]["score"])[:10]

    out = {
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "methodology":      "S&P Corporate Methodology (simplified) — 5-factor composite",
        "weights":          WEIGHTS,
        "distress_threshold": DISTRESS_THRESHOLD,
        "summary": {
            "total":          len(tickers),
            "scored":         len(snapshot),
            "distress_count": distress_count,
            "score_mean":     round(float(np.mean(scored_vals)), 2) if scored_vals else None,
            "score_median":   round(float(np.median(scored_vals)), 2) if scored_vals else None,
            "grade_counts":   grade_counts,
        },
        "top10_safest":    [{"ticker": t, **v} for t, v in top10],
        "top10_riskiest":  [{"ticker": t, **v} for t, v in bot10],
        "tickers": dict(sorted(snapshot.items(), key=lambda x: x[1]["score"], reverse=True)),
    }
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"[OK] Saved JSON -> {OUT_JSON}")

    # ── 콘솔 요약 ────────────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  Scored   : {len(snapshot)} / {len(tickers)}")
    print(f"  Distress (<{DISTRESS_THRESHOLD}): {distress_count}")
    print(f"  Mean score : {out['summary']['score_mean']}")
    if scored_vals:
        print(f"  Grade breakdown:")
        for g, cnt in sorted(grade_counts.items(), key=lambda x: -x[1]):
            print(f"    {g:<10} {cnt:>4}")
    print(f"\n  Top 5 (safest):")
    for t, v in top10[:5]:
        print(f"    {t:<8} {v['score']:>6.1f}  {v['grade']}")
    print(f"\n  Bottom 5 (riskiest):")
    for t, v in bot10[:5]:
        print(f"    {t:<8} {v['score']:>6.1f}  {v['grade']}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
