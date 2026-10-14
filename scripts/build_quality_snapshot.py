"""SEC Company Facts → S&P 500 quality snapshot (ROE, GP/A, operating margin).

추출 규칙 (2026-10 수정, 이전 버전의 기간·태그 혼합 문제 수정)
------------------------------------------------------------------
이전 버전은 태그별로 10-K/10-Q 값을 섞은 뒤 종료일이 가장 최근인 값 하나를 골랐고,
후보 태그 중 '값이 하나라도 있는 첫 태그'를 채택했다. 그 결과
  - 손익 항목에 분기(3개월) 값이 들어가고 자본·자산은 시점 값이라 비율의 기간이 맞지 않았고
  - 오래전에 쓰다 만 태그(예: 2010년대 'Revenues')가 최신 태그보다 먼저 채택돼
    영업이익이 매출보다 큰 불가능한 비율이 생겼다.

현재 규칙
  1. 기간  : 손익 항목은 10-K(및 20-F/40-F, 정정본 포함)의 연간 값만 사용한다.
             기간 길이(end-start)가 350~380일인 값만 연간으로 인정한다(52/53주 회계연도 포함).
             fy/fp 필드는 '문서'의 회계기간이라 비교기 수치에도 같은 값이 붙으므로 쓰지 않는다.
  2. 기준일: 회사별 최신 회계연도 말(FYE)을 하나 정하고, 모든 항목을 그 날짜로 맞춘다.
             손익은 그 FYE로 끝나는 연간 값, 자본·자산은 그 FYE 시점 값.
             그 날짜에 값이 없는 항목은 과거 값으로 채우지 않고 결측으로 둔다.
  3. 태그  : 후보 태그 목록은 이전과 같다. 다만 FYE 시점에 값이 있는 태그만 후보가 된다.
             같은 FYE에 매출 태그가 둘 이상이고 1% 넘게 다르면 플래그를 남긴다.
  4. 단위  : USD 단위만 사용한다.
  5. 연결 기준: 순이익이 지배기업 귀속(NetIncomeLoss)이면 자본도 지배기업 귀속(StockholdersEquity),
             비지배지분 포함 순이익(ProfitLoss)이면 비지배지분 포함 자본을 우선 짝짓는다.
             짝이 안 맞으면 계산은 하되 플래그를 남긴다.
  6. 정정  : 같은 기간 값이 여러 공시에 있으면 가장 늦게 제출된 값(정정 반영)을 쓴다.
  7. 검증  : 영업이익>매출, 매출총이익>매출, 자본≤0, 최신 FYE가 18개월 이상 지남 등은
             해당 비율을 결측(점수 0.5 중립)으로 두고 플래그로 남긴다.

점수 산식(0.4·ROE_pct + 0.4·GP/A_pct + 0.2·Margin_pct, 결측 0.5 중립)과 출력 키는 이전과 같다.
출력에는 fiscal_year_end, source(항목별 태그·공시번호), flags 키가 추가된다.

사용:
  python scripts/build_quality_snapshot.py                 # SEC에서 받거나 캐시 사용, data/quality_snapshot.json 갱신
  python scripts/build_quality_snapshot.py --out data/quality_snapshot_v2.json --offline
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "sec_cache"

SP500_FILE = DATA_DIR / "sp500_current_wiki.json"
OUT_FILE = DATA_DIR / "quality_snapshot.json"

SEC_TICKER_CIK_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
ANNUAL_MIN_DAYS = 350
ANNUAL_MAX_DAYS = 380
STALE_DAYS = 550  # 최신 FYE가 약 18개월보다 오래되면 비율을 쓰지 않는다
UNIT = "USD"

# 후보 태그(우선순위 순). 목록 자체는 이전 버전과 같다.
FLOW_TAGS: dict[str, list[str]] = {
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "gross_profit": ["GrossProfit"],
    "revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
    ],
    "op_income": ["OperatingIncomeLoss"],
}
INSTANT_TAGS: dict[str, list[str]] = {
    "equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "CommonStockholdersEquity",
    ],
    "assets": ["Assets"],
}
# 순이익 태그 → 같은 연결 기준의 자본 태그
EQUITY_BASIS = {
    "NetIncomeLoss": "StockholdersEquity",
    "ProfitLoss": "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
}


# ---------------------------------------------------------------------------
# 환경·네트워크
# ---------------------------------------------------------------------------

def load_env_local_from_candidates() -> Path | None:
    candidates = [
        ROOT / ".env.local",
        ROOT.parent / ".env.local",
        Path.cwd() / ".env.local",
    ]

    for path in candidates:
        if not path.exists():
            continue

        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            if key and key not in os.environ:
                os.environ[key] = value

        return path

    return None


LOADED_ENV_PATH = load_env_local_from_candidates()
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "").strip()

CACHE_DIR.mkdir(parents=True, exist_ok=True)

_SESSION: requests.Session | None = None


def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    if not SEC_USER_AGENT:
        raise RuntimeError(
            "SEC_USER_AGENT is missing (이름 + 이메일 형식). "
            f"Checked .env.local under: {ROOT}, {ROOT.parent}, and current working directory."
        )
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": SEC_USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json, text/plain, */*",
            "Connection": "keep-alive",
        }
    )
    _SESSION = session
    return session


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, allow_nan=False)


def get_json_with_cache(
    url: str,
    cache_path: Path,
    *,
    force_refresh: bool = False,
    offline: bool = False,
    timeout: int = 30,
) -> Any:
    if cache_path.exists() and not force_refresh:
        return read_json(cache_path)
    if offline:
        raise RuntimeError(f"offline mode: no cache for {url}")

    resp = get_session().get(url, timeout=timeout)

    if resp.status_code == 200:
        data = resp.json()
        write_json(cache_path, data)
        return data

    if cache_path.exists():
        print(f"[WARN] SEC fetch failed ({resp.status_code}) for {url}. Using cached file.")
        return read_json(cache_path)

    raise RuntimeError(f"SEC fetch failed with status {resp.status_code} for {url}")


def load_sp500_tickers() -> list[str]:
    payload = read_json(SP500_FILE, default={})
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        raise RuntimeError(f"Failed to load items from {SP500_FILE}")
    tickers = sorted(
        {str(i.get("ticker", "")).strip().upper() for i in items if isinstance(i, dict)} - {""}
    )
    if not tickers:
        raise RuntimeError("No tickers found in sp500_current_wiki.json")
    return tickers


def load_ticker_cik_map(force_refresh: bool = False, offline: bool = False) -> dict[str, str]:
    data = get_json_with_cache(
        SEC_TICKER_CIK_URL,
        CACHE_DIR / "company_tickers.json",
        force_refresh=force_refresh,
        offline=offline,
    )
    mapping: dict[str, str] = {}
    if isinstance(data, dict):
        for v in data.values():
            if not isinstance(v, dict):
                continue
            ticker = str(v.get("ticker", "")).strip().upper()
            cik_raw = v.get("cik_str")
            if ticker and cik_raw is not None:
                mapping[ticker] = str(cik_raw).zfill(10)
    if not mapping:
        raise RuntimeError("Failed to build ticker->CIK map from SEC data")
    return mapping


def fetch_company_facts(cik: str, force_refresh: bool = False, offline: bool = False) -> dict[str, Any] | None:
    try:
        data = get_json_with_cache(
            SEC_FACTS_URL.format(cik=cik),
            CACHE_DIR / f"companyfacts_{cik}.json",
            force_refresh=force_refresh,
            offline=offline,
        )
        return data if isinstance(data, dict) else None
    except Exception as e:
        print(f"[WARN] companyfacts fetch failed for CIK {cik}: {e}")
        return None


# ---------------------------------------------------------------------------
# 추출
# ---------------------------------------------------------------------------

def _to_date(s: Any) -> date | None:
    try:
        return datetime.strptime(str(s), "%Y-%m-%d").date()
    except Exception:
        return None


def _usd_points(facts: dict[str, Any], tag: str) -> list[dict[str, Any]]:
    try:
        pts = facts["us-gaap"][tag]["units"][UNIT]
    except (KeyError, TypeError):
        return []
    return pts if isinstance(pts, list) else []


def annual_series(facts: dict[str, Any], tag: str) -> dict[date, dict[str, Any]]:
    """연간 손익 값: {기간 종료일: 값 기록}. 같은 기간은 가장 늦게 제출된 값."""
    out: dict[date, dict[str, Any]] = {}
    for p in _usd_points(facts, tag):
        if p.get("form") not in ANNUAL_FORMS:
            continue
        start, end = _to_date(p.get("start")), _to_date(p.get("end"))
        val = p.get("val")
        if start is None or end is None or not isinstance(val, (int, float)):
            continue
        if not (ANNUAL_MIN_DAYS <= (end - start).days <= ANNUAL_MAX_DAYS):
            continue
        rec = {
            "val": float(val),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "tag": tag,
            "accn": p.get("accn"),
            "form": p.get("form"),
            "filed": p.get("filed"),
        }
        prev = out.get(end)
        if prev is None or str(rec["filed"]) >= str(prev["filed"]):
            out[end] = rec
    return out


def instant_series(facts: dict[str, Any], tag: str) -> dict[date, dict[str, Any]]:
    """연차보고서의 시점 값(자본·자산): {기준일: 값 기록}."""
    out: dict[date, dict[str, Any]] = {}
    for p in _usd_points(facts, tag):
        if p.get("form") not in ANNUAL_FORMS or p.get("start"):
            continue
        end = _to_date(p.get("end"))
        val = p.get("val")
        if end is None or not isinstance(val, (int, float)):
            continue
        rec = {
            "val": float(val),
            "end": end.isoformat(),
            "tag": tag,
            "accn": p.get("accn"),
            "form": p.get("form"),
            "filed": p.get("filed"),
        }
        prev = out.get(end)
        if prev is None or str(rec["filed"]) >= str(prev["filed"]):
            out[end] = rec
    return out


def latest_fiscal_year_end(flow: dict[str, dict[str, dict[date, dict[str, Any]]]]) -> date | None:
    """매출·영업이익·순이익 후보 태그 전체에서 가장 최근 연간 기간 종료일."""
    ends = [
        end
        for item in ("revenue", "op_income", "net_income")
        for series in flow[item].values()
        for end in series
    ]
    return max(ends) if ends else None


def pick_at(series_by_tag: dict[str, dict[date, dict[str, Any]]], tags: list[str], fye: date) -> dict[str, Any] | None:
    for tag in tags:
        rec = series_by_tag.get(tag, {}).get(fye)
        if rec is not None:
            return rec
    return None


def extract_financials(companyfacts: dict[str, Any], today: date | None = None) -> dict[str, Any]:
    """회사 하나의 최신 회계연도 기준 재무값·출처·플래그."""
    today = today or datetime.now(timezone.utc).date()
    empty = {k: None for k in ("net_income", "equity", "gross_profit", "assets", "revenue", "op_income")}
    facts = companyfacts.get("facts", {}) if isinstance(companyfacts, dict) else {}
    if not isinstance(facts, dict) or "us-gaap" not in facts:
        return {**empty, "fiscal_year_end": None, "period_start": None, "source": {}, "flags": ["no_us_gaap_facts"]}

    flow = {item: {t: annual_series(facts, t) for t in tags} for item, tags in FLOW_TAGS.items()}
    inst = {item: {t: instant_series(facts, t) for t in tags} for item, tags in INSTANT_TAGS.items()}

    fye = latest_fiscal_year_end(flow)
    if fye is None:
        return {**empty, "fiscal_year_end": None, "period_start": None, "source": {}, "flags": ["no_annual_data"]}

    flags: list[str] = []
    picked: dict[str, dict[str, Any] | None] = {}
    for item, tags in FLOW_TAGS.items():
        picked[item] = pick_at(flow[item], tags, fye)

    # 순이익과 같은 연결 기준의 자본을 우선 선택
    ni_tag = picked["net_income"]["tag"] if picked["net_income"] else None
    eq_tags = list(INSTANT_TAGS["equity"])
    preferred_eq = EQUITY_BASIS.get(ni_tag or "")
    if preferred_eq:
        eq_tags.remove(preferred_eq)
        eq_tags.insert(0, preferred_eq)
    picked["equity"] = pick_at(inst["equity"], eq_tags, fye)
    picked["assets"] = pick_at(inst["assets"], INSTANT_TAGS["assets"], fye)

    if preferred_eq and picked["equity"] and picked["equity"]["tag"] != preferred_eq:
        flags.append("roe_basis_mismatch")

    # 같은 FYE에 매출 태그가 여럿이고 서로 다르면 기록
    rev_vals = [flow["revenue"][t][fye]["val"] for t in FLOW_TAGS["revenue"] if fye in flow["revenue"][t]]
    if len(rev_vals) > 1 and max(rev_vals) > 0 and (max(rev_vals) - min(rev_vals)) / max(rev_vals) > 0.01:
        flags.append("revenue_tag_conflict")

    for item, rec in picked.items():
        if rec is None:
            flags.append(f"missing_at_fye:{item}")

    if (today - fye).days > STALE_DAYS:
        flags.append("stale_fye")

    period_start = None
    for item in ("revenue", "net_income", "op_income"):
        if picked.get(item):
            period_start = picked[item]["start"]
            break

    out = {item: (rec["val"] if rec else None) for item, rec in picked.items()}
    out["fiscal_year_end"] = fye.isoformat()
    out["period_start"] = period_start
    out["source"] = {
        item: {k: rec[k] for k in ("tag", "accn", "form", "filed")} for item, rec in picked.items() if rec
    }
    out["flags"] = flags
    return out


# ---------------------------------------------------------------------------
# 비율·점수
# ---------------------------------------------------------------------------

def safe_ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den is None:
        return None
    try:
        num, den = float(num), float(den)
    except Exception:
        return None
    if den == 0 or math.isnan(num) or math.isnan(den):
        return None
    return num / den


def compute_quality_metrics(fin: dict[str, Any]) -> dict[str, float | None]:
    """비율 계산. 해석이 불가능한 값은 결측으로 두고 fin['flags']에 사유를 추가한다."""
    flags: list[str] = fin.setdefault("flags", [])
    if "stale_fye" in flags:
        return {"roe": None, "gpa": None, "margin": None}

    ni, eq = fin.get("net_income"), fin.get("equity")
    gp, assets = fin.get("gross_profit"), fin.get("assets")
    rev, op = fin.get("revenue"), fin.get("op_income")

    roe = None
    if ni is not None and eq is not None:
        if eq <= 0:
            flags.append("nonpositive_equity")
        else:
            roe = safe_ratio(ni, eq)
            # 값은 정확하지만 자본이 매우 작아 ROE 해석이 불안정한 경우(정보용, 값은 유지)
            if assets is not None and assets > 0 and eq / assets < 0.05:
                flags.append("equity_lt_5pct_assets")

    gpa = None
    if gp is not None and assets is not None and assets > 0:
        if rev is not None and rev > 0 and gp > rev * 1.0001:
            flags.append("gross_profit_gt_revenue")
        else:
            gpa = safe_ratio(gp, assets)

    margin = None
    if op is not None and rev is not None:
        if rev <= 0:
            flags.append("nonpositive_revenue")
        elif op > rev * 1.0001:
            flags.append("opinc_gt_revenue")
        else:
            margin = safe_ratio(op, rev)

    return {"roe": roe, "gpa": gpa, "margin": margin}


def percentile_rank(series: pd.Series) -> pd.Series:
    valid = series.notna()
    out = pd.Series(index=series.index, dtype=float)
    if valid.sum() == 0:
        return out
    out.loc[valid] = series.loc[valid].rank(method="average", pct=True)
    return out


def score_frame(df: pd.DataFrame) -> pd.DataFrame:
    """이전 버전과 같은 점수 산식."""
    df = df.copy()
    df["roe_pct"] = percentile_rank(df["roe"])
    df["gpa_pct"] = percentile_rank(df["gpa"])
    df["margin_pct"] = percentile_rank(df["margin"])
    df["quality_score_raw"] = (
        0.4 * df["roe_pct"].fillna(0.5)
        + 0.4 * df["gpa_pct"].fillna(0.5)
        + 0.2 * df["margin_pct"].fillna(0.5)
    )
    df["quality_score_100"] = (df["quality_score_raw"].clip(0, 1) * 100).round().astype(int)
    return df


def _clean(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def build_quality_snapshot(
    force_refresh_sec: bool = False,
    out_file: Path = OUT_FILE,
    offline: bool = False,
    today: date | None = None,
) -> dict[str, Any]:
    if LOADED_ENV_PATH is not None:
        print(f"[INFO] Loaded env from: {LOADED_ENV_PATH}")

    tickers = load_sp500_tickers()
    cik_map = load_ticker_cik_map(force_refresh=force_refresh_sec, offline=offline)

    rows: list[dict[str, Any]] = []
    missing_cik: list[str] = []
    total = len(tickers)
    for i, ticker in enumerate(tickers, start=1):
        cik = cik_map.get(ticker)
        if not cik:
            missing_cik.append(ticker)
            continue

        cached = (CACHE_DIR / f"companyfacts_{cik}.json").exists()
        companyfacts = fetch_company_facts(cik, force_refresh=force_refresh_sec, offline=offline)
        if not companyfacts:
            continue

        fin = extract_financials(companyfacts, today=today)
        metrics = compute_quality_metrics(fin)
        rows.append({"ticker": ticker, "cik": cik, **fin, **metrics})

        if i % 50 == 0 or i == total:
            print(f"[INFO] Processed {i}/{total}: {ticker}")
        if not cached and not offline:
            time.sleep(0.15)

    if not rows:
        raise RuntimeError("No quality rows were built. SEC fetch likely failed or returned empty data.")

    df = score_frame(pd.DataFrame(rows))

    cols = [
        "ticker", "cik", "net_income", "equity", "gross_profit", "assets", "revenue", "op_income",
        "roe", "gpa", "margin", "roe_pct", "gpa_pct", "margin_pct", "quality_score_raw", "quality_score_100",
        "fiscal_year_end", "period_start", "source", "flags",
    ]
    items = [{k: _clean(r.get(k)) for k in cols} for r in df.to_dict(orient="records")]

    flag_counts: dict[str, int] = {}
    for it in items:
        for f in it["flags"]:
            flag_counts[f] = flag_counts.get(f, 0) + 1

    result = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source": "SEC Company Facts API",
        "extraction_version": "2026-10 annual-FYE-aligned",
        "items": items,
        "summary": {
            "tickers_requested": total,
            "tickers_scored": len(items),
            "missing_cik": missing_cik,
            "formula": "0.4 * ROE_pct + 0.4 * GP/A_pct + 0.2 * OperatingMargin_pct",
            "notes": [
                "ROE = Net Income / Equity (same fiscal-year-end; parent NI paired with parent equity)",
                "GP/A = Gross Profit / Total Assets (same fiscal-year-end)",
                "Operating Margin = Operating Income / Revenue (same annual period)",
                "Flow items: annual 10-K/20-F/40-F values only (period length 350-380 days); latest filed value per period",
                "All items aligned to each company's latest fiscal year end; no fallback to older periods",
                "USD unit only",
                "Ratios set to missing (neutral 0.5) for: equity<=0, op income>revenue, gross profit>revenue, FYE older than 550 days",
                "Informational flags (value kept): revenue_tag_conflict, roe_basis_mismatch, equity_lt_5pct_assets",
                "Percentile ranks are cross-sectional across available S&P500 names",
                "Missing components are neutral-filled at 0.5 in final quality score",
                "Snapshot uses latest filings as of run date; not point-in-time for historical backtests",
            ],
            "flag_counts": flag_counts,
        },
    }

    write_json(out_file, result)
    print(f"[OK] Saved -> {out_file}")
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=OUT_FILE, help="출력 경로 (기본: data/quality_snapshot.json)")
    ap.add_argument("--offline", action="store_true", help="네트워크 없이 data/sec_cache만 사용")
    ap.add_argument("--force-refresh", action="store_true", help="캐시를 무시하고 SEC에서 다시 받기")
    args = ap.parse_args()
    out = args.out if args.out.is_absolute() else (Path.cwd() / args.out)
    build_quality_snapshot(force_refresh_sec=args.force_refresh, out_file=out, offline=args.offline)


if __name__ == "__main__":
    main()
