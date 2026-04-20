from __future__ import annotations

import json
import os
import time
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
if not SEC_USER_AGENT:
    raise RuntimeError(
        "SEC_USER_AGENT is missing. "
        f"Checked .env.local under: {ROOT}, {ROOT.parent}, and current working directory."
    )

HEADERS = {
    "User-Agent": SEC_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
}

CACHE_DIR.mkdir(parents=True, exist_ok=True)


def make_session() -> requests.Session:
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
    session.headers.update(HEADERS)
    return session


SESSION = make_session()


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_sp500_items() -> list[dict[str, Any]]:
    payload = read_json(SP500_FILE, default={})
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        raise RuntimeError(f"Failed to load items from {SP500_FILE}")
    return items


def load_sp500_tickers() -> list[str]:
    items = load_sp500_items()
    tickers: list[str] = []

    for item in items:
        if not isinstance(item, dict):
            continue
        t = str(item.get("ticker", "")).strip().upper()
        if t:
            tickers.append(t)

    tickers = sorted(set(tickers))
    if not tickers:
        raise RuntimeError("No tickers found in sp500_current_wiki.json")

    return tickers


def get_json_with_cache(
    url: str,
    cache_path: Path,
    *,
    force_refresh: bool = False,
    timeout: int = 30,
) -> Any:
    if cache_path.exists() and not force_refresh:
        return read_json(cache_path)

    resp = SESSION.get(url, timeout=timeout)

    if resp.status_code == 200:
        data = resp.json()
        write_json(cache_path, data)
        return data

    if cache_path.exists():
        print(f"[WARN] SEC fetch failed ({resp.status_code}) for {url}. Using cached file.")
        return read_json(cache_path)

    raise RuntimeError(f"SEC fetch failed with status {resp.status_code} for {url}")


def load_ticker_cik_map(force_refresh: bool = False) -> dict[str, str]:
    cache_path = CACHE_DIR / "company_tickers.json"
    data = get_json_with_cache(
        SEC_TICKER_CIK_URL,
        cache_path,
        force_refresh=force_refresh,
    )

    mapping: dict[str, str] = {}
    if isinstance(data, dict):
        for v in data.values():
            if not isinstance(v, dict):
                continue
            ticker = str(v.get("ticker", "")).strip().upper()
            cik_raw = v.get("cik_str")
            if not ticker or cik_raw is None:
                continue
            cik = str(cik_raw).zfill(10)
            mapping[ticker] = cik

    if not mapping:
        raise RuntimeError("Failed to build ticker->CIK map from SEC data")

    return mapping


def try_extract_latest_from_tag(facts: dict[str, Any], taxonomy: str, tag: str) -> float | None:
    try:
        units_dict = facts[taxonomy][tag]["units"]
    except Exception:
        return None

    frames: list[pd.DataFrame] = []

    for _, values in units_dict.items():
        if not isinstance(values, list) or not values:
            continue
        df = pd.DataFrame(values)
        if df.empty or "val" not in df.columns:
            continue
        frames.append(df)

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)

    if "form" in df.columns:
        df = df[df["form"].astype(str).isin(["10-K", "10-Q", "20-F", "40-F"])]

    if df.empty:
        return None

    if "end" in df.columns:
        df["end"] = pd.to_datetime(df["end"], errors="coerce")
    else:
        df["end"] = pd.NaT

    if "filed" in df.columns:
        df["filed"] = pd.to_datetime(df["filed"], errors="coerce")
    else:
        df["filed"] = pd.NaT

    df = df.dropna(subset=["val"])
    if df.empty:
        return None

    df = df.sort_values(["end", "filed"], ascending=[True, True])
    val = df.iloc[-1]["val"]

    try:
        return float(val)
    except Exception:
        return None


def first_available_tag(facts: dict[str, Any], candidates: list[tuple[str, str]]) -> float | None:
    for taxonomy, tag in candidates:
        val = try_extract_latest_from_tag(facts, taxonomy, tag)
        if val is not None:
            return val
    return None


def fetch_company_facts(cik: str, force_refresh: bool = False) -> dict[str, Any] | None:
    cache_path = CACHE_DIR / f"companyfacts_{cik}.json"
    url = SEC_FACTS_URL.format(cik=cik)

    try:
        data = get_json_with_cache(url, cache_path, force_refresh=force_refresh, timeout=30)
        if not isinstance(data, dict):
            return None
        return data
    except Exception as e:
        print(f"[WARN] companyfacts fetch failed for CIK {cik}: {e}")
        return None


def extract_financials(companyfacts: dict[str, Any]) -> dict[str, float | None]:
    facts = companyfacts.get("facts", {})
    if not isinstance(facts, dict):
        return {
            "net_income": None,
            "equity": None,
            "gross_profit": None,
            "assets": None,
            "revenue": None,
            "op_income": None,
        }

    net_income = first_available_tag(
        facts,
        [
            ("us-gaap", "NetIncomeLoss"),
            ("us-gaap", "ProfitLoss"),
        ],
    )
    equity = first_available_tag(
        facts,
        [
            ("us-gaap", "StockholdersEquity"),
            ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
            ("us-gaap", "CommonStockholdersEquity"),
        ],
    )
    gross_profit = first_available_tag(
        facts,
        [
            ("us-gaap", "GrossProfit"),
        ],
    )
    assets = first_available_tag(
        facts,
        [
            ("us-gaap", "Assets"),
        ],
    )
    revenue = first_available_tag(
        facts,
        [
            ("us-gaap", "Revenues"),
            ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
            ("us-gaap", "SalesRevenueNet"),
        ],
    )
    op_income = first_available_tag(
        facts,
        [
            ("us-gaap", "OperatingIncomeLoss"),
        ],
    )

    return {
        "net_income": net_income,
        "equity": equity,
        "gross_profit": gross_profit,
        "assets": assets,
        "revenue": revenue,
        "op_income": op_income,
    }


def safe_ratio(num: float | None, den: float | None) -> float | None:
    try:
        if num is None or den is None:
            return None
        if den == 0:
            return None
        if pd.isna(num) or pd.isna(den):
            return None
        return float(num) / float(den)
    except Exception:
        return None


def compute_quality_metrics(fin: dict[str, float | None]) -> dict[str, float | None]:
    roe = safe_ratio(fin.get("net_income"), fin.get("equity"))
    gpa = safe_ratio(fin.get("gross_profit"), fin.get("assets"))
    margin = safe_ratio(fin.get("op_income"), fin.get("revenue"))

    return {
        "roe": roe,
        "gpa": gpa,
        "margin": margin,
    }


def percentile_rank(series: pd.Series) -> pd.Series:
    valid = series.notna()
    out = pd.Series(index=series.index, dtype=float)
    if valid.sum() == 0:
        return out
    out.loc[valid] = series.loc[valid].rank(method="average", pct=True)
    return out


def build_quality_snapshot(force_refresh_sec: bool = False) -> None:
    if LOADED_ENV_PATH is not None:
        print(f"[INFO] Loaded env from: {LOADED_ENV_PATH}")
    print(f"[INFO] Using SEC_USER_AGENT: {SEC_USER_AGENT}")

    tickers = load_sp500_tickers()
    cik_map = load_ticker_cik_map(force_refresh=force_refresh_sec)

    rows: list[dict[str, Any]] = []

    total = len(tickers)
    for i, ticker in enumerate(tickers, start=1):
        cik = cik_map.get(ticker)
        if not cik:
            print(f"[WARN] Missing CIK for {ticker}")
            continue

        companyfacts = fetch_company_facts(cik, force_refresh=force_refresh_sec)
        if not companyfacts:
            continue

        fin = extract_financials(companyfacts)
        metrics = compute_quality_metrics(fin)

        rows.append(
            {
                "ticker": ticker,
                "cik": cik,
                "net_income": fin.get("net_income"),
                "equity": fin.get("equity"),
                "gross_profit": fin.get("gross_profit"),
                "assets": fin.get("assets"),
                "revenue": fin.get("revenue"),
                "op_income": fin.get("op_income"),
                "roe": metrics.get("roe"),
                "gpa": metrics.get("gpa"),
                "margin": metrics.get("margin"),
            }
        )

        if i % 25 == 0 or i == total:
            print(f"[INFO] Processed {i}/{total}: {ticker}")

        time.sleep(0.15)

    if not rows:
        raise RuntimeError("No quality rows were built. SEC fetch likely failed or returned empty data.")

    df = pd.DataFrame(rows)

    df["roe_pct"] = percentile_rank(df["roe"])
    df["gpa_pct"] = percentile_rank(df["gpa"])
    df["margin_pct"] = percentile_rank(df["margin"])

    df["quality_score_raw"] = (
        0.4 * df["roe_pct"].fillna(0.5)
        + 0.4 * df["gpa_pct"].fillna(0.5)
        + 0.2 * df["margin_pct"].fillna(0.5)
    )

    df["quality_score_100"] = (df["quality_score_raw"].clip(0, 1) * 100).round().astype(int)

    result = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "source": "SEC Company Facts API",
        "items": df.to_dict(orient="records"),
        "summary": {
            "tickers_requested": len(tickers),
            "tickers_scored": int(len(df)),
            "formula": "0.4 * ROE_pct + 0.4 * GP/A_pct + 0.2 * OperatingMargin_pct",
            "notes": [
                "ROE = Net Income / Equity",
                "GP/A = Gross Profit / Total Assets",
                "Operating Margin = Operating Income / Revenue",
                "Percentile ranks are cross-sectional across available S&P500 names",
                "Missing components are neutral-filled at 0.5 in final quality score",
            ],
        },
    }

    write_json(OUT_FILE, result)
    print(f"[OK] Saved -> {OUT_FILE}")


if __name__ == "__main__":
    build_quality_snapshot(force_refresh_sec=False)