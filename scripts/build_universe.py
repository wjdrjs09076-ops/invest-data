# scripts/build_universe.py
from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SP500_CANDIDATE_URLS = [
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv",
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
]

UNIVERSE_FILE = DATA_DIR / "universe.json"
SP500_FILE = DATA_DIR / "sp500_tickers.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# 네 현재 universe.json에 이미 들어있는 그룹 목록을 최대한 재사용
# (nasdaq100, dow30 티커는 기존 파일에서 유지)
GROUP_KEYS = ["sp500", "nasdaq100", "dow30"]


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize_ticker(t: str) -> str:
    return str(t).strip().upper().replace(".", "-")


def fetch_sp500_rows() -> list[dict[str, str]]:
    last_reason = ""
    for url in SP500_CANDIDATE_URLS:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                last_reason = f"{url} -> HTTP {r.status_code}"
                continue

            text = (r.text or "").strip()
            if not text:
                last_reason = f"{url} -> empty body"
                continue

            reader = csv.DictReader(StringIO(text))
            fieldnames = reader.fieldnames or []

            if "Symbol" not in fieldnames:
                last_reason = f"{url} -> missing Symbol column: {fieldnames}"
                continue

            sector_col = "GICS Sector" if "GICS Sector" in fieldnames else "Sector"
            sub_industry_col = (
                "GICS Sub-Industry" if "GICS Sub-Industry" in fieldnames else "Sub-Industry"
            )

            rows: list[dict[str, str]] = []
            seen = set()

            for row in reader:
                symbol = normalize_ticker(row.get("Symbol", ""))
                if not symbol or symbol in seen:
                    continue
                seen.add(symbol)

                rows.append(
                    {
                        "ticker": symbol,
                        "name": (row.get("Security") or "").strip(),
                        "sector": (row.get(sector_col) or "").strip() or "Unknown",
                        "subIndustry": (row.get(sub_industry_col) or "").strip(),
                        "headquarters": (row.get("Headquarters Location") or "").strip(),
                    }
                )

            if 450 <= len(rows) <= 550:
                return rows

            last_reason = f"{url} -> unexpected row count {len(rows)}"

        except Exception as e:
            last_reason = f"{url} -> error: {e}"

    raise RuntimeError(f"Failed to fetch S&P 500 constituents. last_reason={last_reason}")


def main():
    # 기존 universe.json 읽기
    existing = load_json(UNIVERSE_FILE, default={})
    if not isinstance(existing, dict):
        existing = {}

    # 기존 그룹 목록 최대한 유지
    existing_groups: dict[str, list[str]] = {}
    for g in GROUP_KEYS:
        arr = existing.get(g, [])
        if not isinstance(arr, list):
            arr = []
        existing_groups[g] = [normalize_ticker(x) for x in arr if isinstance(x, str)]

    # sp500은 최신 CSV 기준으로 교체
    sp500_rows = fetch_sp500_rows()
    sp500_tickers = [row["ticker"] for row in sp500_rows]

    # 기존 universe의 nasdaq100 / dow30는 유지
    existing_groups["sp500"] = sp500_tickers

    # 티커별 메타데이터 맵
    meta_by_ticker: dict[str, dict[str, Any]] = {}

    # 1) S&P500에서 가져온 메타데이터 먼저 반영
    for row in sp500_rows:
        t = row["ticker"]
        meta_by_ticker[t] = {
            "ticker": t,
            "name": row["name"],
            "sector": row["sector"] or "Unknown",
            "subIndustry": row["subIndustry"],
            "headquarters": row["headquarters"],
            "indexFlags": ["sp500"],
        }

    # 2) 기존 groups의 nasdaq100 / dow30를 합치기
    for group_name in ["nasdaq100", "dow30"]:
        for ticker in existing_groups[group_name]:
            meta = meta_by_ticker.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "name": "",
                    "sector": "Unknown",
                    "subIndustry": "",
                    "headquarters": "",
                    "indexFlags": [],
                },
            )
            if group_name not in meta["indexFlags"]:
                meta["indexFlags"].append(group_name)

    # 3) sp500 group 정리
    for ticker in existing_groups["sp500"]:
        meta = meta_by_ticker.setdefault(
            ticker,
            {
                "ticker": ticker,
                "name": "",
                "sector": "Unknown",
                "subIndustry": "",
                "headquarters": "",
                "indexFlags": [],
            },
        )
        if "sp500" not in meta["indexFlags"]:
            meta["indexFlags"].append("sp500")

    # 최종 리스트
    items = sorted(meta_by_ticker.values(), key=lambda x: x["ticker"])

    # build_score_snapshot.py가 잘 읽도록 dict 구조로 저장
    universe_out = {
        "items": items,
        "sp500": existing_groups["sp500"],
        "nasdaq100": sorted(set(existing_groups["nasdaq100"])),
        "dow30": sorted(set(existing_groups["dow30"])),
    }

    save_json(UNIVERSE_FILE, universe_out)
    save_json(SP500_FILE, existing_groups["sp500"])

    print(f"[OK] Saved universe -> {UNIVERSE_FILE}")
    print(f"[OK] Saved sp500 tickers -> {SP500_FILE}")
    print(f"[INFO] items={len(items)} sp500={len(existing_groups['sp500'])} "
          f"nasdaq100={len(set(existing_groups['nasdaq100']))} dow30={len(set(existing_groups['dow30']))}")


if __name__ == "__main__":
    main()