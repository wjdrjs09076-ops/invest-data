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

SP400_FILE = DATA_DIR / "sp400_current_wiki.json"
SP600_FILE = DATA_DIR / "sp600_current_wiki.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

GROUP_KEYS = ["sp500", "sp400", "sp600", "nasdaq100", "dow30"]


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


def fetch_sp500_rows():

    last_reason = ""

    for url in SP500_CANDIDATE_URLS:
        try:

            r = requests.get(url, headers=HEADERS, timeout=30)

            if r.status_code != 200:
                last_reason = f"{url} -> HTTP {r.status_code}"
                continue

            reader = csv.DictReader(StringIO(r.text))

            rows = []
            seen = set()

            for row in reader:

                symbol = normalize_ticker(row.get("Symbol", ""))

                if not symbol or symbol in seen:
                    continue

                seen.add(symbol)

                rows.append(
                    {
                        "ticker": symbol,
                        "name": row.get("Security", ""),
                        "sector": row.get("GICS Sector", "Unknown"),
                        "subIndustry": row.get("GICS Sub-Industry", ""),
                        "headquarters": row.get("Headquarters Location", ""),
                    }
                )

            if 450 <= len(rows) <= 550:
                return rows

            last_reason = f"unexpected row count {len(rows)}"

        except Exception as e:
            last_reason = str(e)

    raise RuntimeError(f"Failed to fetch S&P500 constituents: {last_reason}")


def load_index_file(path: Path):

    data = load_json(path, default={})

    items = data.get("items", [])

    tickers = []

    for item in items:
        t = normalize_ticker(item.get("ticker"))
        if t:
            tickers.append(t)

    return tickers


def main():

    existing = load_json(UNIVERSE_FILE, default={})

    existing_groups = {}

    for g in GROUP_KEYS:
        arr = existing.get(g, [])
        existing_groups[g] = [normalize_ticker(x) for x in arr]

    sp500_rows = fetch_sp500_rows()
    sp500_tickers = [row["ticker"] for row in sp500_rows]

    sp400_tickers = load_index_file(SP400_FILE)
    sp600_tickers = load_index_file(SP600_FILE)

    existing_groups["sp500"] = sp500_tickers
    existing_groups["sp400"] = sp400_tickers
    existing_groups["sp600"] = sp600_tickers

    meta_by_ticker = {}

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

    for group_name in GROUP_KEYS:

        for ticker in existing_groups.get(group_name, []):

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

    items = sorted(meta_by_ticker.values(), key=lambda x: x["ticker"])

    universe_out = {
        "items": items,
        "sp500": existing_groups["sp500"],
        "sp400": existing_groups["sp400"],
        "sp600": existing_groups["sp600"],
        "nasdaq100": sorted(set(existing_groups.get("nasdaq100", []))),
        "dow30": sorted(set(existing_groups.get("dow30", []))),
    }

    save_json(UNIVERSE_FILE, universe_out)
    save_json(SP500_FILE, existing_groups["sp500"])

    print("Universe built successfully")
    print("Total tickers:", len(items))


if __name__ == "__main__":
    main()