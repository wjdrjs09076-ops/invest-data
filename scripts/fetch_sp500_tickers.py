import csv
import json
from io import StringIO
from pathlib import Path

import requests

CANDIDATE_URLS = [
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv",
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "sp500_tickers.json"


def normalize_ticker(t: str) -> str:
    t = t.strip().upper()
    t = t.replace(".", "-")
    return t


def try_fetch(url: str):
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"

        text = (r.text or "").strip()
        if not text:
            return None, "empty body"

        reader = csv.DictReader(StringIO(text))
        if "Symbol" not in (reader.fieldnames or []):
            return None, f"missing Symbol column. columns={reader.fieldnames}"

        tickers = []
        seen = set()

        for row in reader:
            raw = (row.get("Symbol") or "").strip()
            if not raw:
                continue

            ticker = normalize_ticker(raw)

            if ticker in seen:
                continue
            seen.add(ticker)
            tickers.append(ticker)

        if not (450 <= len(tickers) <= 550):
            return None, f"unexpected ticker count ({len(tickers)})"

        return tickers, "ok"

    except Exception as e:
        return None, f"error: {e}"


def main():
    print("Downloading S&P500 tickers from CSV source...")

    last_reason = ""
    tickers = None

    for url in CANDIDATE_URLS:
        print(f"Try: {url}")
        t, reason = try_fetch(url)
        print(f" -> {reason}")
        last_reason = reason
        if t:
            tickers = t
            break

    if not tickers:
        raise RuntimeError(f"Failed to fetch valid tickers. last_reason={last_reason}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(tickers, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Found {len(tickers)} tickers")
    print(f"Saved -> {OUT}")


if __name__ == "__main__":
    main()