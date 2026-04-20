from __future__ import annotations

import io
import json
import re
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"

EVENTS_OUT = DATA_DIR / "sp400_membership_events.json"
CURRENT_OUT = DATA_DIR / "sp400_current_wiki.json"

COPY_TO_PUBLIC = True


@dataclass
class MembershipEvent:
    date: str
    added: list[str]
    removed: list[str]


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def copy_to_public(src: Path) -> None:
    if not COPY_TO_PUBLIC:
        return
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst = PUBLIC_DATA_DIR / src.name
    shutil.copy2(src, dst)
    print(f"[OK] Copied -> {dst}")


def normalize_ticker(t: Any) -> str:
    s = str(t).strip().upper()
    s = re.sub(r"\[[^\]]*\]", "", s).strip()
    s = s.replace(".", "-")
    s = s.replace("/", "-")
    s = re.sub(r"\s+", "", s)
    return s


def clean_text(v: Any) -> str:
    s = "" if v is None else str(v)
    s = re.sub(r"\[[^\]]*\]", "", s)
    s = s.replace("\xa0", " ")
    return s.strip()


def flatten_columns(columns) -> list[str]:
    out: list[str] = []

    if isinstance(columns, pd.MultiIndex):
        for tup in columns:
            parts = [clean_text(x) for x in tup if clean_text(x)]
            out.append(" ".join(parts).strip())
    else:
        out = [clean_text(c) for c in columns]

    return out


def find_table_by_columns(tables: list[pd.DataFrame], required_keywords: list[str]) -> pd.DataFrame | None:
    for df in tables:
        cols = flatten_columns(df.columns)
        cols_lower = [c.lower() for c in cols]

        if all(any(key in c for c in cols_lower) for key in required_keywords):
            df2 = df.copy()
            df2.columns = cols
            return df2

    return None


def fetch_wiki_tables() -> list[pd.DataFrame]:
    print(f"[INFO] Reading tables from {WIKI_URL}")

    headers = {
        "User-Agent": "invest-portal-backtest/1.0 (personal research project)"
    }

    resp = requests.get(WIKI_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    html = resp.text
    tables = pd.read_html(io.StringIO(html))

    if not tables:
        raise RuntimeError("No tables found on Wikipedia page")

    print(f"[INFO] Found {len(tables)} tables")
    return tables


def parse_current_constituents(tables: list[pd.DataFrame]) -> dict[str, dict[str, Any]]:
    df = find_table_by_columns(
        tables,
        required_keywords=["symbol", "security"],
    )
    if df is None:
        raise RuntimeError("Failed to locate current constituents table")

    colmap = {c.lower(): c for c in df.columns}

    symbol_col = next((colmap[c] for c in colmap if "symbol" in c), None)
    security_col = next((colmap[c] for c in colmap if "security" in c), None)
    sector_col = next((colmap[c] for c in colmap if "gics sector" in c), None)
    subindustry_col = next((colmap[c] for c in colmap if "sub-industry" in c or "sub industry" in c), None)
    hq_col = next((colmap[c] for c in colmap if "headquarters" in c), None)

    if symbol_col is None:
        raise RuntimeError("Current constituents table is missing Symbol column")

    result: dict[str, dict[str, Any]] = {}

    for _, row in df.iterrows():
        ticker = normalize_ticker(row.get(symbol_col))
        if not ticker:
            continue

        item = {
            "ticker": ticker,
            "name": clean_text(row.get(security_col)) if security_col else "",
            "sector": clean_text(row.get(sector_col)) if sector_col else "Unknown",
            "subIndustry": clean_text(row.get(subindustry_col)) if subindustry_col else "",
            "headquarters": clean_text(row.get(hq_col)) if hq_col else "",
            "indexFlags": ["sp500"],
        }
        result[ticker] = item

    if not result:
        raise RuntimeError("Parsed current constituents table but got zero tickers")

    print(f"[INFO] Parsed current constituents: {len(result)} tickers")
    return result


def parse_change_date(value: Any) -> str | None:
    s = clean_text(value)
    if not s:
        return None

    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None

    return dt.strftime("%Y-%m-%d")


def extract_tickers_from_cell(value: Any) -> list[str]:
    s = clean_text(value)
    if not s or s.lower() == "nan":
        return []

    inside_parens = re.findall(r"\(([A-Za-z.\-\/]+)\)", s)
    tickers = [normalize_ticker(x) for x in inside_parens if normalize_ticker(x)]

    candidates = re.findall(r"\b[A-Z]{1,5}(?:[.\-\/][A-Z]{1,3})?\b", s)
    tickers.extend(normalize_ticker(x) for x in candidates if normalize_ticker(x))

    out: list[str] = []
    seen = set()
    for t in tickers:
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)

    return out


def parse_changes_table(
    tables: list[pd.DataFrame],
    current_universe: dict[str, dict[str, Any]],
) -> list[MembershipEvent]:
    df = find_table_by_columns(
        tables,
        required_keywords=["date", "added", "removed"],
    )
    if df is None:
        raise RuntimeError("Failed to locate changes table")

    colmap = {c.lower(): c for c in df.columns}

    date_col = next((colmap[c] for c in colmap if "date" in c), None)
    if date_col is None:
        raise RuntimeError("Changes table missing date column")

    added_cols = [colmap[c] for c in colmap if "added" in c]
    removed_cols = [colmap[c] for c in colmap if "removed" in c]

    if not added_cols or not removed_cols:
        raise RuntimeError("Changes table missing added/removed columns")

    events: list[MembershipEvent] = []

    for _, row in df.iterrows():
        date_str = parse_change_date(row.get(date_col))
        if not date_str:
            continue

        added: list[str] = []
        removed: list[str] = []

        for c in added_cols:
            added.extend(extract_tickers_from_cell(row.get(c)))
        for c in removed_cols:
            removed.extend(extract_tickers_from_cell(row.get(c)))

        added = list(dict.fromkeys([t for t in added if t]))
        removed = list(dict.fromkeys([t for t in removed if t]))

        if not added and not removed:
            continue

        events.append(MembershipEvent(date=date_str, added=added, removed=removed))

    events.sort(key=lambda x: x.date)

    print(f"[INFO] Parsed membership change events: {len(events)} rows")
    return events


def reconstruct_membership_as_of(
    as_of: str,
    current_universe: dict[str, dict[str, Any]],
    events: list[MembershipEvent],
) -> list[str]:
    target = pd.to_datetime(as_of)
    members = set(current_universe.keys())

    for ev in sorted(events, key=lambda x: x.date, reverse=True):
        ev_date = pd.to_datetime(ev.date)
        if ev_date > target:
            for t in ev.added:
                members.discard(t)
            for t in ev.removed:
                members.add(t)

    return sorted(members)


def build_payload(
    current_universe: dict[str, dict[str, Any]],
    events: list[MembershipEvent],
) -> dict[str, Any]:
    return {
        "source": "Wikipedia - List of S&P 500 companies",
        "current_count": len(current_universe),
        "event_count": len(events),
        "current_tickers": sorted(current_universe.keys()),
        "events": [asdict(ev) for ev in events],
        "notes": [
            "Current membership is parsed from the Wikipedia S&P 500 constituents table.",
            "Change history is parsed from the Wikipedia changes table.",
            "Historical membership should be reconstructed by reversing change events from the current set.",
            "Ticker normalization converts dots to dashes for compatibility with yfinance (e.g. BRK.B -> BRK-B).",
        ],
    }


def main() -> None:
    tables = fetch_wiki_tables()
    current_universe = parse_current_constituents(tables)
    events = parse_changes_table(tables, current_universe)

    events_payload = build_payload(current_universe, events)
    current_payload = {
        "source": "Wikipedia - List of S&P 500 companies",
        "count": len(current_universe),
        "items": list(current_universe.values()),
    }

    save_json(EVENTS_OUT, events_payload)
    save_json(CURRENT_OUT, current_payload)

    print(f"[OK] Saved -> {EVENTS_OUT}")
    print(f"[OK] Saved -> {CURRENT_OUT}")

    copy_to_public(EVENTS_OUT)
    copy_to_public(CURRENT_OUT)

    example_date = "2023-01-01"
    reconstructed = reconstruct_membership_as_of(example_date, current_universe, events)
    print(f"[INFO] Reconstructed membership as of {example_date}: {len(reconstructed)} tickers")


if __name__ == "__main__":
    main()