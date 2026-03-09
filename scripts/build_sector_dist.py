# scripts/build_sector_dist.py
from __future__ import annotations

import json
import math
import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

FUNDAMENTALS_PATH = DATA_DIR / "fundamentals.json"
OUT_PATH = DATA_DIR / "sector_dist.json"


def load_json(path: Path, default=None):
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )


def safe_num(x):
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def normalize_sector_name(sector_raw: str | None) -> str:
    s = (sector_raw or "").strip()

    mapping = {
        "Technology": "Information Technology",
        "Information Technology": "Information Technology",
        "Healthcare": "Health Care",
        "Health Care": "Health Care",
        "Consumer Defensive": "Consumer Staples",
        "Consumer Staples": "Consumer Staples",
        "Consumer Cyclical": "Consumer Discretionary",
        "Consumer Discretionary": "Consumer Discretionary",
        "Financial": "Financials",
        "Financials": "Financials",
        "Communication": "Communication Services",
        "Communication Services": "Communication Services",
        "Industrials": "Industrials",
        "Energy": "Energy",
        "Materials": "Materials",
        "Utilities": "Utilities",
        "Real Estate": "Real Estate",
    }

    return mapping.get(s, s or "Unknown")


def append_if_valid(arr: list[float], value, *, min_value: float = 0.0, max_value: float | None = None):
    v = safe_num(value)
    if v is None:
        return
    if v <= min_value:
        return
    if max_value is not None and v > max_value:
        return
    arr.append(v)


def main():
    fundamentals = load_json(FUNDAMENTALS_PATH, default={})
    if not isinstance(fundamentals, dict):
        raise RuntimeError("fundamentals.json must be a dict[ticker -> record]")

    sectors: dict[str, dict[str, list[float] | int]] = {}
    debug_sample: dict[str, dict] = {}

    for ticker, rec in fundamentals.items():
        if not isinstance(rec, dict):
            continue

        sector = normalize_sector_name(rec.get("sector"))
        multiples = rec.get("multiples") or {}

        pe = multiples.get("pe")
        ps = multiples.get("ps")

        bucket = sectors.setdefault(
            sector,
            {
                "pe": [],
                "ps": [],
                "n": 0,
            },
        )

        # guardrail:
        # pe: 0 ~ 200
        # ps: 0 ~ 50
        append_if_valid(bucket["pe"], pe, min_value=0.0, max_value=200.0)
        append_if_valid(bucket["ps"], ps, min_value=0.0, max_value=50.0)

        bucket["n"] += 1

        if len(debug_sample) < 20:
            debug_sample[ticker] = {
                "sector": sector,
                "pe": safe_num(pe),
                "ps": safe_num(ps),
            }

    # 정렬 + 중복 제거
    for sector, obj in sectors.items():
        pe_list = sorted(set(round(x, 6) for x in obj["pe"]))  # type: ignore[index]
        ps_list = sorted(set(round(x, 6) for x in obj["ps"]))  # type: ignore[index]

        obj["pe"] = pe_list
        obj["ps"] = ps_list
        obj["pe_n"] = len(pe_list)
        obj["ps_n"] = len(ps_list)

    payload = {
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "note": "Sector distributions for PE/PS computed from fundamentals.json (Yahoo pipeline).",
        "source": "fundamentals.json",
        "sectors": sectors,
        "debug_sample": debug_sample,
    }

    save_json(OUT_PATH, payload)

    print("Wrote:", OUT_PATH)
    print("[INFO] sector counts:")
    for sector, obj in sorted(sectors.items()):
        print(
            f" - {sector}: "
            f"n={obj['n']} "
            f"pe_n={obj['pe_n']} "
            f"ps_n={obj['ps_n']}"
        )


if __name__ == "__main__":
    main()