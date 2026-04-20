import json
import datetime
from pathlib import Path
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "data" / "universe.json"
OUT_DIR = ROOT / "data" / "news"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MAX_PER_TICKER = 25

def gdelt_fetch(ticker: str, limit: int = 25):
    query = f"({ticker}) (stock OR shares OR earnings OR revenue OR guidance OR outlook)"
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "sort": "datedesc",
        "maxrecords": str(limit),
    }
    url = "https://api.gdeltproject.org/api/v2/doc/doc?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "invest-data/1.0", "Accept": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
        data = json.loads(raw)

    articles = data.get("articles") or data.get("results") or []
    items = []
    for a in articles:
        title = a.get("title") or a.get("name") or ""
        link = a.get("url") or a.get("shareUrl") or ""
        if not title or not link:
            continue
        items.append({
            "title": title,
            "url": link,
            "domain": a.get("domain", ""),
            "seendate": a.get("seendate", "") or a.get("date", "") or a.get("datetime", ""),
        })
    return items[:limit]

def main():
    universe = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    # 3개 유니버스 합쳐서 중복 제거
    tickers = set(universe.get("sp500", []) + universe.get("nasdaq100", []) + universe.get("dow30", []))

    generated = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    for t in sorted(tickers):
        t = t.upper().strip()
        if not t:
            continue

        try:
            items = gdelt_fetch(t, MAX_PER_TICKER)
            payload = {"ticker": t, "generated_at_utc": generated, "count": len(items), "items": items}
            (OUT_DIR / f"{t}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print("OK", t, len(items))
        except Exception as e:
            # 실패해도 전체 작업이 멈추지 않게
            payload = {"ticker": t, "generated_at_utc": generated, "error": str(e), "count": 0, "items": []}
            (OUT_DIR / f"{t}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print("FAIL", t, e)

if __name__ == "__main__":
    main()
