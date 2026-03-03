import os, json, time, datetime
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "data" / "universe.json"
OUT_PATH = ROOT / "data" / "score_snapshot.json"

PORTAL_BASE_URL = os.environ.get("PORTAL_BASE_URL", "").rstrip("/")
SEC_UA = os.environ.get("SEC_USER_AGENT", "invest-portal (contact: example@example.com)")

if not PORTAL_BASE_URL:
    raise SystemExit("PORTAL_BASE_URL missing")
if not SEC_UA:
    raise SystemExit("SEC_USER_AGENT missing")

S = requests.Session()
S.headers.update({"User-Agent": SEC_UA})

def get_json(url: str):
    r = S.get(url, timeout=40)
    r.raise_for_status()
    return r.json()

def load_universe():
    return json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))

def safe_num(x):
    return isinstance(x, (int, float)) and x == x

def summary_stats(scores):
    if not scores:
        return {"n": 0, "mean": None, "p10": None, "p50": None, "p90": None}
    xs = sorted(scores)
    n = len(xs)
    mean = sum(xs) / n

    def q(p):
        if n == 1: 
            return xs[0]
        idx = int(round(p * (n - 1)))
        idx = max(0, min(n - 1, idx))
        return xs[idx]

    return {"n": n, "mean": round(mean, 2), "p10": q(0.10), "p50": q(0.50), "p90": q(0.90)}

def main():
    uni = load_universe()

    payload = {
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "source": "portal_api:/api/recommendation",
        "universes": {},
    }

    for label, key in [("S&P 500", "sp500"), ("NASDAQ-100", "nasdaq100"), ("Dow 30", "dow30")]:
        tickers = [str(t).upper().strip() for t in uni.get(key, []) if str(t).strip()]
        scores = []
        per_ticker = {}

        # Finnhub/SEC rate 안정성 위해 천천히
        for i, t in enumerate(tickers, 1):
            url = f"{PORTAL_BASE_URL}/api/recommendation?ticker={t}"
            try:
                j = get_json(url)
                sc = j.get("score")
                conf = j.get("confidence")
                sector = (j.get("summary") or {}).get("sector")
                if safe_num(sc):
                    sc = float(sc)
                    scores.append(sc)
                    per_ticker[t] = {
                        "score": sc,
                        "confidence": conf,
                        "sector": sector,
                    }
            except Exception:
                pass

            # 너무 빠르면 막힐 수 있어서
            time.sleep(0.35)

            if i % 50 == 0:
                print(f"[{label}] {i}/{len(tickers)}")

        payload["universes"][key] = {
            "label": label,
            "stats": summary_stats(scores),
            "scores": per_ticker,  # ticker->score mapping
        }

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote:", OUT_PATH)

if __name__ == "__main__":
    main()
