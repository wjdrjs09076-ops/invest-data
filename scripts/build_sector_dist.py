import os, json, time, math, datetime
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "data" / "universe.json"
OUT_PATH = ROOT / "data" / "sector_dist.json"

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
SEC_UA = os.environ.get("SEC_USER_AGENT", "invest-portal (contact: example@example.com)")

if not FINNHUB_API_KEY:
    raise SystemExit("FINNHUB_API_KEY missing")
if not SEC_UA:
    raise SystemExit("SEC_USER_AGENT missing")

SEC_TICKER_CIK_URL = "https://www.sec.gov/files/company_tickers.json"

S = requests.Session()
S.headers.update({"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"})

def get_json(url, headers=None):
    r = S.get(url, headers=headers or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def finnhub_profile2(ticker: str):
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={FINNHUB_API_KEY}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def load_universe():
    return json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))

def load_ticker_to_cik():
    data = get_json(SEC_TICKER_CIK_URL)
    # company_tickers.json is {0:{cik_str, ticker, title}, 1:{...}}
    m = {}
    for _, row in data.items():
        t = str(row.get("ticker", "")).upper()
        cik = int(row.get("cik_str", 0))
        if t and cik:
            m[t] = f"{cik:010d}"
    return m

def sec_companyfacts(cik10: str):
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
    return get_json(url)

def pick_sector(p: dict) -> str:
    # priority: gicsSector > finnhubIndustry > Unknown
    s = (p.get("gicsSector") or "").strip()
    if s:
        return s
    s = (p.get("finnhubIndustry") or "").strip()
    if s:
        return s
    return "Unknown"

def latest_annual_value(facts: dict, usgaap_key: str):
    # returns latest FY value (USD) from companyfacts
    try:
        node = facts["facts"]["us-gaap"][usgaap_key]["units"]["USD"]
    except Exception:
        return None

    # prefer FY, form 10-K
    candidates = []
    for it in node:
        if it.get("fy") is None:
            continue
        form = (it.get("form") or "").upper()
        if form not in ("10-K", "20-F", "40-F"):
            continue
        # only annual (fp="FY") if present
        fp = (it.get("fp") or "").upper()
        if fp and fp != "FY":
            continue
        val = it.get("val")
        end = it.get("end")
        if isinstance(val, (int, float)) and end:
            candidates.append((end, float(val)))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]

def safe_num(x):
    return isinstance(x, (int, float)) and math.isfinite(x)

def main():
    uni = load_universe()
    tickers = set()
    for k in ("sp500", "nasdaq100", "dow30"):
        for t in uni.get(k, []):
            tickers.add(str(t).upper().strip())
    tickers = sorted([t for t in tickers if t])

    t2cik = load_ticker_to_cik()

    sectors = {}  # sector -> {"pe":[], "ps":[], "n":int}
    per_ticker = {}  # optional debug: ticker -> computed

    # throttles (Finnhub free-tier friendly)
    finnhub_sleep = 1.1   # ~55 calls/min
    sec_sleep = 0.15      # <= ~6-7 calls/sec

    for idx, t in enumerate(tickers, 1):
        # ---- Finnhub profile2 (sector + marketCap) ----
        try:
            p = finnhub_profile2(t)
        except Exception:
            continue
        time.sleep(finnhub_sleep)

        sector = pick_sector(p)
        mcap_mil = p.get("marketCapitalization")  # million USD (finnhub doc)
        if not safe_num(mcap_mil) or mcap_mil <= 0:
            continue
        mcap = float(mcap_mil) * 1_000_000.0

        cik10 = t2cik.get(t)
        if not cik10:
            continue

        # ---- SEC companyfacts (Revenue, NetIncome) ----
        try:
            facts = sec_companyfacts(cik10)
        except Exception:
            continue
        time.sleep(sec_sleep)

        revenue = latest_annual_value(facts, "Revenues") or latest_annual_value(facts, "SalesRevenueNet")
        netinc = latest_annual_value(facts, "NetIncomeLoss")

        ps = None
        pe = None

        if safe_num(revenue) and revenue and revenue > 0:
            ps = mcap / float(revenue)

        if safe_num(netinc) and netinc and netinc > 0:
            pe = mcap / float(netinc)

        if sector not in sectors:
            sectors[sector] = {"pe": [], "ps": [], "n": 0}

        if safe_num(pe) and 0 < pe < 500:  # guardrail
            sectors[sector]["pe"].append(float(pe))
        if safe_num(ps) and 0 < ps < 200:  # guardrail
            sectors[sector]["ps"].append(float(ps))

        sectors[sector]["n"] += 1
        per_ticker[t] = {"sector": sector, "pe": pe, "ps": ps}

        if idx % 50 == 0:
            print(f"[{idx}/{len(tickers)}] processed...")

    # sort arrays for percentile
    for s, obj in sectors.items():
        obj["pe"] = sorted(obj["pe"])
        obj["ps"] = sorted(obj["ps"])
        obj["pe_n"] = len(obj["pe"])
        obj["ps_n"] = len(obj["ps"])

    payload = {
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "note": "Sector distributions for PE/PS computed from SEC annual Revenue/NetIncome and Finnhub market cap.",
        "sectors": sectors,
        "debug_sample": dict(list(per_ticker.items())[:15]),
    }

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote:", OUT_PATH)

if __name__ == "__main__":
    main()
