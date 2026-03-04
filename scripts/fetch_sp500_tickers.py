# scripts/fetch_sp500_tickers.py
import json
import re
import pandas as pd
import requests

URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def main():
    r = requests.get(URL, timeout=30, headers={"User-Agent": "invest-data/1.0"})
    r.raise_for_status()

    # 위키 페이지의 표들을 전부 읽어오고, "Symbol" 컬럼이 있는 테이블을 찾는다
    tables = pd.read_html(r.text)
    target = None
    for t in tables:
        if "Symbol" in t.columns:
            target = t
            break

    if target is None:
        raise RuntimeError("No table with 'Symbol' column found. Wikipedia structure may have changed.")

    symbols = target["Symbol"].astype(str).tolist()

    out = []
    for s in symbols:
        s = s.strip().upper()
        # BRK.B 같은 케이스는 Finnhub/야후에서 BRK-B로 쓰는 경우가 많으니 변환
        s = s.replace(".", "-")
        # 아주 기본적인 안전 필터
        if re.fullmatch(r"[A-Z0-9\-]+", s):
            out.append(s)

    out = sorted(set(out))

    print(f"Saved {len(out)} tickers.")
    if len(out) < 450:
        raise RuntimeError(f"Ticker count too low ({len(out)}). Parsing likely broken.")

    with open("data/sp500_tickers.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

if __name__ == "__main__":
    main()
