# scripts/fetch_sp500_tickers.py
import json
import os
import re
import sys
from urllib.request import urlopen, Request

URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def main():
    # 현재 작업 디렉토리 기준으로 data 폴더를 확정
    repo_root = os.getcwd()
    out_dir = os.path.join(repo_root, "data")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "sp500_tickers.json")

    req = Request(
        URL,
        headers={"User-Agent": "invest-data/1.0 (contact: wjdrjs09076@gmail.com)"},
    )
    html = urlopen(req, timeout=30).read().decode("utf-8", errors="ignore")

    # 위키 테이블의 Symbol 컬럼은 <td><a ...>MMM</a></td> 형태가 많음
    # BRK.B 같은 건 위키에서 BRK.B로 나오고, 우리쪽은 BRK-B로 통일
    tickers = re.findall(r'<td><a[^>]*>([A-Z.\-]+)</a></td>', html)
    tickers = [t.replace(".", "-") for t in tickers]

    # 중복 제거 + 정렬
    tickers = sorted(set(tickers))

    if len(tickers) < 400:
        print(f"[WARN] ticker count looks too small: {len(tickers)}", file=sys.stderr)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(tickers, f, indent=2)

    print(f"[OK] Saved {len(tickers)} tickers -> {out_path}")

if __name__ == "__main__":
    main()
