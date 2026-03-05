# scripts/fetch_sp500_tickers.py
import json
import re
from pathlib import Path

import requests

# ✅ Stooq: S&P 500 구성 종목을 "텍스트/CSV" 형태로 주는 엔드포인트 후보들
# - 위키는 403이 자주 나서(봇 차단) Stooq 쪽이 더 안정적일 때가 많음.
# - 아래 후보 중 하나라도 성공하면 sp500_tickers.json 생성됨.
CANDIDATE_URLS = [
    # (1) 인덱스 구성요소(components/constituents) 계열 (가장 먼저 시도)
    "https://stooq.com/q/i/?s=spx&i=const",
    "https://stooq.com/q/i/?s=sp500&i=const",
    # (2) 혹시 포맷이 바뀌었을 때 대비(실패하면 자동 스킵)
    "https://stooq.com/q/l/?s=spx&f=sd2t2ohlcv&h&e=csv",
]

# ✅ Stooq 요청용 User-Agent
HEADERS = {"User-Agent": "invest-data/1.0 (contact: wjdrjs09076@gmail.com)"}

ROOT = Path(__file__).resolve().parents[1]  # invest-data/
OUT = ROOT / "data" / "sp500_tickers.json"


def normalize_ticker(t: str) -> str:
    t = t.strip().upper()
    # BRK.B -> BRK-B (일반적 표기 통일)
    t = t.replace(".", "-")
    # AAPL.US 같은 접미가 붙는 경우 제거
    t = re.sub(r"\.US$", "", t)
    return t


def extract_tickers_from_text(text: str) -> list[str]:
    """
    Stooq 응답이 어떤 형태든(줄바꿈 텍스트/CSV) 티커로 보이는 토큰을 최대한 추출.
    - 알파벳/숫자/대시/점(정규화 전) 형태만 통과
    - 너무 긴 토큰/이상 토큰 제거
    - 중복 제거(순서 유지)
    """
    raw_tokens = re.split(r"[,\s;\t\r\n]+", text)

    out: list[str] = []
    for tok in raw_tokens:
        if not tok:
            continue

        # 티커로 보이는 패턴만
        if not re.match(r"^[A-Za-z0-9.\-]+$", tok):
            continue
        if len(tok) > 12:
            continue

        t = normalize_ticker(tok)

        # 너무 짧거나 숫자만이면 제외
        if len(t) < 1:
            continue
        if re.match(r"^\d+$", t):
            continue

        out.append(t)

    seen = set()
    uniq: list[str] = []
    for t in out:
        if t in seen:
            continue
        seen.add(t)
        uniq.append(t)

    return uniq


def try_fetch(url: str) -> tuple[list[str] | None, str]:
    """
    url에서 데이터를 받아 티커 리스트를 추출.
    성공하면 (tickers, "ok") 반환.
    실패하면 (None, reason) 반환.
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"

        text = (r.text or "").strip()
        if not text:
            return None, "empty body"

        tickers = extract_tickers_from_text(text)

        # S&P500이면 최소 수백개는 나와야 정상
        if len(tickers) < 200:
            hint = text[:220].replace("\n", " ")
            return None, f"too few tickers ({len(tickers)}). hint={hint}"

        return tickers, "ok"

    except Exception as e:
        return None, f"error: {e}"


def main():
    print("Downloading S&P500 tickers from Stooq (fallback URLs)...")

    last_reason = ""
    tickers: list[str] | None = None

    for url in CANDIDATE_URLS:
        print(f"Try: {url}")
        t, reason = try_fetch(url)
        print(f" -> {reason}")
        last_reason = reason
        if t:
            tickers = t
            break

    if not tickers:
        raise RuntimeError(f"Failed to fetch tickers from all URLs. last_reason={last_reason}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(tickers, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Found {len(tickers)} tickers")
    print(f"Saved -> {OUT}")


if __name__ == "__main__":
    main()