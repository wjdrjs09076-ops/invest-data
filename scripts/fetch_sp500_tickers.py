import json
import re
from pathlib import Path

import requests

# ✅ Stooq: S&P 500 구성 종목 (가벼운 텍스트/CSV 소스)
# - Stooq는 여러 인덱스 구성요소를 텍스트/CSV 형태로 제공하는 경우가 많고,
#   위키처럼 HTML 파싱이 아니라서 안정적임.
#
# ⚠️ 만약 아래 URL이 404/변경되면,
#   콘솔에 찍히는 status/본문 일부를 기반으로 바로 다른 엔드포인트로 바꿔줄게.

CANDIDATE_URLS = [
    # (1) 가장 흔히 쓰는: index 구성요소 다운로드 류
    "https://stooq.com/q/i/?s=spx&i=const",      # 예시: 구성 종목 텍스트/CSV 형태로 내려오는 경우
    "https://stooq.com/q/i/?s=sp500&i=const",    # 예시: sp500 별칭이 존재할 수 있음
    # (2) 혹시 다른 포맷일 때 대비
    "https://stooq.com/q/l/?s=spx&f=sd2t2ohlcv&h&e=csv",  # 실패 대비용(작동 안 하면 자동 스킵)
]

HEADERS = {"User-Agent": "invest-data/1.0 (contact: wjdrjs09076@gmail.com)"}

ROOT = Path(__file__).resolve().parents[1]  # invest-data/
OUT = ROOT / "data" / "sp500_tickers.json"


def normalize_ticker(t: str) -> str:
    t = t.strip().upper()
    # BRK.B -> BRK-B (일반적인 표기 통일)
    t = t.replace(".", "-")
    # 혹시 US 마켓 접미/접두가 붙는 경우 제거(예: AAPL.US)
    t = re.sub(r"\.US$", "", t)
    return t


def extract_tickers_from_text(text: str) -> list[str]:
    """
    Stooq 응답이 어떤 형태든(줄바꿈 텍스트 / CSV) 티커로 보이는 토큰을 최대한 추출.
    - 알파벳/숫자/대시/점(정규화 전) 형태만 통과
    - 너무 긴 토큰/이상 토큰 제거
    """
    # CSV/텍스트 통합 토큰화
    raw_tokens = re.split(r"[,\s;\t\r\n]+", text)

    out: list[str] = []
    for tok in raw_tokens:
        if not tok:
            continue
        # 티커로 보이는 것만
        if not re.match(r"^[A-Za-z0-9.\-]+$", tok):
            continue
        if len(tok) > 10:
            continue

        t = normalize_ticker(tok)

        # 너무 짧거나 숫자만이면 제외
        if len(t) < 1:
            continue
        if re.match(r"^\d+$", t):
            continue

        out.append(t)

    # 중복 제거(순서 유지)
    seen = set()
    uniq = []
    for t in out:
        if t in seen:
            continue
        seen.add(t)
        uniq.append(t)
    return uniq


def try_fetch(url: str) -> tuple[list[str] | None, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"

        text = r.text.strip()
        if not text:
            return None, "empty body"

        tickers = extract_tickers_from_text(text)

        # S&P500이면 최소 수백개는 나와야 정상
        if len(tickers) < 200:
            # 디버깅용: 응답 일부 힌트
            hint = text[:200].replace("\n", " ")
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
    OUT.write_text(json.dumps(tickers, indent=2), encoding="utf-8")

    print(f"Found {len(tickers)} tickers")
    print(f"Saved -> {OUT}")


if __name__ == "__main__":
    main()
