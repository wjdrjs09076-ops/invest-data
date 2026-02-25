import json
import math
import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "data" / "universe.json"
OUT_PATH = ROOT / "data" / "banners.json"


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    # series must be 1D
    series = pd.Series(series).dropna()
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1 / period, adjust=False).mean()
    ma_down = down.ewm(alpha=1 / period, adjust=False).mean()
    rs = ma_up / ma_down.replace(0, math.nan)
    return 100 - (100 / (1 + rs))


def _to_close_series(df: pd.DataFrame) -> pd.Series | None:
    """
    yfinance는 상황에 따라 Close가 DataFrame(멀티컬럼)으로 잡히는 경우가 있음.
    무조건 1D Series로 정규화.
    """
    if df is None or df.empty:
        return None
    if "Close" not in df.columns:
        return None

    close = df["Close"]

    # close가 DataFrame(멀티컬럼)인 경우: 마지막 컬럼 하나 사용
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, -1]

    if not isinstance(close, pd.Series):
        try:
            close = pd.Series(close)
        except Exception:
            return None

    close = close.dropna()
    return close if not close.empty else None


def score_row(ret_20: float, ret_5: float, vol_20: float | None, rsi_14: float) -> tuple[float, str]:
    """
    0~100 점수 (MVP 룰)
    - RSI 낮고(과매도) + 반등(5D>0) => 가점
    - RSI 높고 + 과열 모멘텀 => 감점
    - 변동성 너무 높으면 감점
    """
    score = 50.0

    # RSI 기반
    if rsi_14 <= 30:
        score += 18
    elif rsi_14 <= 35:
        score += 12
    elif rsi_14 >= 75:
        score -= 14
    elif rsi_14 >= 70:
        score -= 10

    # 단기 반등/하락(5일)
    score += clamp(ret_5 * 100, -4, 4) * 2.0  # -8 ~ +8

    # 20일 모멘텀(너무 과열이면 감점, 너무 약세면 가점)
    if ret_20 >= 0.25:
        score -= 10
    elif ret_20 <= -0.20:
        score += 6

    # 변동성(연율화)
    if vol_20 is not None:
        if vol_20 >= 0.70:
            score -= 10
        elif vol_20 >= 0.50:
            score -= 6

    score = clamp(score, 0, 100)

    label = "HOLD"
    if score >= 65:
        label = "BUY"
    elif score <= 35:
        label = "SELL"

    return score, label


def fetch_prices(tickers, period="6mo"):
    frames = {}
    for t in tickers:
        try:
            df = yf.download(
                t,
                period=period,
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="column",
                threads=False,
            )
            if df is None or df.empty:
                continue
            df = df.dropna()
            frames[t] = df
        except Exception:
            continue
    return frames


def build_universe_block(label, tickers):
    prices = fetch_prices(tickers)

    rows = []
    for t, df in prices.items():
        close = _to_close_series(df)
        if close is None or len(close) < 60:
            continue

        # 최소 21개 필요(20D 수익률)
        if len(close) < 21:
            continue

        ret_20 = close.iloc[-1] / close.iloc[-21] - 1
        ret_5 = close.iloc[-1] / close.iloc[-6] - 1

        vol_20_raw = close.pct_change().rolling(20).std().iloc[-1]
        vol_20 = float(vol_20_raw) * (252 ** 0.5) if pd.notna(vol_20_raw) else None

        rsi_series = rsi(close, 14)
        if rsi_series is None or len(pd.Series(rsi_series).dropna()) == 0:
            continue
        rsi_14 = float(pd.Series(rsi_series).dropna().iloc[-1])

        score, signal = score_row(float(ret_20), float(ret_5), vol_20, rsi_14)

        rows.append(
            {
                "ticker": t,
                "ret_20": float(ret_20),
                "ret_5": float(ret_5),
                "vol_20": vol_20,
                "rsi_14": rsi_14,
                "score": float(score),
                "signal": signal,  # BUY/HOLD/SELL
            }
        )

    dfm = pd.DataFrame(rows)
    if dfm.empty:
        return {"universe": label, "sections": []}

    # 섹션용 랭킹
    buy = dfm[dfm["signal"] == "BUY"].sort_values(["score", "ret_5"], ascending=False).head(10)
    mom = dfm.sort_values("ret_20", ascending=False).head(10)
    rev = dfm[(dfm["rsi_14"] < 35) & (dfm["ret_5"] > 0)].sort_values("ret_5", ascending=False).head(10)
    risk = dfm.dropna(subset=["vol_20"]).sort_values("vol_20", ascending=False).head(10)

    def to_items(x, label_fn, reason_fn=None):
        items = []
        for _, r in x.iterrows():
            items.append(
                {
                    "ticker": r["ticker"],
                    "label": label_fn(r),
                    "reason": reason_fn(r) if reason_fn else "",
                }
            )
        return items

    return {
        "universe": label,
        "sections": [
            {
                "title": "Today's BUY Candidates",
                "items": to_items(
                    buy,
                    lambda r: f"{r['signal']} • score {r['score']:.0f}",
                    lambda r: f"RSI {r['rsi_14']:.0f}, 5D {r['ret_5']*100:.1f}%, 20D {r['ret_20']*100:.1f}%",
                ),
            },
            {
                "title": "Momentum TOP",
                "items": to_items(mom, lambda r: f"20D {r['ret_20']*100:.1f}%"),
            },
            {
                "title": "Oversold Reversal",
                "items": to_items(rev, lambda r: f"RSI {r['rsi_14']:.0f}, 5D {r['ret_5']*100:.1f}%"),
            },
            {
                "title": "Risk High",
                "items": to_items(risk, lambda r: f"Vol {r['vol_20']*100:.0f}%"),
            },
        ],
    }


def main():
    universe = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))

    payload = {
        "generated_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "universes": [
            build_universe_block("S&P 500", universe.get("sp500", [])),
            build_universe_block("NASDAQ-100", universe.get("nasdaq100", [])),
            build_universe_block("Dow 30", universe.get("dow30", [])),
        ],
    }

    # ✅ 빈 파일 방지: 최소 하나라도 아이템이 있어야 저장
    total_items = 0
    for u in payload["universes"]:
        for sec in u.get("sections", []):
            total_items += len(sec.get("items", []))

    if total_items == 0:
        raise RuntimeError("No banner items generated. Aborting write to avoid empty banners.json")

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote:", OUT_PATH)


if __name__ == "__main__":
    main()
