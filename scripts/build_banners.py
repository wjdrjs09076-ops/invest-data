import json
import math
import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_PATH = ROOT / "data" / "universe.json"
OUT_PATH = ROOT / "data" / "banners.json"


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
    yfinance는 상황에 따라 컬럼이 MultiIndex로 내려오거나,
    Close가 DataFrame 형태로 잡히는 경우가 있음.
    이 함수는 Close를 무조건 1D Series로 정규화한다.
    """
    if df is None or df.empty:
        return None

    if "Close" not in df.columns:
        return None

    close = df["Close"]

    # close가 DataFrame(멀티컬럼)인 경우: 마지막 컬럼 하나 사용
    if isinstance(close, pd.DataFrame):
        # 보통 컬럼이 ('Close', 'AAPL') 같은 MultiIndex거나, 여러 컬럼일 수 있음
        close = close.iloc[:, -1]

    # 이제 close는 Series여야 함
    if not isinstance(close, pd.Series):
        try:
            close = pd.Series(close)
        except Exception:
            return None

    close = close.dropna()
    return close if not close.empty else None


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
                group_by="column",  # 컬럼형태를 조금 더 안정적으로
                threads=False,      # Actions에서 안정성 ↑
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

        # 안전하게 인덱스 체크
        if len(close) < 21:
            continue

        ret_20 = close.iloc[-1] / close.iloc[-21] - 1
        ret_5 = close.iloc[-1] / close.iloc[-6] - 1

        vol_20 = close.pct_change().rolling(20).std().iloc[-1]
        vol_20 = float(vol_20) * (252 ** 0.5) if pd.notna(vol_20) else None

        rsi_series = rsi(close, 14)
        if rsi_series is None or len(rsi_series.dropna()) == 0:
            continue
        # 마지막 값을 스칼라로 강제
        rsi_14 = float(pd.Series(rsi_series).dropna().iloc[-1])

        rows.append(
            {
                "ticker": t,
                "ret_20": float(ret_20),
                "ret_5": float(ret_5),
                "vol_20": vol_20,
                "rsi_14": rsi_14,
            }
        )

    dfm = pd.DataFrame(rows)
    if dfm.empty:
        return {"universe": label, "sections": []}

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

    # ✅ 빈 파일 방지: 최소한 하나라도 아이템이 있어야 저장
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
