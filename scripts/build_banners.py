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
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1 / period, adjust=False).mean()
    ma_down = down.ewm(alpha=1 / period, adjust=False).mean()
    rs = ma_up / ma_down.replace(0, math.nan)
    return 100 - (100 / (1 + rs))


def fetch_prices(tickers, period="6mo"):
    frames = {}
    for t in tickers:
        try:
            df = yf.download(t, period=period, interval="1d", auto_adjust=True, progress=False)
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
        close = df["Close"].copy()
        if len(close) < 60:
            continue

        ret_20 = close.iloc[-1] / close.iloc[-21] - 1
        ret_5 = close.iloc[-1] / close.iloc[-6] - 1
        vol_20 = close.pct_change().rolling(20).std().iloc[-1] * (252 ** 0.5)
       rsi_14 = float(rsi(close, 14).iloc[-1].squeeze())

        rows.append(
            {
                "ticker": t,
                "ret_20": float(ret_20),
                "ret_5": float(ret_5),
                "vol_20": float(vol_20) if pd.notna(vol_20) else None,
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

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote:", OUT_PATH)


if __name__ == "__main__":
    main()

