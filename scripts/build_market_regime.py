from __future__ import annotations

import json
import shutil
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"

BENCHMARK   = "SPY"
MA_WINDOW   = 200
LOOKBACK    = "18mo"

# 레짐 필터 임계치
VIX_THRESH        = 25.0   # VIX > 25 → risk-off
YIELD_10Y_THRESH  = 5.0    # 10Y 금리 > 5.0% → risk-off


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def copy_to_public(src: Path) -> None:
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dst = PUBLIC_DATA_DIR / src.name
    shutil.copy2(src, dst)
    print(f"Copied -> {dst}")


def fetch_series(ticker: str, period: str = "5d") -> float | None:
    try:
        df = yf.download(ticker, period=period, interval="1d",
                         auto_adjust=True, progress=False)
        if df is None or df.empty:
            return None
        s = df["Close"].squeeze().dropna()
        return float(s.iloc[-1]) if len(s) else None
    except Exception:
        return None


def main():
    print("Downloading SPY for MA200...")
    df = yf.download(BENCHMARK, period=LOOKBACK, interval="1d",
                     auto_adjust=True, progress=False, threads=True)
    if df is None or df.empty:
        raise RuntimeError("Failed to download SPY data")

    close = df["Close"].dropna()
    if isinstance(close, pd.DataFrame):
        close = close.squeeze()
    if len(close) < MA_WINDOW:
        raise RuntimeError("Not enough SPY history")

    ma200      = close.rolling(MA_WINDOW).mean()
    last_close = float(close.iloc[-1])
    last_ma200 = float(ma200.iloc[-1])
    spy_risk_off = last_close < last_ma200

    print("Fetching VIX and 10Y yield...")
    last_vix   = fetch_series("^VIX")
    last_yield = fetch_series("^TNX")   # 단위: %

    vix_risk_off   = (last_vix   is not None) and (last_vix   > VIX_THRESH)
    yield_risk_off = (last_yield is not None) and (last_yield > YIELD_10Y_THRESH)

    # 세 조건 중 하나라도 해당하면 risk-off
    regime   = "RISK_OFF" if (spy_risk_off or vix_risk_off or yield_risk_off) else "RISK_ON"
    exposure = 0.5 if regime == "RISK_OFF" else 1.0

    # 트리거된 조건 기록
    triggers = []
    if spy_risk_off:   triggers.append(f"SPY({last_close:.1f}) < MA200({last_ma200:.1f})")
    if vix_risk_off:   triggers.append(f"VIX({last_vix:.1f}) > {VIX_THRESH}")
    if yield_risk_off: triggers.append(f"10Y({last_yield:.2f}%) > {YIELD_10Y_THRESH}%")

    print(f"Regime: {regime}  |  triggers: {triggers if triggers else 'none'}")

    out = {
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "benchmark":         BENCHMARK,
        "ma_window":         MA_WINDOW,
        "last_close":        last_close,
        "last_ma":           last_ma200,
        "last_vix":          last_vix,
        "last_yield_10y":    last_yield,
        "vix_threshold":     VIX_THRESH,
        "yield_threshold":   YIELD_10Y_THRESH,
        "triggers":          triggers,
        "regime":            regime,
        "suggested_exposure": exposure,
    }

    out_path = DATA_DIR / "market_regime.json"
    save_json(out_path, out)
    print(f"Saved -> {out_path}")
    copy_to_public(out_path)


if __name__ == "__main__":
    main()
