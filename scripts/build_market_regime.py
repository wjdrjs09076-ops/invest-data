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

BENCHMARK = "SPY"
MA_WINDOW = 200
LOOKBACK_PERIOD = "18mo"


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def copy_to_public(src: Path) -> None:
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)

    dst = PUBLIC_DATA_DIR / src.name
    shutil.copy2(src, dst)

    print(f"Copied -> {dst}")


def main():

    print("Downloading benchmark...")

    df = yf.download(
        BENCHMARK,
        period=LOOKBACK_PERIOD,
        interval="1d",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    if df is None or df.empty:
        raise RuntimeError("Failed to download benchmark data")

    close = df["Close"].dropna()

    # DataFrame → Series 변환 대응
    if isinstance(close, pd.DataFrame):
        close = close.squeeze()

    if len(close) < MA_WINDOW:
        raise RuntimeError("Not enough benchmark history")

    ma200 = close.rolling(MA_WINDOW).mean()

    last_close = float(close.values[-1])
    last_ma200 = float(ma200.values[-1])

    regime = "RISK_ON" if last_close >= last_ma200 else "RISK_OFF"

    exposure = 1.0 if regime == "RISK_ON" else 0.5

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "benchmark": BENCHMARK,
        "ma_window": MA_WINDOW,
        "last_close": last_close,
        "last_ma": last_ma200,
        "regime": regime,
        "suggested_exposure": exposure,
    }

    out_path = DATA_DIR / "market_regime.json"

    save_json(out_path, out)

    print(f"Saved -> {out_path}")

    copy_to_public(out_path)


if __name__ == "__main__":
    main()