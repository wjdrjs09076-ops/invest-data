#!/usr/bin/env python3
"""
build_live_performance.py — 실전 추적 초기화 / 재구성

equity_curve_regime.json에서 LIVE_START(2026-05-04) 이후 구간을 추출해
live_performance.json을 생성(또는 재구성)한다.

run_backtest_regime.py 실행 후에 이 스크립트를 돌리면
LIVE 구간 데이터가 최신화된다.

출력:
  data/live_performance.json
  public/data/live_performance.json
"""
from __future__ import annotations

import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT            = Path(__file__).resolve().parents[1]
DATA_DIR        = ROOT / "data"
PUBLIC_DATA_DIR = ROOT.parent / "public" / "data"

LIVE_START = "2026-05-04"
STRATEGY_LABEL = "Regime Filter Strategy (MA200 + VIX + Defensive Sleeve)"


def _load_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def build() -> None:
    curve_path = DATA_DIR / "equity_curve_regime.json"
    if not curve_path.exists():
        print(f"[오류] {curve_path} 없음 — run_backtest_regime.py 먼저 실행하세요.")
        return

    raw: list[dict] = _load_json(curve_path)  # type: ignore
    if not raw:
        print("[오류] equity_curve_regime.json 비어있음")
        return

    # LIVE_START 이후 레코드 필터링
    live_records = [r for r in raw if r["date"] >= LIVE_START]
    if not live_records:
        print(f"[WARN] {LIVE_START} 이후 데이터 없음 (curve 마지막: {raw[-1]['date']})")
        return

    # equity 재기준화: LIVE_START 첫 거래일 = 1.0
    base = float(live_records[0]["strategy"])
    running_max = base

    daily: list[dict] = []
    for r in live_records:
        strat_raw = float(r["strategy"])
        equity    = strat_raw / base
        bench_raw = float(r.get("benchmark", base)) / float(raw[0].get("benchmark", 1.0))
        bench_eq  = bench_raw / (float(live_records[0].get("benchmark", 1.0)) / float(raw[0].get("benchmark", 1.0)))

        running_max = max(running_max, strat_raw)
        dd = strat_raw / running_max - 1.0

        prev = daily[-1] if daily else None
        dr = float(equity / prev["equity"] - 1.0) if prev else 0.0

        daily.append({
            "date":          r["date"],
            "equity":        round(equity, 6),
            "daily_return":  round(dr, 6),
            "drawdown":      round(dd, 6),
            "benchmark":     round(bench_eq, 6),
            "regime":        r.get("regime_bucket", "risk_on"),
            "stock_exposure": float(r.get("stock_exposure", 1.0)),
        })

    # 요약 통계
    total_ret = daily[-1]["equity"] - 1.0
    days = len(daily)
    years = days / 252
    cagr = daily[-1]["equity"] ** (1 / years) - 1.0 if years > 0 else 0.0
    rets = [d["daily_return"] for d in daily[1:]]
    vol  = (sum(r**2 for r in rets) / len(rets) - (sum(rets) / len(rets))**2) ** 0.5 * math.sqrt(252) if rets else 0.0
    sharpe = cagr / vol if vol > 0 else 0.0
    mdd = min(d["drawdown"] for d in daily)

    # 현재 포트폴리오 (live_state.json에서 로드)
    live_state = _load_json(DATA_DIR / "live_state.json") or {}

    payload = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "live_start":     LIVE_START,
        "strategy_label": STRATEGY_LABEL,
        "summary": {
            "total_return": round(total_ret, 6),
            "days":         days,
            "cagr":         round(cagr, 6),
            "sharpe":       round(sharpe, 4),
            "mdd":          round(mdd, 6),
            "last_date":    daily[-1]["date"],
            "last_equity":  daily[-1]["equity"],
        },
        "current_portfolio": live_state.get("portfolio_weights", {}),
        "regime_bucket":     live_state.get("regime_bucket", "unknown"),
        "stock_exposure":    live_state.get("stock_exposure", 1.0),
        "daily":             daily,
    }

    out = DATA_DIR / "live_performance.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"[OK] 저장 → {out}  ({days}일, 총수익={total_ret:+.2%}, Sharpe={sharpe:.2f})")

    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(out, PUBLIC_DATA_DIR / "live_performance.json")
    print(f"[OK] 복사 → {PUBLIC_DATA_DIR / 'live_performance.json'}")


if __name__ == "__main__":
    build()
