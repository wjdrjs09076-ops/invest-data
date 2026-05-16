#!/usr/bin/env python3
"""
사이즈 중립 기관 크라우딩 백테스트 비교
- inst_crowding (기존, 사이즈 미통제)
- inst_crowding_neutral (log mcap 회귀 잔차, 사이즈 중립)
- AE-VQC + inst_crowding_neutral 앙상블

결과: data/neutral_crowding_result.json (compute_dsr_crowding.py에서 읽음)
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
sys.path.insert(0, str(ROOT / "scripts"))

from run_backtest_new import BacktestEngine

EXPERIMENTS = [
    ("inst_crowding (기존, 사이즈 미통제)",   {"inst_crowding": 1.0}),
    ("inst_crowding_neutral (사이즈 중립)",   {"inst_crowding_neutral": 1.0}),
    ("AE-VQC 단독",                          {"quantum_ml_ae": 1.0}),
    ("AE-VQC + neutral 앙상블",              {"quantum_ml_ae": 1.0, "inst_crowding_neutral": 1.0}),
]


def main():
    engine = BacktestEngine()
    engine.load_data()
    engine.use_quantum_signal_ae()

    rows = []
    for label, weights in EXPERIMENTS:
        print(f"\n[실험] {label}")
        r = engine.run(weights)
        gen = round(r.oos_sharpe / r.is_sharpe, 3) if r.is_sharpe > 0.01 else None
        rows.append({
            "label":      label,
            "weights":    weights,
            "IS_sharpe":  round(r.is_sharpe,  3),
            "IS_cagr":    round(r.is_cagr,    4),
            "IS_mdd":     round(r.is_mdd,     4),
            "OOS_sharpe": round(r.oos_sharpe, 3),
            "OOS_cagr":   round(r.oos_cagr,   4),
            "OOS_mdd":    round(r.oos_mdd,    4),
            "generalize": gen,
        })
        print(f"  IS SR={r.is_sharpe:.3f}  OOS SR={r.oos_sharpe:.3f}  OOS CAGR={r.oos_cagr:.1%}")

    print("\n" + "=" * 72)
    print(f"{'전략':<38} {'IS SR':>7} {'OOS SR':>7} {'일반화':>8} {'OOS CAGR':>9}")
    print("-" * 72)
    for row in rows:
        gen_s = f"{row['generalize']:.3f}" if row["generalize"] is not None else "   N/A"
        print(f"{row['label']:<38} {row['IS_sharpe']:>7.3f} {row['OOS_sharpe']:>7.3f} {gen_s:>8} {row['OOS_cagr']:>8.1%}")
    print("=" * 72)

    out_path = DATA_DIR / "neutral_crowding_result.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "results": rows,
        }, f, indent=2, ensure_ascii=False)
    print(f"\n[저장] {out_path.name}")


if __name__ == "__main__":
    main()
