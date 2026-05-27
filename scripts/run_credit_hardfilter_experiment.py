#!/usr/bin/env python3
"""
run_credit_hardfilter_experiment.py

credit hard filter (score < 25 제외) 효과 실험.
랭킹 팩터가 아닌 유니버스 필터로서 credit score의 효과를 측정.

실험 설계:
  baseline             — baseline 그대로 (필터 없음)
  baseline_hardfilter  — baseline + credit hard filter (score < 25 제외)

목표: MDD 개선폭 측정, Sharpe/CAGR 비용 평가
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from run_backtest_new import BacktestEngine  # noqa: E402

BASELINE_WEIGHTS = {
    "mom12_1":     1.0,
    "institutional": 0.5,
    "insider":     0.5,
}

EXPERIMENTS = [
    {
        "id": "baseline",
        "weights": BASELINE_WEIGHTS,
        "credit_hard_filter": False,
    },
    {
        "id": "baseline_hardfilter_25",
        "weights": BASELINE_WEIGHTS,
        "credit_hard_filter": True,
        "credit_threshold": 25.0,
    },
    {
        "id": "baseline_hardfilter_30",
        "weights": BASELINE_WEIGHTS,
        "credit_hard_filter": True,
        "credit_threshold": 30.0,
    },
    {
        "id": "baseline_hardfilter_20",
        "weights": BASELINE_WEIGHTS,
        "credit_hard_filter": True,
        "credit_threshold": 20.0,
    },
]

HEADER = f"{'ID':<35} {'IS SR':>6} {'OOS SR':>7} {'IS CAGR':>9} {'OOS CAGR':>10} {'OOS MDD':>8}"
ROW_FMT = "{id:<35} {is_sr:>6.3f} {oos_sr:>7.3f} {is_cagr:>+9.2%} {oos_cagr:>+10.2%} {oos_mdd:>+8.2%}"


def main() -> None:
    engine = BacktestEngine()

    print("-" * 70)
    print(HEADER)
    print("-" * 70)

    results = []
    for cfg in EXPERIMENTS:
        res = engine.run(
            cfg["weights"],
            credit_hard_filter=cfg.get("credit_hard_filter", False),
            credit_threshold=cfg.get("credit_threshold", 25.0),
        )
        row = {
            "id":       cfg["id"],
            "is_sr":    res.is_sharpe,
            "oos_sr":   res.oos_sharpe,
            "is_cagr":  res.is_cagr,
            "oos_cagr": res.oos_cagr,
            "oos_mdd":  res.oos_mdd,
        }
        print(ROW_FMT.format(**row))
        results.append({
            "id":       cfg["id"],
            "is_sharpe":  res.is_sharpe,
            "oos_sharpe": res.oos_sharpe,
            "is_cagr":    res.is_cagr,
            "oos_cagr":   res.oos_cagr,
            "oos_mdd":    res.oos_mdd,
            "is_mdd":     res.is_mdd,
        })

    print("-" * 70)

    out_path = ROOT / "data" / "credit_hardfilter_experiment.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] 결과 저장 -> {out_path}")


if __name__ == "__main__":
    main()
