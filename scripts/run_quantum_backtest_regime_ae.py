#!/usr/bin/env python3
"""
run_quantum_backtest_regime_ae.py — AE-VQC 리짐 조건부 백테스트

레짐에 따라 quantum_ml_ae 가중치를 동적으로 조절:
  ① Baseline         : 전통 팩터 고정 (quantum 없음)
  ② AE-VQC Fixed     : quantum_ml_ae 0.30 항상 고정 (이전 결과 재확인)
  ③ AE-VQC Adaptive  : risk_on=0.30 / mid=0.15 / risk_off=0.00
  ④ AE-VQC Aggressive: risk_on=0.50 / mid=0.20 / risk_off=0.00

가설: 모멘텀 성격의 AE-VQC 신호는 추세장(risk_on)에서 알파가 집중되고,
      약세장(risk_off)에서는 오히려 노이즈가 될 수 있다.

결과 저장: data/quantum_backtest_regime_ae_result.json
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
sys.path.insert(0, str(Path(__file__).resolve().parent))

BASE_WEIGHTS: dict[str, float] = {
    "mom12_1":   0.30,
    "mom6_1":    0.20,
    "rs_spy_6m": 0.15,
    "evebit":    0.20,
    "pb":        0.15,
}


def ae_adaptive_fn(regime: str) -> dict[str, float]:
    """리짐별 AE-VQC 가중치 — 추세장 집중, 약세장 제거"""
    base = dict(BASE_WEIGHTS)
    if regime == "risk_on":
        base["quantum_ml_ae"] = 0.30
    elif regime == "mid":
        base["quantum_ml_ae"] = 0.15
    # risk_off: quantum 없음
    return base


def ae_aggressive_fn(regime: str) -> dict[str, float]:
    """리짐별 AE-VQC 가중치 — risk_on에서 최대한 활용"""
    base = dict(BASE_WEIGHTS)
    if regime == "risk_on":
        base["quantum_ml_ae"] = 0.50
    elif regime == "mid":
        base["quantum_ml_ae"] = 0.20
    # risk_off: quantum 없음
    return base


def run_fixed(label: str, weights: dict[str, float], use_vqc_ae: bool = False) -> dict:
    from run_backtest_new import BacktestEngine

    t0     = time.time()
    engine = BacktestEngine(verbose=True)
    if use_vqc_ae:
        engine.use_quantum_signal_ae()

    print(f"\n{'='*64}")
    print(f"[실험] {label}")
    print(f"  weights  : {weights}")
    print(f"  AE-VQC   : {use_vqc_ae}  (fixed weight)")
    print(f"{'='*64}")

    result  = engine.run(weights)
    elapsed = time.time() - t0
    print(f"\n{result.summary()}")
    print(f"  (소요: {elapsed:.1f}초)")

    return _pack(label, result, elapsed, mode="fixed", ae=use_vqc_ae)


def run_adaptive(label: str, weights_fn, desc: str) -> dict:
    from run_backtest_new import BacktestEngine

    t0     = time.time()
    engine = BacktestEngine(verbose=True)
    engine.use_quantum_signal_ae()

    print(f"\n{'='*64}")
    print(f"[실험] {label}")
    print(f"  모드: {desc}")
    print(f"{'='*64}")

    result  = engine.run_adaptive(weights_fn)
    elapsed = time.time() - t0
    print(f"\n{result.summary()}")
    print(f"  (소요: {elapsed:.1f}초)")

    return _pack(label, result, elapsed, mode="adaptive", ae=True)


def _pack(label: str, result, elapsed: float, mode: str, ae: bool) -> dict:
    return {
        "label":       label,
        "mode":        mode,
        "use_vqc_ae":  ae,
        "elapsed_sec": round(elapsed, 1),
        "is_cagr":     result.is_cagr,
        "is_sharpe":   result.is_sharpe,
        "is_mdd":      result.is_mdd,
        "oos_cagr":    result.oos_cagr,
        "oos_sharpe":  result.oos_sharpe,
        "oos_mdd":     result.oos_mdd,
        "verdict":     result.verdict(),
        "is_metrics":  result.is_metrics,
        "oos_metrics": result.oos_metrics,
    }


def load_prev() -> list[dict]:
    path = DATA_DIR / "quantum_backtest_ae_result.json"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f).get("experiments", [])


def print_comparison(results: list[dict], prev: list[dict]) -> None:
    sep  = "=" * 80
    fmt  = "{:<40} {:>8} {:>7} {:>9} {:>7}"
    dfmt = "{:<40} {:>+8.2%} {:>7.2f} {:>+9.2%} {:>7.2f}"

    print(f"\n{sep}")
    print("리짐 조건부 AE-VQC 비교 (이번 실험)")
    print(sep)
    print(fmt.format("실험", "IS CAGR", "IS SR", "OOS CAGR", "OOS SR"))
    print("-" * 80)
    for r in results:
        print(dfmt.format(r["label"], r["is_cagr"], r["is_sharpe"], r["oos_cagr"], r["oos_sharpe"]))

    if prev:
        print(f"\n{sep}")
        print("이전 실험 참조 (quantum_backtest_ae_result.json)")
        print(sep)
        print(fmt.format("실험", "IS CAGR", "IS SR", "OOS CAGR", "OOS SR"))
        print("-" * 80)
        for r in prev:
            print(dfmt.format(r["label"], r["is_cagr"], r["is_sharpe"], r["oos_cagr"], r["oos_sharpe"]))

    print(sep)


def main():
    ae_params = DATA_DIR / "quantum_vqc_ae_params.pkl"
    if not ae_params.exists():
        print("[오류] quantum_vqc_ae_params.pkl 없음. quantum_signal_ae.py 먼저 실행.")
        sys.exit(1)

    results: list[dict] = []

    experiments = [
        ("fixed",      "① Baseline",           BASE_WEIGHTS,       False),
        ("fixed",      "② AE-VQC Fixed(0.30)", {**BASE_WEIGHTS, "quantum_ml_ae": 0.30}, True),
    ]

    for mode, label, weights, use_ae in experiments:
        try:
            res = run_fixed(label, weights, use_vqc_ae=use_ae)
            results.append(res)
        except Exception as e:
            print(f"[ERROR] {label}: {e}")
            import traceback; traceback.print_exc()

    adaptive_experiments = [
        ("③ AE-VQC Adaptive (0.30/0.15/0.00)",   ae_adaptive_fn,   "risk_on=0.30 / mid=0.15 / risk_off=0.00"),
        ("④ AE-VQC Aggressive (0.50/0.20/0.00)",  ae_aggressive_fn, "risk_on=0.50 / mid=0.20 / risk_off=0.00"),
    ]

    for label, fn, desc in adaptive_experiments:
        try:
            res = run_adaptive(label, fn, desc)
            results.append(res)
        except Exception as e:
            print(f"[ERROR] {label}: {e}")
            import traceback; traceback.print_exc()

    prev = load_prev()
    print_comparison(results, prev)

    out_file = DATA_DIR / "quantum_backtest_regime_ae_result.json"
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "description":  "AE-VQC 리짐 조건부 가중치 실험 (risk_on/mid/risk_off별 quantum 가중치 조절)",
        "regime_weights": {
            "adaptive":   {"risk_on": 0.30, "mid": 0.15, "risk_off": 0.00},
            "aggressive": {"risk_on": 0.50, "mid": 0.20, "risk_off": 0.00},
        },
        "experiments": results,
        "prev_ref":    prev,
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"\n[OK] 결과 저장 → {out_file}")


if __name__ == "__main__":
    main()
