#!/usr/bin/env python3
"""
run_quantum_backtest_ae.py — AE-VQC (Amplitude Encoding) 백테스트 실험

비교 구성:
  ① Baseline          : 전통 팩터 (모멘텀+밸류), VQC 없음
  ② +AE-VQC           : Baseline + AmplitudeEmbedding VQC (4큐비트, 16팩터)
  ③ +VQC (대조군)     : Baseline + AngleEmbedding VQC (6큐비트, 6팩터)

직전 실험(quantum_backtest_result.json) 결과도 함께 출력해 4-way 비교 가능.

사전 조건:
  python quantum_signal_ae.py    # AE-VQC 학습 필수 (quantum_vqc_ae_params.pkl)
  python quantum_signal.py       # 기존 VQC 재사용 (quantum_vqc_params.pkl 이미 존재)

결과 저장: data/quantum_backtest_ae_result.json
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
    "mom12_1":    0.30,
    "mom6_1":     0.20,
    "rs_spy_6m":  0.15,
    "evebit":     0.20,
    "pb":         0.15,
}
AE_VQC_WEIGHTS = {**BASE_WEIGHTS, "quantum_ml_ae": 0.30}
VQC_WEIGHTS    = {**BASE_WEIGHTS, "quantum_ml":    0.30}


def run_experiment(
    label:       str,
    weights:     dict[str, float],
    use_vqc:     bool = False,   # AngleEmbedding VQC (quantum_ml)
    use_vqc_ae:  bool = False,   # AmplitudeEmbedding VQC (quantum_ml_ae)
    use_qubo:    bool = False,
) -> dict:
    from run_backtest_new import BacktestEngine

    t0     = time.time()
    engine = BacktestEngine(verbose=True)

    if use_vqc:
        engine.use_quantum_signal()

    if use_vqc_ae:
        engine.use_quantum_signal_ae()

    if use_qubo:
        engine.use_quantum_optimizer(top_n=15, risk_aversion=0.3, penalty=5.0, solver="auto")

    print(f"\n{'='*64}")
    print(f"[실험] {label}")
    print(f"  weights   : {weights}")
    print(f"  VQC(Angle): {use_vqc}  |  VQC(AE): {use_vqc_ae}  |  QUBO: {use_qubo}")
    print(f"{'='*64}")

    result  = engine.run(weights)
    elapsed = time.time() - t0

    print(f"\n{result.summary()}")
    print(f"  (소요: {elapsed:.1f}초)")

    return {
        "label":       label,
        "weights":     weights,
        "use_vqc":     use_vqc,
        "use_vqc_ae":  use_vqc_ae,
        "use_qubo":    use_qubo,
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


def load_prev_results() -> list[dict]:
    """quantum_backtest_result.json에서 이전 실험 결과 로드 (비교용)"""
    path = DATA_DIR / "quantum_backtest_result.json"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("experiments", [])


def print_comparison(results: list[dict], prev_results: list[dict]):
    """현재 실험 + 이전 실험 결과를 한 테이블로 출력"""
    sep   = "=" * 76
    fmt   = "{:<38} {:>8} {:>7} {:>9} {:>7}"
    dfmt  = "{:<38} {:>+8.2%} {:>7.2f} {:>+9.2%} {:>7.2f}"

    print(f"\n{sep}")
    print("비교 요약 (현재 실험)")
    print(sep)
    print(fmt.format("실험", "IS CAGR", "IS SR", "OOS CAGR", "OOS SR"))
    print("-" * 76)
    for r in results:
        print(dfmt.format(
            r["label"], r["is_cagr"], r["is_sharpe"], r["oos_cagr"], r["oos_sharpe"]
        ))

    if prev_results:
        print(f"\n{sep}")
        print("이전 실험 참조 (quantum_backtest_result.json)")
        print(sep)
        print(fmt.format("실험", "IS CAGR", "IS SR", "OOS CAGR", "OOS SR"))
        print("-" * 76)
        for r in prev_results:
            print(dfmt.format(
                r["label"], r["is_cagr"], r["is_sharpe"], r["oos_cagr"], r["oos_sharpe"]
            ))

    print(sep)


def main():
    # AE-VQC 파라미터 존재 확인
    ae_params = DATA_DIR / "quantum_vqc_ae_params.pkl"
    if not ae_params.exists():
        print(
            "[오류] quantum_vqc_ae_params.pkl 없음.\n"
            "먼저 학습을 실행하세요:\n"
            "  python quantum_signal_ae.py"
        )
        sys.exit(1)

    experiments_cfg = [
        ("① Baseline",             BASE_WEIGHTS,   False, False, False),
        ("② +AE-VQC (4q/16f AE)", AE_VQC_WEIGHTS, False, True,  False),
        ("③ +VQC (6q/6f Angle)",  VQC_WEIGHTS,    True,  False, False),
    ]

    results = []
    for label, weights, use_vqc, use_vqc_ae, use_qubo in experiments_cfg:
        try:
            res = run_experiment(label, weights, use_vqc, use_vqc_ae, use_qubo)
            results.append(res)
        except Exception as e:
            print(f"[ERROR] {label}: {e}")
            import traceback
            traceback.print_exc()

    prev = load_prev_results()
    print_comparison(results, prev)

    out_file = DATA_DIR / "quantum_backtest_ae_result.json"
    payload  = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "description":  "AE-VQC (AmplitudeEmbedding 4q/16f) vs VQC (AngleEmbedding 6q/6f) vs Baseline",
        "experiments":  results,
        "prev_ref":     prev,
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"\n[OK] 결과 저장 → {out_file}")


if __name__ == "__main__":
    main()
