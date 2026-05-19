#!/usr/bin/env python3
"""
experiment_harness.py — 실험 실행·검증·로그 하네스

사용법:
    from experiment_harness import run_experiments

    run_experiments([
        {
            "id":      "baseline_v1",
            "weights": {"mom12_1": 0.5, "evebit": 0.5},
        },
        {
            "id":      "qubo_tuned",
            "weights": {"mom12_1": 0.4, "evebit": 0.3, "pb": 0.3},
            "qubo":    {"risk_aversion": 0.05, "penalty": 3.0},
        },
        {
            "id":      "vqc_v1",
            "weights": {"mom12_1": 0.4, "evebit": 0.3, "quantum_ml": 0.3},
            "vqc":     True,
        },
    ])

config 키:
    id          : 실험 식별자 (필수, 중복 시 덮어씀)
    weights     : {factor: weight} (필수)
    qubo        : dict → QuantumPortfolioOptimizer kwargs / False (기본 비활성)
    vqc         : bool → VQC 신호 활성 여부 (기본 False)
    note        : 실험 메모 (선택)

결과:
    data/experiment_log.json 에 누적 저장
    터미널에 비교표 출력
"""
from __future__ import annotations

import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LOG_FILE = DATA_DIR / "experiment_log.json"

sys.path.insert(0, str(Path(__file__).resolve().parent))


# ─────────────────────────────────────────────────────────────
# 검증 기준
# ─────────────────────────────────────────────────────────────

def _validate(result_dict: dict) -> dict[str, str]:
    """실험 결과에 대한 자동 검증. 반환: {check_name: "PASS"/"WARN"/"FAIL"}"""
    checks: dict[str, str] = {}

    oos_sharpe = result_dict.get("oos_sharpe", 0)
    is_sharpe  = result_dict.get("is_sharpe", 0)
    oos_cagr   = result_dict.get("oos_cagr", 0)
    oos_mdd    = result_dict.get("oos_mdd", 0)

    # OOS Sharpe > 0
    checks["oos_sharpe_positive"] = "PASS" if oos_sharpe > 0 else "FAIL"

    # OOS/IS Sharpe 비율 (과적합 검사)
    ratio = oos_sharpe / is_sharpe if is_sharpe > 0 else 0
    if ratio >= 0.70:
        checks["overfit_check"] = "PASS"
    elif ratio >= 0.50:
        checks["overfit_check"] = "WARN"
    else:
        checks["overfit_check"] = "FAIL"

    # OOS MDD < -40% 경고
    checks["oos_mdd_limit"] = "WARN" if oos_mdd < -0.40 else "PASS"

    # OOS CAGR > 0
    checks["oos_cagr_positive"] = "PASS" if oos_cagr > 0 else "WARN"

    return checks


# ─────────────────────────────────────────────────────────────
# 단일 실험 실행
# ─────────────────────────────────────────────────────────────

def _run_one(cfg: dict[str, Any], engine=None) -> dict[str, Any]:
    from run_backtest_new import BacktestEngine

    exp_id  = cfg["id"]
    weights = cfg["weights"]
    qubo    = cfg.get("qubo", False)
    vqc     = cfg.get("vqc", False)
    note    = cfg.get("note", "")

    if engine is None:
        engine = BacktestEngine(verbose=True)

    if vqc:
        engine.use_quantum_signal()

    if qubo:
        kwargs = qubo if isinstance(qubo, dict) else {}
        engine.use_quantum_optimizer(**kwargs)

    print(f"\n{'='*60}")
    print(f"[실험] {exp_id}")
    if note:
        print(f"  note  : {note}")
    print(f"  weights: {weights}")
    print(f"  VQC   : {vqc}  |  QUBO: {bool(qubo)}")
    print(f"{'='*60}")

    t0     = time.time()
    result = engine.run(weights)
    elapsed = round(time.time() - t0, 1)

    print(f"\n{result.summary()}")
    print(f"  소요: {elapsed}초")

    checks = _validate({
        "oos_sharpe": result.oos_sharpe,
        "is_sharpe":  result.is_sharpe,
        "oos_cagr":   result.oos_cagr,
        "oos_mdd":    result.oos_mdd,
    })
    print(f"  검증: {checks}")

    return {
        "id":          exp_id,
        "note":        note,
        "weights":     weights,
        "use_vqc":     vqc,
        "use_qubo":    bool(qubo),
        "qubo_kwargs": qubo if isinstance(qubo, dict) else {},
        "elapsed_sec": elapsed,
        "run_at":      datetime.now(timezone.utc).isoformat(),
        "is_cagr":     result.is_cagr,
        "is_sharpe":   result.is_sharpe,
        "is_mdd":      result.is_mdd,
        "oos_cagr":    result.oos_cagr,
        "oos_sharpe":  result.oos_sharpe,
        "oos_mdd":     result.oos_mdd,
        "verdict":     result.verdict(),
        "checks":      checks,
        "is_metrics":  result.is_metrics,
        "oos_metrics": result.oos_metrics,
        "status":      "ok",
    }


# ─────────────────────────────────────────────────────────────
# 로그 관리
# ─────────────────────────────────────────────────────────────

def _load_log() -> list[dict]:
    if not LOG_FILE.exists():
        return []
    with open(LOG_FILE, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("experiments", [])


def _save_log(experiments: list[dict]):
    existing = {e["id"]: e for e in _load_log()}
    for e in experiments:
        existing[e["id"]] = e
    payload = {
        "updated_at":  datetime.now(timezone.utc).isoformat(),
        "experiments": list(existing.values()),
    }
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


# ─────────────────────────────────────────────────────────────
# 비교표 출력
# ─────────────────────────────────────────────────────────────

def _print_table(results: list[dict]):
    print("\n" + "=" * 78)
    print("실험 비교표")
    print("=" * 78)
    print(f"{'ID':<22} {'IS SR':>6} {'OOS SR':>7} {'IS CAGR':>8} {'OOS CAGR':>9} {'verdict'}")
    print("-" * 78)
    for r in results:
        if r.get("status") != "ok":
            print(f"{r['id']:<22}  ERROR: {r.get('error','')[:40]}")
            continue
        print(
            f"{r['id']:<22} "
            f"{r['is_sharpe']:>6.2f} "
            f"{r['oos_sharpe']:>7.2f} "
            f"{r['is_cagr']:>+8.2%} "
            f"{r['oos_cagr']:>+9.2%}  "
            f"{r['verdict']}"
        )
    print("=" * 78)
    print(f"결과 저장: {LOG_FILE}")


# ─────────────────────────────────────────────────────────────
# 메인 API
# ─────────────────────────────────────────────────────────────

def run_experiments(
    configs: list[dict[str, Any]],
    reuse_engine: bool = True,
) -> list[dict]:
    """
    configs 리스트의 실험을 순차 실행.

    reuse_engine=True  → 데이터를 1회만 로드 (빠름, 권장)
    reuse_engine=False → 실험마다 새 엔진 (메모리 절약)
    """
    from run_backtest_new import BacktestEngine

    engine = BacktestEngine(verbose=True) if reuse_engine else None

    results: list[dict] = []
    for cfg in configs:
        try:
            # 엔진 재사용 시 매 실험마다 양자 모듈 초기화
            if reuse_engine and engine is not None:
                engine._qubo = None
                engine._vqc  = None
            res = _run_one(cfg, engine if reuse_engine else None)
        except Exception as e:
            res = {
                "id":     cfg.get("id", "unknown"),
                "status": "error",
                "error":  str(e),
                "run_at": datetime.now(timezone.utc).isoformat(),
                "traceback": traceback.format_exc(),
            }
            print(f"[ERROR] {cfg.get('id')}: {e}")

        results.append(res)

    _save_log(results)
    _print_table(results)
    return results


# ─────────────────────────────────────────────────────────────
# 로그 조회 유틸
# ─────────────────────────────────────────────────────────────

def show_log(n: int = 10):
    """최근 n개 실험 결과 출력"""
    experiments = _load_log()
    if not experiments:
        print("[INFO] 기록된 실험 없음")
        return
    recent = sorted(experiments, key=lambda e: e.get("run_at", ""))[-n:]
    _print_table(recent)


def compare(ids: list[str]):
    """특정 ID들만 비교"""
    experiments = {e["id"]: e for e in _load_log()}
    selected = [experiments[i] for i in ids if i in experiments]
    if not selected:
        print("[INFO] 해당 ID 없음")
        return
    _print_table(selected)


if __name__ == "__main__":
    show_log()
