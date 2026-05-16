#!/usr/bin/env python3
"""
inst_crowding / inst_crowding_neutral DSR 검증
기존 실험 풀(N개)에 순서대로 추가해 다중검정 보정 후 유의성 판단.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
from scipy import stats

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

_EMAX_N01 = {1: 0.0, 2: 0.564, 3: 0.846, 4: 1.029,
             5: 1.163, 6: 1.267, 7: 1.352, 8: 1.424,
             9: 1.485, 10: 1.539, 11: 1.586, 12: 1.629}

def _emax_n01(n: int) -> float:
    if n in _EMAX_N01:
        return _EMAX_N01[n]
    return math.sqrt(2 * math.log(n) - math.log(math.log(n)) - math.log(4 * math.pi))

def psr(sr_ann: float, sr_star_ann: float, T: int,
        skew: float = 0.0, kurt: float = 3.0) -> float:
    sr_pp      = sr_ann / math.sqrt(252)
    sr_star_pp = sr_star_ann / math.sqrt(252)
    var_sr = (1 - skew * sr_pp + (kurt - 1) / 4 * sr_pp ** 2) / (T - 1)
    if var_sr <= 0:
        return float("nan")
    return float(stats.norm.cdf((sr_pp - sr_star_pp) / math.sqrt(var_sr)))

def sr_star_from_list(sr_list: list[float]) -> float:
    n   = len(sr_list)
    mu  = float(np.mean(sr_list))
    sig = float(np.std(sr_list, ddof=1)) if n > 1 else 0.0
    return mu + sig * _emax_n01(n)

def _flag(d: float) -> str:
    return "OK" if d >= 0.95 else ("~~" if d >= 0.80 else "NO")


def load_prior_experiments() -> list[dict]:
    results = []
    for fname in ["quantum_backtest_result.json",
                  "quantum_backtest_ae_result.json",
                  "quantum_backtest_regime_ae_result.json",
                  "ml_comparison_result.json"]:
        p = DATA_DIR / fname
        if not p.exists():
            continue
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        for e in d.get("experiments", []):
            key = (e["label"], round(e["oos_sharpe"], 3))
            if not any((x["label"], round(x["oos_sharpe"], 3)) == key for x in results):
                results.append(e)
    return results


def check_signal(label: str, sr: float, pool_srs: list[float], T_OOS: int) -> tuple[float, float, int]:
    all_srs     = pool_srs + [sr]
    sr_star_val = sr_star_from_list(all_srs)
    dsr_val     = psr(sr, sr_star_val, T_OOS, skew=-0.5, kurt=5.0)
    psr0_val    = psr(sr, 0.0,         T_OOS, skew=-0.5, kurt=5.0)
    N           = len(all_srs)

    print(f"\n[{label}]")
    print(f"  OOS SR={sr:.3f}  N={N}  SR*={sr_star_val:.3f}  PSR(0)={psr0_val:.4f}  DSR={dsr_val:.4f}  {_flag(dsr_val)}")
    print(f"  감도 분석 (skew / kurt / DSR):")
    for skw in [-1.0, -0.5, 0.0]:
        row = "    "
        for krt in [3.0, 5.0, 7.0]:
            d = psr(sr, sr_star_val, T_OOS, skew=skw, kurt=krt)
            row += f"skew={skw:.1f}/kurt={krt:.0f}: {d:.4f}({_flag(d)})  "
        print(row)

    return dsr_val, sr_star_val, N


def main():
    T_OOS = 1610  # 2020-01-01 ~ 2026-05-11 거래일 수

    prior     = load_prior_experiments()
    prior_srs = [e["oos_sharpe"] for e in prior]

    # inst_crowding 결과 (45일 lag 보정 버전)
    crowding_path = DATA_DIR / "inst_crowding_result.json"
    with open(crowding_path, encoding="utf-8") as f:
        crowding_data = json.load(f)
    crowding_sr = next(
        r["OOS_sharpe"] for r in crowding_data["results"]
        if r["name"] == "inst_crowding_only"
    )

    # inst_crowding_neutral OOS SR (run_neutral_crowding_test.py 결과)
    neutral_path = DATA_DIR / "neutral_crowding_result.json"
    if not neutral_path.exists():
        print("[ERROR] neutral_crowding_result.json 없음.")
        print("  먼저 실행: python scripts/run_neutral_crowding_test.py")
        return
    with open(neutral_path, encoding="utf-8") as f:
        neutral_data = json.load(f)
    neutral_sr = next(
        r["OOS_sharpe"] for r in neutral_data["results"]
        if "neutral" in r["label"] and "VQC" not in r["label"]
    )

    print("=" * 72)
    print("Deflated Sharpe Ratio - inst_crowding / inst_crowding_neutral")
    print("=" * 72)
    print(f"\n기존 실험 풀 ({len(prior)}개):")
    for e in prior:
        print(f"  {e['label']:<44} OOS SR={e['oos_sharpe']:.3f}")

    # 검증 순서: crowding 먼저, neutral은 crowding 포함 풀로 검증
    dsr_cr, sr_star_cr, N_cr = check_signal(
        "inst_crowding (사이즈 미통제)", crowding_sr, prior_srs, T_OOS
    )
    dsr_n, sr_star_n, N_n = check_signal(
        "inst_crowding_neutral (사이즈 중립)", neutral_sr,
        prior_srs + [crowding_sr], T_OOS
    )

    print("\n" + "=" * 72)
    print(f"{'신호':<38} {'OOS SR':>7} {'N':>4} {'SR*':>7} {'DSR':>8} {'판정':>5}")
    print("-" * 72)
    print(f"{'inst_crowding':<38} {crowding_sr:>7.3f} {N_cr:>4} {sr_star_cr:>7.3f} {dsr_cr:>8.4f}  {_flag(dsr_cr)}")
    print(f"{'inst_crowding_neutral':<38} {neutral_sr:>7.3f} {N_n:>4} {sr_star_n:>7.3f} {dsr_n:>8.4f}  {_flag(dsr_n)}")
    print("=" * 72)
    print("\n[해석] PSR(SR*) >= 0.95 -> OK, 0.80~0.95 -> 경계(~~), <0.80 -> NO")


if __name__ == "__main__":
    main()
