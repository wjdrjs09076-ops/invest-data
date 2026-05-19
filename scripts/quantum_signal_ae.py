#!/usr/bin/env python3
"""
quantum_signal_ae.py — Amplitude Encoding VQC 알파 신호 생성기

quantum_signal.py (AngleEmbedding, 6큐비트, 6팩터)의 대안 구현.
AmplitudeEmbedding을 사용해 4큐비트로 FACTOR_UNIVERSE 전체(16팩터)를 인코딩.

AngleEmbedding vs AmplitudeEmbedding:
  AngleEmbedding    : 1큐비트 = 1팩터 (6큐비트 → 6팩터)
  AmplitudeEmbedding: 2^n 진폭 = n 팩터 (4큐비트 → 2^4=16팩터)

회로 구조:
  AmplitudeEmbedding(16팩터 → 4큐비트)
  → [Rot(phi,theta,omega) × 4큐비트 + CNOT 순환 얽힘] × 3레이어
  → PauliZ(0) 측정 → sigmoid → [0,1] 신호

파라미터 파일 (quantum_signal.py와 별도):
  data/quantum_vqc_ae_params.pkl   학습된 회로 파라미터
  data/quantum_vqc_ae_norm.pkl     z-score 정규화 mu/std
  data/quantum_vqc_ae_meta.json    메타 정보

사용 예:
  # 학습
  python quantum_signal_ae.py

  # BacktestEngine 연동 (run_backtest_new.py에 quantum_ml_ae 팩터 등록 후)
  engine.use_quantum_signal_ae()
  result = engine.run({..., "quantum_ml_ae": 0.3})
"""
from __future__ import annotations

import json
import pickle
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

ROOT        = Path(__file__).resolve().parents[1]
DATA_DIR    = ROOT / "data"
PARAMS_FILE = DATA_DIR / "quantum_vqc_ae_params.pkl"
NORM_FILE   = DATA_DIR / "quantum_vqc_ae_norm.pkl"
META_FILE   = DATA_DIR / "quantum_vqc_ae_meta.json"

# ─────────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────────
DEFAULT_N_QUBITS = 4        # 2^4 = 16 진폭 슬롯 → 16팩터 수용
DEFAULT_N_LAYERS = 3        # AngleEmbedding(2레이어)보다 1 더 — 큐비트 감소분 보완
DEFAULT_N_EPOCHS = 60
DEFAULT_LR       = 0.05
DEFAULT_BATCH    = 32

# FACTOR_UNIVERSE 전체 16팩터 (quantum_ml 제외) — 순서 고정
AE_INPUT_FEATURES: list[str] = [
    # 가격 모멘텀
    "mom12_1",
    "mom9_1",
    "mom6_1",
    "mom3_1",
    "mom1",
    # 상대강도 vs SPY
    "rs_spy_12m",
    "rs_spy_6m",
    "rs_spy_3m",
    # 밸류에이션
    "evebit",
    "evebitda",
    "pb",
    "pe",
    "ps",
    # 퀄리티 (zscore → IS 기간 데이터 부족으로 IC=0, inst_crowding_neutral로 교체)
    "inst_crowding_neutral",
    # 수급
    "institutional",
    "insider",
]
assert len(AE_INPUT_FEATURES) == 2 ** DEFAULT_N_QUBITS, (
    f"AE_INPUT_FEATURES 길이({len(AE_INPUT_FEATURES)})가 "
    f"2^DEFAULT_N_QUBITS({2**DEFAULT_N_QUBITS})와 달라야 합니다."
)


# ═════════════════════════════════════════════════════════════
# 회로 정의
# ═════════════════════════════════════════════════════════════

def _make_ae_circuit(n_qubits: int, n_layers: int):
    """AmplitudeEmbedding 기반 QNode 반환 (CPU 시뮬레이터)"""
    try:
        import pennylane as qml
    except ImportError:
        raise ImportError("pip install pennylane 를 먼저 실행하세요.")

    dev = qml.device("default.qubit", wires=n_qubits)

    @qml.qnode(dev, interface="autograd")
    def circuit(params: np.ndarray, features: np.ndarray) -> float:
        """
        params  : (n_layers, n_qubits, 3)  — Rot 파라미터
        features: (2^n_qubits,)            — z-score 정규화된 팩터 벡터
                                             PennyLane이 자동으로 unit-norm 변환
        """
        # Amplitude Encoding: 16개 실수값 → 4큐비트 양자상태 진폭
        qml.AmplitudeEmbedding(
            features,
            wires=range(n_qubits),
            normalize=True,   # unit-norm 자동 정규화
            pad_with=0.0,     # features 길이 < 2^n_qubits 시 패딩
        )

        # 변분 레이어: Rot + CNOT 순환 얽힘
        for layer in range(n_layers):
            for q in range(n_qubits):
                qml.Rot(
                    params[layer, q, 0],
                    params[layer, q, 1],
                    params[layer, q, 2],
                    wires=q,
                )
            for q in range(n_qubits):
                qml.CNOT(wires=[q, (q + 1) % n_qubits])

        return qml.expval(qml.PauliZ(0))

    return circuit


# ═════════════════════════════════════════════════════════════
# 학습기
# ═════════════════════════════════════════════════════════════

class AEVQCTrainer:
    """
    AmplitudeEmbedding VQC 학습기.

    학습 데이터:
      X : (N, 2^n_qubits) — z-score 정규화된 팩터 행렬
      y : (N,)            — 레이블 (1=상위 50% 수익률, 0=하위 50%)
    """

    def __init__(
        self,
        n_qubits: int   = DEFAULT_N_QUBITS,
        n_layers: int   = DEFAULT_N_LAYERS,
        n_epochs: int   = DEFAULT_N_EPOCHS,
        lr:       float = DEFAULT_LR,
        batch:    int   = DEFAULT_BATCH,
        verbose:  bool  = True,
        seed:     int   = 42,
    ):
        self.n_qubits = n_qubits
        self.n_layers = n_layers
        self.n_epochs = n_epochs
        self.lr       = lr
        self.batch    = batch
        self.verbose  = verbose
        self.seed     = seed
        self.params_  = None
        self._circuit = None

    def _get_circuit(self):
        if self._circuit is None:
            self._circuit = _make_ae_circuit(self.n_qubits, self.n_layers)
        return self._circuit

    def _loss(self, params: np.ndarray, X: np.ndarray, y: np.ndarray):
        import pennylane.numpy as pnp
        circuit = self._get_circuit()
        total   = pnp.array(0.0)
        for xi, yi in zip(X, y):
            out  = circuit(params, xi)
            prob = 1.0 / (1.0 + pnp.exp(-out))
            prob = pnp.clip(prob, 1e-7, 1.0 - 1e-7)
            total = total + -(yi * pnp.log(prob) + (1 - yi) * pnp.log(1 - prob))
        return total / len(X)

    def fit(self, X: np.ndarray, y: np.ndarray) -> "AEVQCTrainer":
        """
        X : (N, 2^n_qubits) — z-score 정규화 완료된 팩터
        y : (N,) binary {0, 1}
        """
        try:
            import pennylane as qml
            from pennylane import numpy as pnp
        except ImportError:
            raise ImportError("pip install pennylane 를 먼저 실행하세요.")

        n_features = 2 ** self.n_qubits
        assert X.shape[1] == n_features, (
            f"피처 수({X.shape[1]})가 2^n_qubits({n_features})와 다릅니다."
        )

        rng    = np.random.default_rng(self.seed)
        params = pnp.array(
            rng.uniform(-np.pi, np.pi, (self.n_layers, self.n_qubits, 3)),
            requires_grad=True,
        )
        opt = qml.AdamOptimizer(stepsize=self.lr)
        N   = len(X)

        if self.verbose:
            print(
                f"[AE-VQC] 학습 시작: {N}개 샘플 / "
                f"{self.n_qubits}큐비트 (2^{self.n_qubits}={n_features}팩터) / "
                f"{self.n_layers}레이어 / {self.n_epochs}에폭"
            )

        for epoch in range(self.n_epochs):
            idx        = rng.permutation(N)
            epoch_loss = 0.0
            n_batches  = 0

            for start in range(0, N, self.batch):
                batch_idx = idx[start : start + self.batch]
                Xb = X[batch_idx]
                yb = y[batch_idx]

                def batch_loss(p):
                    return self._loss(p, Xb, yb)

                params, loss_val = opt.step_and_cost(batch_loss, params)
                epoch_loss += float(loss_val)
                n_batches  += 1

            if self.verbose and (epoch % 10 == 0 or epoch == self.n_epochs - 1):
                avg_loss = epoch_loss / max(n_batches, 1)
                preds    = self._predict_raw(np.array(params), X)
                acc      = float(np.mean((preds >= 0.5).astype(int) == y.astype(int)))
                print(
                    f"  epoch {epoch+1:3d}/{self.n_epochs}  "
                    f"loss={avg_loss:.4f}  train_acc={acc:.3f}"
                )

        self.params_ = np.array(params)
        return self

    def _predict_raw(self, params: np.ndarray, X: np.ndarray) -> np.ndarray:
        circuit = self._get_circuit()
        return np.array([
            float(1.0 / (1.0 + np.exp(-float(circuit(params, xi))))) for xi in X
        ])

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if self.params_ is None:
            raise RuntimeError("먼저 fit()을 호출하세요.")
        return self._predict_raw(self.params_, X)

    def save(
        self,
        params_path: Path          = PARAMS_FILE,
        norm_mu:     np.ndarray | None = None,
        norm_std:    np.ndarray | None = None,
        norm_path:   Path          = NORM_FILE,
        meta_path:   Path          = META_FILE,
    ):
        if self.params_ is None:
            raise RuntimeError("먼저 fit()을 호출하세요.")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(params_path, "wb") as f:
            pickle.dump(self.params_, f)
        if norm_mu is not None and norm_std is not None:
            with open(norm_path, "wb") as f:
                pickle.dump({"mu": norm_mu, "std": norm_std}, f)
            print(f"[AE-VQC] 정규화 파라미터 저장 → {norm_path}")
        meta = {
            "n_qubits":       self.n_qubits,
            "n_layers":       self.n_layers,
            "encoding":       "amplitude",
            "n_features":     2 ** self.n_qubits,
            "input_features": AE_INPUT_FEATURES,
            "saved_at":       pd.Timestamp.now().isoformat(),
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
        print(f"[AE-VQC] 저장 완료 → {params_path}")


# ═════════════════════════════════════════════════════════════
# 추론기 (BacktestEngine에서 호출)
# ═════════════════════════════════════════════════════════════

class AEVQCSignal:
    """
    학습된 AE-VQC 파라미터로 추론.
    BacktestEngine의 _raw_signal()에서 type="quantum_ml_ae"일 때 호출.
    """

    def __init__(self):
        self._params:   np.ndarray | None = None
        self._mu:       np.ndarray | None = None
        self._std:      np.ndarray | None = None
        self._circuit                     = None
        self._n_qubits: int               = DEFAULT_N_QUBITS
        self._n_layers: int               = DEFAULT_N_LAYERS
        self._loaded:   bool              = False

    def load(
        self,
        params_path: Path = PARAMS_FILE,
        norm_path:   Path = NORM_FILE,
        meta_path:   Path = META_FILE,
    ) -> bool:
        if self._loaded:
            return True
        if not params_path.exists():
            warnings.warn(
                "[AE-VQC] quantum_vqc_ae_params.pkl 없음 — "
                "quantum_signal_ae.py로 먼저 학습하세요."
            )
            return False
        with open(params_path, "rb") as f:
            raw = pickle.load(f)
        # 앙상블(리스트) 또는 단일 파라미터 모두 지원
        if isinstance(raw, list):
            self._params_list = [np.array(p) for p in raw]
            self._params      = self._params_list[0]
        else:
            self._params_list = [np.array(raw)]
            self._params      = self._params_list[0]

        if norm_path.exists():
            with open(norm_path, "rb") as f:
                norm      = pickle.load(f)
            self._mu  = norm["mu"]
            self._std = norm["std"]
        if meta_path.exists():
            with open(meta_path, encoding="utf-8") as f:
                meta           = json.load(f)
            self._n_qubits = meta.get("n_qubits", DEFAULT_N_QUBITS)
            self._n_layers = meta.get("n_layers", DEFAULT_N_LAYERS)
        self._circuit = _make_ae_circuit(self._n_qubits, self._n_layers)
        self._loaded  = True
        n_members = len(self._params_list)
        print(
            f"[AE-VQC] 로드 완료 ("
            f"{self._n_qubits}큐비트 / "
            f"2^{self._n_qubits}={2**self._n_qubits}팩터 / "
            f"{self._n_layers}레이어 / 앙상블={n_members}개)"
        )
        return True

    def predict(self, feature_dict: dict[str, float | None]) -> float | None:
        """
        feature_dict : {factor_name: raw_value or None}
        반환         : 0.0 ~ 1.0 (높을수록 양호한 신호), 유효 팩터 부족 시 None
        앙상블 학습 시 각 회로 출력의 평균값 반환.
        """
        if not self._loaded:
            return None
        vec = self._make_feature_vector(feature_dict)
        if vec is None:
            return None
        try:
            probs = []
            for p in self._params_list:
                out  = self._circuit(p, vec)
                prob = float(1.0 / (1.0 + np.exp(-float(out))))
                probs.append(prob)
            return float(np.mean(probs))
        except Exception:
            return None

    def _make_feature_vector(
        self, feature_dict: dict[str, float | None]
    ) -> np.ndarray | None:
        """
        팩터값 → z-score 정규화 벡터
        결측 팩터는 정규화 후 0.0(평균)으로 처리.
        PennyLane AmplitudeEmbedding이 unit-norm으로 자동 변환.
        """
        vec     = []
        n_valid = 0
        for fname in AE_INPUT_FEATURES:
            v = feature_dict.get(fname)
            vec.append(0.0 if v is None else float(v))
            if v is not None:
                n_valid += 1

        if n_valid < len(AE_INPUT_FEATURES) // 2:
            return None

        arr = np.array(vec, dtype=float)

        if self._mu is not None and self._std is not None:
            arr = (arr - self._mu) / (self._std + 1e-8)
        else:
            # 저장된 norm이 없을 경우 폴백 (학습 시와 다를 수 있으므로 경고)
            warnings.warn("[AE-VQC] norm 파일 없음 — tanh 폴백 사용 (추론 품질 저하 가능)")
            arr = np.tanh(arr / 2.0)

        return arr


# ═════════════════════════════════════════════════════════════
# 학습 파이프라인
# ═════════════════════════════════════════════════════════════

def normalize_features(X: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Z-score 정규화. mu, std 반환 → 추론 시 재사용. NaN-aware."""
    mu  = np.nanmean(X, axis=0)
    std = np.nanstd(X, axis=0) + 1e-8
    return np.nan_to_num((X - mu) / std, nan=0.0), mu, std


def build_training_data_ae(
    engine,
    forward_days: int = 21,
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """
    IS 기간 리밸런싱 스냅샷에서 AE용 16팩터 학습 데이터 생성.
    quantum_signal.py의 build_training_data와 동일한 구조,
    INPUT_FEATURES를 AE_INPUT_FEATURES(16개)로 교체한 버전.
    """
    from run_backtest_new import (
        IS_START, IS_END, _monthly_dates, _reconstruct_universe,
        MIN_HISTORY, FACTOR_UNIVERSE,
    )

    engine.load_data()

    is_dates = engine.trading_dates[
        (engine.trading_dates >= pd.Timestamp(IS_START)) &
        (engine.trading_dates <= pd.Timestamp(IS_END))
    ]
    rebal_dates = _monthly_dates(is_dates)

    valid_features = [f for f in AE_INPUT_FEATURES if f in FACTOR_UNIVERSE]
    norm_w = {k: 1.0 / len(valid_features) for k in valid_features}

    X_list, y_list, t_list = [], [], []

    is_end_ts = pd.Timestamp(IS_END)

    for rebal_date in rebal_dates:
        future_dates = engine.trading_dates[engine.trading_dates > rebal_date]
        if len(future_dates) < forward_days:
            continue
        future_date = future_dates[forward_days - 1]
        if future_date > is_end_ts:   # OOS 라벨 오염 방지
            continue

        members = _reconstruct_universe(rebal_date, engine.universe, engine.events)
        sliced: dict[str, pd.Series] = {}
        for t in members:
            ps = engine.price_map.get(t)
            if ps is not None:
                ps2 = ps[ps.index <= rebal_date]
                if len(ps2) >= MIN_HISTORY:
                    sliced[t] = ps2

        if not sliced:
            continue

        scores = engine._score_all(
            list(sliced.keys()), sliced,
            engine.bench[engine.bench.index <= rebal_date],
            rebal_date, norm_w,
        )

        forward_rets: dict[str, float] = {}
        for t in scores:
            ps = engine.price_map.get(t)
            if ps is None:
                continue
            p_now = ps[ps.index <= rebal_date]
            p_fut = ps[ps.index <= future_date]
            if p_now.empty or p_fut.empty:
                continue
            forward_rets[t] = float(p_fut.iloc[-1] / p_now.iloc[-1] - 1.0)

        if not forward_rets:
            continue

        median_ret = np.median(list(forward_rets.values()))

        for t, fwd_ret in forward_rets.items():
            s       = scores.get(t, {})
            row     = []
            n_valid = 0
            for fname in AE_INPUT_FEATURES:
                fval = s.get("factors", {}).get(fname)
                row.append(np.nan if fval is None else float(fval))
                if fval is not None:
                    n_valid += 1

            if n_valid < len(AE_INPUT_FEATURES) // 2:
                continue

            X_list.append(row)
            y_list.append(1 if fwd_ret >= median_ret else 0)
            t_list.append(t)

    X = np.array(X_list, dtype=float)
    y = np.array(y_list, dtype=int)
    print(f"[AE-VQC DATA] {len(X)}개 샘플, 클래스 비율: {y.mean():.3f}")
    return X, y, t_list


def train_and_save_ae(engine=None):
    """IS 기간 데이터로 AE-VQC 학습 후 파라미터 저장"""
    if engine is None:
        from run_backtest_new import BacktestEngine
        engine = BacktestEngine()

    print("[AE-VQC] 학습 데이터 생성 중...")
    X_raw, y, _ = build_training_data_ae(engine)

    if len(X_raw) < 50:
        print(f"[AE-VQC] 학습 샘플 부족({len(X_raw)}) — 종료")
        return

    MAX_SAMPLES = 300
    if len(X_raw) > MAX_SAMPLES:
        rng_sub = np.random.default_rng(42)
        idx_sub = rng_sub.choice(len(X_raw), MAX_SAMPLES, replace=False)
        X_raw   = X_raw[idx_sub]
        y       = y[idx_sub]
        print(f"[AE-VQC] CPU 한계로 {MAX_SAMPLES}개로 서브샘플링")

    X, mu, std = normalize_features(X_raw)

    trainer = AEVQCTrainer(
        n_qubits = DEFAULT_N_QUBITS,
        n_layers = DEFAULT_N_LAYERS,
        n_epochs = DEFAULT_N_EPOCHS,
        verbose  = True,
    )
    trainer.fit(X, y)
    trainer.save(norm_mu=mu, norm_std=std)
    print("[AE-VQC] 학습 완료.")


if __name__ == "__main__":
    train_and_save_ae()
