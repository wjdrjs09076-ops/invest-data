#!/usr/bin/env python3
"""
alpaca_rebalance.py — Alpaca Paper/Live 자동 리밸런싱 실행기

동작 흐름:
  1. live_state.json 에서 목표 포트폴리오 비중 로드
  2. Alpaca 현재 포지션과 계좌 잔고 조회
  3. 리밸런싱 필요 여부 판단 (월 1회 OR 드리프트 임계값 초과)
  4. 매도 → 매수 순서로 시장가 주문 실행
  5. 결과를 data/alpaca_trade_log.json 에 누적 기록

환경변수:
  ALPACA_API_KEY     Alpaca API Key ID
  ALPACA_SECRET_KEY  Alpaca Secret Key
  ALPACA_BASE_URL    기본값: https://paper-api.alpaca.markets (paper)
                     실전: https://api.alpaca.markets
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, date
from pathlib import Path

ROOT     = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

# ─── 설정 ──────────────────────────────────────────────────────
BASE_URL        = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
API_KEY         = os.environ.get("ALPACA_API_KEY", "")
SECRET_KEY      = os.environ.get("ALPACA_SECRET_KEY", "")
DRIFT_THRESHOLD = float(os.environ.get("DRIFT_THRESHOLD", "0.05"))   # 5%p 초과 시 즉시 리밸런싱
MIN_ORDER_USD   = 1.0       # 이보다 작은 주문은 무시 (수수료 절감)
DRY_RUN         = os.environ.get("DRY_RUN", "false").lower() == "true"


# ─── Alpaca 클라이언트 초기화 ───────────────────────────────────
def _get_clients():
    from alpaca.trading.client import TradingClient
    from alpaca.data.historical import StockHistoricalDataClient

    if not API_KEY or not SECRET_KEY:
        raise RuntimeError("ALPACA_API_KEY / ALPACA_SECRET_KEY 환경변수 미설정")

    paper = "paper-api" in BASE_URL
    trading = TradingClient(API_KEY, SECRET_KEY, paper=paper)
    data    = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    return trading, data


# ─── 목표 비중 로드 ────────────────────────────────────────────
def load_target_weights() -> dict[str, float]:
    path = DATA_DIR / "live_state.json"
    if not path.exists():
        raise FileNotFoundError(f"live_state.json 없음: {path}")

    with open(path, encoding="utf-8") as f:
        state = json.load(f)

    weights: dict[str, float] = state.get("portfolio_weights", {})
    if not weights:
        raise ValueError("live_state.json에 portfolio_weights 없음")

    # 정규화
    total = sum(weights.values())
    return {t: w / total for t, w in weights.items() if w > 0}


# ─── 리밸런싱 필요 여부 판단 ───────────────────────────────────
def needs_rebalance(current_pct: dict[str, float],
                    target: dict[str, float],
                    last_rebalance_date: str | None) -> tuple[bool, str]:
    """(리밸런싱 여부, 이유)"""

    # 1. 월 1회: 이번 달 아직 리밸런싱 안 했으면 실행
    today = date.today()
    this_month = f"{today.year}-{today.month:02d}"
    if last_rebalance_date is None or not last_rebalance_date.startswith(this_month):
        return True, f"월 정기 리밸런싱 ({this_month})"

    # 2. 드리프트 초과: 어떤 종목이든 목표 대비 DRIFT_THRESHOLD 이상 벗어나면 즉시
    all_tickers = set(current_pct) | set(target)
    for t in all_tickers:
        drift = abs(current_pct.get(t, 0.0) - target.get(t, 0.0))
        if drift > DRIFT_THRESHOLD:
            return True, f"드리프트 초과: {t} {drift:.1%}"

    return False, "리밸런싱 불필요"


# ─── 현재 포지션 조회 ──────────────────────────────────────────
def get_current_positions(trading) -> tuple[dict[str, float], float]:
    """현재 포지션 {ticker: 시장가치}, 총 포트폴리오 가치 반환"""
    from alpaca.trading.requests import GetPortfolioHistoryRequest

    account   = trading.get_account()
    portfolio_value = float(account.portfolio_value)
    cash      = float(account.cash)

    positions = trading.get_all_positions()
    pos_map: dict[str, float] = {}
    for p in positions:
        pos_map[p.symbol] = float(p.market_value)

    return pos_map, portfolio_value


# ─── 현재가 조회 ───────────────────────────────────────────────
def get_latest_prices(data_client, tickers: list[str]) -> dict[str, float]:
    from alpaca.data.requests import StockLatestQuoteRequest

    req    = StockLatestQuoteRequest(symbol_or_symbols=tickers)
    quotes = data_client.get_stock_latest_quote(req)
    prices: dict[str, float] = {}
    for sym, q in quotes.items():
        mid = (float(q.bid_price) + float(q.ask_price)) / 2
        if mid > 0:
            prices[sym] = mid
    return prices


# ─── 주문 실행 ─────────────────────────────────────────────────
def submit_order(trading, symbol: str, qty: float, side: str, dry_run: bool) -> dict:
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
    qty_rounded = max(1, round(qty))  # 정수 주(share) 단위

    if dry_run:
        print(f"  [DRY] {side.upper()} {symbol} x{qty_rounded}")
        return {"symbol": symbol, "side": side, "qty": qty_rounded, "status": "dry_run"}

    req = MarketOrderRequest(
        symbol=symbol,
        qty=qty_rounded,
        side=order_side,
        time_in_force=TimeInForce.DAY,
    )
    order = trading.submit_order(req)
    print(f"  [주문] {side.upper()} {symbol} x{qty_rounded}  id={order.id}")
    return {
        "symbol":   symbol,
        "side":     side,
        "qty":      qty_rounded,
        "order_id": str(order.id),
        "status":   str(order.status),
    }


# ─── 메인 리밸런싱 로직 ───────────────────────────────────────
def run_rebalance() -> dict:
    print(f"=== Alpaca 리밸런싱 {'[DRY RUN] ' if DRY_RUN else ''}===")
    print(f"  URL  : {BASE_URL}")
    print(f"  시각 : {datetime.now(timezone.utc).isoformat()}")

    trading, data_client = _get_clients()
    target_w = load_target_weights()
    print(f"\n목표 포트폴리오 ({len(target_w)}개 종목):")
    for t, w in sorted(target_w.items()):
        print(f"  {t:<6} {w:.2%}")

    # 현재 포지션
    pos_values, portfolio_value = get_current_positions(trading)
    print(f"\n계좌 총 자산: ${portfolio_value:,.0f}")

    current_pct = {t: v / portfolio_value for t, v in pos_values.items()}

    # 로그 파일에서 마지막 리밸런싱 날짜 읽기
    log_path = DATA_DIR / "alpaca_trade_log.json"
    log_data: list[dict] = []
    if log_path.exists():
        with open(log_path, encoding="utf-8") as f:
            log_data = json.load(f)
    last_rebal = log_data[-1]["date"] if log_data else None

    should_rebal, reason = needs_rebalance(current_pct, target_w, last_rebal)
    print(f"\n리밸런싱 판단: {'실행' if should_rebal else '스킵'} — {reason}")

    if not should_rebal:
        return {"action": "skip", "reason": reason, "portfolio_value": portfolio_value}

    # 현재가 조회
    all_tickers = sorted(set(target_w) | set(pos_values))
    prices = get_latest_prices(data_client, all_tickers)
    print(f"\n현재가 조회: {len(prices)}개")

    # 목표 금액 및 주(share) 수 계산
    target_values  = {t: portfolio_value * w for t, w in target_w.items()}
    target_shares  = {t: v / prices[t] for t, v in target_values.items() if t in prices}

    # 현재 보유 주수
    positions_raw  = trading.get_all_positions()
    current_shares = {p.symbol: float(p.qty) for p in positions_raw}

    # 매도/매수 계산
    sells, buys = [], []
    for t in all_tickers:
        cur = current_shares.get(t, 0.0)
        tgt = target_shares.get(t, 0.0)
        delta = tgt - cur
        price = prices.get(t, 0.0)

        if abs(delta) * price < MIN_ORDER_USD:
            continue
        if delta < 0:
            sells.append((t, abs(delta), price))
        elif delta > 0:
            buys.append((t, delta, price))

    orders_executed = []

    # 매도 먼저 (현금 확보)
    print(f"\n[매도 {len(sells)}건]")
    for sym, qty, price in sorted(sells, key=lambda x: -x[1] * x[2]):
        result = submit_order(trading, sym, qty, "sell", DRY_RUN)
        orders_executed.append(result)

    # 매수
    print(f"\n[매수 {len(buys)}건]")
    for sym, qty, price in sorted(buys, key=lambda x: -x[1] * x[2]):
        result = submit_order(trading, sym, qty, "buy", DRY_RUN)
        orders_executed.append(result)

    # 로그 저장
    entry = {
        "date":            date.today().isoformat(),
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "reason":          reason,
        "portfolio_value": round(portfolio_value, 2),
        "target_weights":  {t: round(w, 4) for t, w in target_w.items()},
        "orders":          orders_executed,
        "dry_run":         DRY_RUN,
    }
    log_data.append(entry)
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False)
    print(f"\n[OK] 로그 저장: {log_path}  (총 {len(log_data)}회)")

    return entry


if __name__ == "__main__":
    try:
        result = run_rebalance()
        print(f"\n완료: {result.get('action', 'rebalanced')}")
    except Exception as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
