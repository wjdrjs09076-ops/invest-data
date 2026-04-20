from __future__ import annotations

import os
import json
import requests
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any
from dotenv import load_dotenv

# ==========================================
# [필수 임포트] 백테스트 모듈 활용
# ==========================================
from build_score_snapshot import (
    build_metrics_for_group,
    score_group,
    compute_portfolio_weights,
    PORTFOLIO_WEIGHT_METHOD,
    MIN_WEIGHT,
    MAX_WEIGHT,
    VOL_FALLBACK,
    VOL_WEIGHT_FLOOR,
    ABS_MOM_63D_MIN,
    ABS_MOM_252D_MIN,
    annualized_volatility, 
)

# ==========================================
# 1. 실전용 파라미터 및 경로 설정
# ==========================================
ROOT = Path(__file__).resolve().parents[1] # 여기가 invest-data 폴더
DATA_DIR = ROOT / "data"
CURRENT_UNIVERSE_FILES = [DATA_DIR / "sp500_current_wiki.json", DATA_DIR / "sp400_current_wiki.json", DATA_DIR / "sp600_current_wiki.json"]

FINAL_HOLDINGS_FILE = DATA_DIR / "final_holdings.json"
LAST_REGIME_FILE = DATA_DIR / "last_regime.txt" 

# 💡 [경로 수정 완료] ROOT(invest-data)의 부모 폴더(invest-portal)에서 .env.local을 찾습니다!
env_path = ROOT.parent / ".env.local"
load_dotenv(dotenv_path=str(env_path))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 확정된 파라미터 세팅 (Master V1.0)
WEIGHT_ALPHA_SCORE = 2.5  
TOP_N = 15
HOLD_BUFFER_N = 25     
SECTOR_MAX_NAMES = 3

REGIME_MA_WINDOW = 200
REGIME_MOM_WINDOW = 63
REGIME_BUFFER = 0.005
REGIME_CONFIRM_DAYS = 2 
VIX_CRASH_THRESHOLD = 40.0

RISK_ON_EXPOSURE = 1.00
MID_EXPOSURE = 0.85
RISK_OFF_EXPOSURE = 0.40
DEFENSIVE_TICKERS = ["TAIL", "DBMF"]

# ==========================================
# 2. 헬퍼 함수 (텔레그램 알림 포함)
# ==========================================
def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists(): return default
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def normalize_ticker(t: str) -> str: 
    return str(t).strip().upper().replace(".", "-")

def load_current_universe() -> dict[str, dict[str, Any]]:
    out = {}
    for file in CURRENT_UNIVERSE_FILES:
        for item in load_json(file, default={}).get("items", []):
            t = normalize_ticker(item.get("ticker", ""))
            if t: out[t] = {"ticker": t, "sector": item.get("sector", "Unknown") or "Unknown"}
    return out

def send_telegram_alert(message: str):
    """텔레그램 봇을 통해 알림을 전송합니다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ 텔레그램 토큰이 설정되지 않아 알림을 건너뜁니다.")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            print("📲 텔레그램 알림 전송 완료!")
        else:
            print(f"⚠️ 텔레그램 알림 실패: {response.text}")
    except Exception as e:
        print(f"⚠️ 텔레그램 전송 중 오류 발생: {e}")

# ==========================================
# 3. 메인 실행 함수
# ==========================================
def run_daily_snapshot():
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n" + "="*75)
    print(f"📡 Invest Portal V1.0 - 자동화 실전 매매 엔진 (Live Execution)")
    print(f"🕒 기준일시: {now_str}")
    print("="*75)

    # 과거 상태 불러오기
    CURRENT_HOLDINGS = []
    if FINAL_HOLDINGS_FILE.exists():
        try:
            with open(FINAL_HOLDINGS_FILE, "r", encoding="utf-8") as f:
                CURRENT_HOLDINGS = json.load(f)
        except Exception:
            pass

    past_regime = "Unknown"
    if LAST_REGIME_FILE.exists():
        with open(LAST_REGIME_FILE, "r", encoding="utf-8") as f:
            past_regime = f.read().strip()

    # 1. 레짐(Regime) 판단
    print("\n[1/3] 거시 경제 지표(SPY, VIX) 스캔 중...")
    regime_df = yf.download(["SPY", "^VIX"], period="2y", progress=False)["Close"]
    spy_px = regime_df["SPY"].dropna()
    vix_px = regime_df["^VIX"].dropna()
    
    df_bench = spy_px.to_frame(name='price')
    df_bench['ma200'] = df_bench['price'].rolling(REGIME_MA_WINDOW).mean()
    df_bench['mom63'] = df_bench['price'].pct_change(REGIME_MOM_WINDOW)
    
    cond_on = (df_bench['price'] >= df_bench['ma200'] * (1+REGIME_BUFFER)) & (df_bench['mom63'] >= 0)
    cond_off = (df_bench['price'] <= df_bench['ma200'] * (1-REGIME_BUFFER)) & (df_bench['mom63'] < 0)
    
    cand_values = np.full(len(df_bench), 'mid', dtype=object)
    cand_values[cond_on] = 'risk_on'
    cand_values[cond_off] = 'risk_off'
    
    regimes = np.full(len(df_bench), 'risk_on', dtype=object)
    prev = 'risk_on'
    for i in range(len(df_bench)):
        cand = cand_values[i]
        if cand in ["risk_on", "risk_off"] and i >= REGIME_CONFIRM_DAYS - 1:
            recent = cand_values[i - REGIME_CONFIRM_DAYS + 1 : i + 1]
            regimes[i] = cand if all(x == cand for x in recent) else prev
        elif cand in ["risk_on", "risk_off"]:
            regimes[i] = cand
        else:
            regimes[i] = prev
        prev = regimes[i]
        
    current_regime_base = regimes[-1]
    current_spy = df_bench['price'].iloc[-1]
    ma200_val = df_bench['ma200'].iloc[-1]
    current_vix = vix_px.iloc[-1]
    
    is_crash = current_spy < ma200_val and current_vix > VIX_CRASH_THRESHOLD
    final_regime = 'risk_off (VIX CRASH)' if is_crash else current_regime_base

    target_exp = RISK_ON_EXPOSURE if 'risk_on' in final_regime else (RISK_OFF_EXPOSURE if 'risk_off' in final_regime else MID_EXPOSURE)

    print(f"   ▶ SPY 현재가: $ {current_spy:.2f} (MA200: $ {ma200_val:.2f})")
    print(f"   ▶ VIX 공포지수: {current_vix:.2f}")
    print(f"   => 🛡️ 현재 레짐 상태: [{final_regime.upper()}] -> 주식 노출 목표: {target_exp:.0%}")

    # 2. 스코어링
    print("\n[2/3] 유니버스 주가 다운로드 및 스코어링 중...")
    univ = load_current_universe()
    tickers = list(univ.keys())
    
    df_dl = yf.download(tickers, period="18mo", auto_adjust=True, progress=False, group_by="ticker", threads=True)
    price_map = {}
    if isinstance(df_dl.columns, pd.MultiIndex):
        level0 = set(df_dl.columns.get_level_values(0))
        for t in tickers:
            if t in level0 and "Close" in df_dl[t]:
                s = df_dl[t]["Close"].dropna()
                if len(s) >= 252: price_map[t] = s

    metrics = build_metrics_for_group(list(price_map.keys()), univ, price_map)
    scored = score_group(metrics, quality_score_map=None)
    
    ranked = [r for r in scored if r.final_score_100 is not None and r.ret63d is not None and r.ret252d is not None]
    ranked = [r for r in ranked if r.ret63d > ABS_MOM_63D_MIN and r.ret252d > ABS_MOM_252D_MIN]
    ranked.sort(key=lambda x: x.final_score_100, reverse=True)

    final_picks, sector_counts = [], {}
    def add_to_out(r):
        sec = r.sector or "Unknown"
        if SECTOR_MAX_NAMES <= 0 or sector_counts.get(sec, 0) < SECTOR_MAX_NAMES:
            final_picks.append(r)
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
            return True
        return False

    held_eligible = [r for i, r in enumerate(ranked) if r.ticker in CURRENT_HOLDINGS and (i + 1) <= HOLD_BUFFER_N]
    for r in held_eligible:
        if len(final_picks) < TOP_N: add_to_out(r)

    for r in ranked:
        if len(final_picks) >= TOP_N: break
        if any(existing.ticker == r.ticker for existing in final_picks): continue
        add_to_out(r)

    spy_vol20 = annualized_volatility(spy_px, 20)
    dynamic_floor = max(VOL_WEIGHT_FLOOR, spy_vol20)
    weighted_picks = compute_portfolio_weights(
        final_picks, method=PORTFOLIO_WEIGHT_METHOD, alpha_score=WEIGHT_ALPHA_SCORE, 
        min_w=MIN_WEIGHT, max_w=MAX_WEIGHT, vol_fallback=VOL_FALLBACK, 
        vol_floor=VOL_WEIGHT_FLOOR, dynamic_vol_floor=dynamic_floor
    )

    # ==========================================
    # 4. 결과 출력 및 텔레그램 알림 구성
    # ==========================================
    new_holdings = [r.ticker for r in weighted_picks]
    buys = [t for t in new_holdings if t not in CURRENT_HOLDINGS]
    sells = [t for t in CURRENT_HOLDINGS if t not in new_holdings]

    print("\n[3/3] 🎯 오늘자 모델 포트폴리오 산출 완료!\n")
    print(f"{'Ticker':<7} | {'Sector':<22} | {'Score':<6} | {'Vol(20d)':<8} | {'Raw Wgt':<8} | {'Target Wgt':<15}")
    print("-" * 80)
    
    total_stock_wgt = 0
    for r in weighted_picks:
        raw_w = r.portfolio_weight
        actual_w = raw_w * target_exp
        total_stock_wgt += actual_w
        buffer_mark = "📌 " if r.ticker in CURRENT_HOLDINGS else "🟢 "
        print(f"{r.ticker:<7} | {str(r.sector)[:20]:<22} | {r.final_score_100:>5.1f} | {r.vol20:>7.1%} | {raw_w:>7.1%} | {buffer_mark}{actual_w:>7.1%}")
    print("-" * 80)

    # --- 텔레그램 메시지 조립 ---
    tg_msg = f"🚀 <b>Invest Portal V1.0 Daily Report</b>\n📅 {now_str}\n\n"
    
    # 1. 레짐 알림
    tg_msg += f"🛡️ <b>Regime:</b> {final_regime.upper()} (Exposure: {target_exp:.0%})\n"
    if past_regime != "Unknown" and past_regime != final_regime:
        tg_msg += f"⚠️ <b>레짐 변경 발생!</b> ({past_regime} ➡️ {final_regime})\n"
    tg_msg += "\n"

    # 2. 매매 알림
    if buys or sells:
        tg_msg += f"🔄 <b>리밸런싱 신호 발생</b>\n"
        if buys: tg_msg += f"🟢 <b>BUY:</b> {', '.join(buys)}\n"
        if sells: tg_msg += f"🔴 <b>SELL:</b> {', '.join(sells)}\n"
    else:
        tg_msg += f"✅ <b>종목 교체 없음 (Hold)</b>\n"
        
    tg_msg += "\n📊 <b>Top 3 Holdings:</b>\n"
    for i, r in enumerate(weighted_picks[:3]):
        tg_msg += f"{i+1}. {r.ticker} ({r.portfolio_weight * target_exp:.1%})\n"

    # 텔레그램 전송
    send_telegram_alert(tg_msg)

    # ==========================================
    # 5. 내일 실행을 위한 현재 상태 저장 (Autonomous Loop)
    # ==========================================
    with open(FINAL_HOLDINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(new_holdings, f)
    with open(LAST_REGIME_FILE, "w", encoding="utf-8") as f:
        f.write(final_regime)

if __name__ == "__main__":
    run_daily_snapshot()