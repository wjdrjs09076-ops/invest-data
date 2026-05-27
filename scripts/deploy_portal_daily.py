#!/usr/bin/env python3
"""
deploy_portal_daily.py

daily update 후 invest-portal public/data 파일 전체를 커밋·푸시하는 헬퍼.
누락 없이 항상 같은 파일 목록을 커밋한다.

사용법:
    python scripts/deploy_portal_daily.py
    python scripts/deploy_portal_daily.py --date 2026-05-27  # 커밋 메시지용 날짜 지정
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PORTAL_DIR = Path(__file__).resolve().parents[2]  # invest-portal/

# portal에 커밋해야 할 public/data 파일 목록 (누락 방지용 고정 리스트)
TARGET_FILES = [
    "public/data/market_regime.json",
    "public/data/score_snapshot.json",
    "public/data/live_performance.json",
    "public/data/live_state.json",
    "public/data/risk_snapshot.json",
    "public/data/fundamentals.json",
    "public/data/daily_valuation.json",
    "public/data/banners.json",
]


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="커밋 메시지용 날짜 (기본값: 오늘)")
    args = parser.parse_args()

    today = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # 변경된 파일만 스테이징
    staged = []
    for rel in TARGET_FILES:
        path = PORTAL_DIR / rel
        if not path.exists():
            continue
        result = run(["git", "diff", "--name-only", rel], cwd=PORTAL_DIR)
        result2 = run(["git", "diff", "--name-only", "--cached", rel], cwd=PORTAL_DIR)
        # 추적되지 않는 파일도 포함
        result3 = run(["git", "ls-files", "--others", "--exclude-standard", rel], cwd=PORTAL_DIR)
        if result.stdout.strip() or result2.stdout.strip() or result3.stdout.strip():
            run(["git", "add", rel], cwd=PORTAL_DIR)
            staged.append(rel)

    if not staged:
        print("[SKIP] 변경된 파일 없음")
        return

    print(f"[STAGE] {len(staged)}개 파일:")
    for f in staged:
        print(f"  {f}")

    msg = (
        f"data: daily update {today}\n\n"
        + "\n".join(f"- {Path(f).name}" for f in staged)
        + "\n\nCo-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
    )

    result = run(["git", "commit", "-m", msg], cwd=PORTAL_DIR)
    if result.returncode != 0:
        print(f"[ERROR] commit 실패:\n{result.stderr}")
        sys.exit(1)
    print(f"[OK] commit: {result.stdout.strip().splitlines()[0]}")

    result = run(["git", "push", "origin", "main"], cwd=PORTAL_DIR)
    if result.returncode != 0:
        print(f"[ERROR] push 실패:\n{result.stderr}")
        sys.exit(1)
    print("[OK] push 완료 → Vercel 배포 시작됨")


if __name__ == "__main__":
    main()
