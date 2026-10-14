"""quality_snapshot 추출 로직 수정 전/후 비교 (2026-10).

같은 SEC companyfacts 캐시(data/sec_cache)에 이전 추출 로직과 수정된 로직을 각각 적용해
로직 변경만의 효과를 측정한다. 이전 로직은 아래 legacy_* 함수에 선택 규칙을 그대로 옮겨 두었고,
어떤 기간·서식의 값을 골랐는지 확인할 수 있도록 메타데이터만 추가로 반환한다.

출력: scripts/validation_outputs/quality_fix_2026-10/
  comparison.csv  종목별 이전/수정 값·기간·태그·점수
  summary.json    집계(불가능 비율, 분기값 사용, 낡은 태그, 기간 불일치, 커버리지, 점수 변화)

사용: python scripts/compare_quality_snapshot.py   (캐시가 있어야 함. 없으면 build_quality_snapshot.py를 먼저 실행)
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_quality_snapshot as q  # noqa: E402

OUT_DIR = Path(__file__).resolve().parent / "validation_outputs" / "quality_fix_2026-10"
PROD_FILE = q.DATA_DIR / "quality_snapshot.json"
ITEMS = ["net_income", "equity", "gross_profit", "assets", "revenue", "op_income"]

LEGACY_TAGS = {
    "net_income": [("us-gaap", "NetIncomeLoss"), ("us-gaap", "ProfitLoss")],
    "equity": [
        ("us-gaap", "StockholdersEquity"),
        ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
        ("us-gaap", "CommonStockholdersEquity"),
    ],
    "gross_profit": [("us-gaap", "GrossProfit")],
    "assets": [("us-gaap", "Assets")],
    "revenue": [
        ("us-gaap", "Revenues"),
        ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
        ("us-gaap", "SalesRevenueNet"),
    ],
    "op_income": [("us-gaap", "OperatingIncomeLoss")],
}


# ---------------------------------------------------------------------------
# 이전 로직 (선택 규칙은 2026-03 버전과 동일)
# ---------------------------------------------------------------------------

def legacy_latest_from_tag(facts: dict[str, Any], taxonomy: str, tag: str) -> dict[str, Any] | None:
    try:
        units_dict = facts[taxonomy][tag]["units"]
    except Exception:
        return None
    frames = []
    for _, values in units_dict.items():  # 모든 단위를 확인 없이 합침
        if isinstance(values, list) and values:
            df = pd.DataFrame(values)
            if not df.empty and "val" in df.columns:
                frames.append(df)
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    if "form" in df.columns:
        df = df[df["form"].astype(str).isin(["10-K", "10-Q", "20-F", "40-F"])]  # 연간·분기 혼합
    if df.empty:
        return None
    df["end"] = pd.to_datetime(df["end"], errors="coerce") if "end" in df.columns else pd.NaT
    df["filed"] = pd.to_datetime(df["filed"], errors="coerce") if "filed" in df.columns else pd.NaT
    df = df.dropna(subset=["val"])
    if df.empty:
        return None
    df = df.sort_values(["end", "filed"], ascending=[True, True])  # 종료일이 가장 최근인 값
    row = df.iloc[-1]
    start = pd.to_datetime(row.get("start"), errors="coerce") if "start" in df.columns else pd.NaT
    end = row["end"]
    return {
        "val": float(row["val"]),
        "tag": tag,
        "end": end.date().isoformat() if pd.notna(end) else None,
        "form": row.get("form"),
        "duration_days": int((end - start).days) if pd.notna(start) and pd.notna(end) else None,
    }


def legacy_first_available(facts: dict[str, Any], candidates: list[tuple[str, str]]) -> dict[str, Any] | None:
    for taxonomy, tag in candidates:  # 값이 하나라도 있는 첫 태그 채택(최신성 확인 없음)
        rec = legacy_latest_from_tag(facts, taxonomy, tag)
        if rec is not None:
            return rec
    return None


def legacy_extract(companyfacts: dict[str, Any]) -> dict[str, dict[str, Any] | None]:
    facts = companyfacts.get("facts", {})
    return {item: legacy_first_available(facts, LEGACY_TAGS[item]) for item in ITEMS}


def legacy_metrics(v: dict[str, float | None]) -> dict[str, float | None]:
    return {
        "roe": q.safe_ratio(v["net_income"], v["equity"]),
        "gpa": q.safe_ratio(v["gross_profit"], v["assets"]),
        "margin": q.safe_ratio(v["op_income"], v["revenue"]),
    }


# ---------------------------------------------------------------------------
# 비교
# ---------------------------------------------------------------------------

def _s(x: Any) -> str | None:
    return x if isinstance(x, str) and x else None


def main() -> None:
    today = datetime.now().date()
    tickers = q.load_sp500_tickers()
    cik_map = q.load_ticker_cik_map(offline=True)

    old_rows, new_rows, detail = [], [], []
    for t in tickers:
        cik = cik_map.get(t)
        if not cik:
            continue
        cf = q.fetch_company_facts(cik, offline=True)
        if not cf:
            continue
        leg = legacy_extract(cf)
        lv = {k: (leg[k]["val"] if leg[k] else None) for k in ITEMS}
        old_rows.append({"ticker": t, **lv, **legacy_metrics(lv)})

        fin = q.extract_financials(cf, today=today)
        met = q.compute_quality_metrics(fin)
        new_rows.append({"ticker": t, **{k: fin[k] for k in ITEMS}, **met})

        d: dict[str, Any] = {"ticker": t, "cik": cik, "new_fye": fin["fiscal_year_end"], "new_flags": ";".join(fin["flags"])}
        for k in ITEMS:
            o = leg[k]
            d[f"old_{k}"] = o["val"] if o else None
            d[f"old_{k}_tag"] = o["tag"] if o else None
            d[f"old_{k}_end"] = o["end"] if o else None
            d[f"old_{k}_form"] = o["form"] if o else None
            d[f"old_{k}_days"] = o["duration_days"] if o else None
            d[f"new_{k}"] = fin[k]
            d[f"new_{k}_tag"] = fin["source"].get(k, {}).get("tag")
        detail.append(d)

    old = q.score_frame(pd.DataFrame(old_rows)).set_index("ticker")
    new = q.score_frame(pd.DataFrame(new_rows)).set_index("ticker")
    det = pd.DataFrame(detail).set_index("ticker")
    for c in ["roe", "gpa", "margin", "quality_score_100"]:
        det[f"old_{c}"] = old[c]
        det[f"new_{c}"] = new[c]
    det["score_diff"] = det["new_quality_score_100"] - det["old_quality_score_100"]

    prod = q.read_json(PROD_FILE, default={})
    prod_score = {x["ticker"]: x.get("quality_score_100") for x in prod.get("items", [])}
    det["prod_quality_score_100"] = pd.Series(prod_score)
    det["prod_generated_at"] = prod.get("generated_at")

    # --- 집계 ---
    flow = ["net_income", "gross_profit", "revenue", "op_income"]
    n = len(det)

    def old_quarterly(row: pd.Series) -> bool:
        return any(row[f"old_{k}_form"] == "10-Q" for k in flow if pd.notna(row[f"old_{k}"]))

    def old_stale(row: pd.Series) -> bool:
        for k in ITEMS:
            e = _s(row[f"old_{k}_end"])
            if e and (today - date.fromisoformat(e)).days > q.STALE_DAYS:
                return True
        return False

    def old_period_mismatch(row: pd.Series) -> bool:
        pairs = [("op_income", "revenue"), ("net_income", "equity"), ("gross_profit", "assets")]
        for a, b in pairs:
            ea, eb = _s(row[f"old_{a}_end"]), _s(row[f"old_{b}_end"])
            if ea and eb and ea != eb:
                return True
        return False

    def impossible(margin: Any) -> bool:
        # 영업이익 > 매출은 회계상 불가능. 영업손실이 매출보다 큰 경우(margin < -1)는 실제로 가능하므로 제외.
        return margin is not None and pd.notna(margin) and margin > 1

    old_q = det.apply(old_quarterly, axis=1)
    old_s = det.apply(old_stale, axis=1)
    old_pm = det.apply(old_period_mismatch, axis=1)
    old_gp_gt_rev = ((det["old_gross_profit"] > det["old_revenue"] * 1.0001) & det["old_revenue"].gt(0)).sum()

    stale_examples = []
    for t, row in det[old_s].iterrows():
        for k in ["revenue", "net_income"]:
            e = _s(row[f"old_{k}_end"])
            if e and (today - date.fromisoformat(e)).days > q.STALE_DAYS:
                stale_examples.append({"ticker": t, "item": k, "old_tag": row[f"old_{k}_tag"], "old_end": e,
                                       "old_val": row[f"old_{k}"], "new_val": row[f"new_{k}"],
                                       "new_tag": row[f"new_{k}_tag"]})

    rank_corr = det["old_quality_score_100"].corr(det["new_quality_score_100"], method="spearman")
    absdiff = det["score_diff"].abs()
    prod_cmp = det.dropna(subset=["prod_quality_score_100"])
    prod_absdiff = (prod_cmp["new_quality_score_100"] - prod_cmp["prod_quality_score_100"]).abs()

    flag_counts: dict[str, int] = {}
    for f in det["new_flags"]:
        for x in filter(None, str(f).split(";")):
            flag_counts[x] = flag_counts.get(x, 0) + 1

    summary = {
        "run_date": today.isoformat(),
        "data": "same SEC companyfacts cache for both versions (data/sec_cache)",
        "tickers_compared": n,
        "tickers_without_cik_in_sec_map": sorted(set(tickers) - set(cik_map)),
        "old": {
            "opinc_gt_revenue_margin_gt_100pct": int(sum(impossible(m) for m in det["old_margin"])),
            "margin_below_minus_100pct": int((det["old_margin"] < -1).sum()),
            "gross_profit_gt_revenue": int(old_gp_gt_rev),
            "any_flow_item_from_10Q": int(old_q.sum()),
            "any_item_period_end_older_than_550d": int(old_s.sum()),
            "ratio_components_with_different_period_ends": int(old_pm.sum()),
            "coverage": {c: int(det[f"old_{c}"].notna().sum()) for c in ["roe", "gpa", "margin"]},
        },
        "new": {
            "opinc_gt_revenue_margin_gt_100pct": int(sum(impossible(m) for m in det["new_margin"])),
            "margin_below_minus_100pct_operating_loss_gt_revenue": det.loc[det["new_margin"] < -1].index.tolist(),
            "coverage": {c: int(det[f"new_{c}"].notna().sum()) for c in ["roe", "gpa", "margin"]},
            "fye_distribution_by_month": det["new_fye"].dropna().str[:7].value_counts().sort_index().to_dict(),
            "flag_counts": dict(sorted(flag_counts.items(), key=lambda kv: -kv[1])),
        },
        "score_change_same_data": {
            "spearman_old_vs_new": round(float(rank_corr), 3),
            "mean_abs_diff": round(float(absdiff.mean()), 1),
            "n_abs_diff_ge_20": int((absdiff >= 20).sum()),
            "n_abs_diff_ge_30": int((absdiff >= 30).sum()),
        },
        "score_change_vs_production_file": {
            "production_generated_at": prod.get("generated_at"),
            "n_compared": int(len(prod_cmp)),
            "mean_abs_diff": round(float(prod_absdiff.mean()), 1),
            "n_abs_diff_ge_20": int((prod_absdiff >= 20).sum()),
        },
        "stale_tag_examples": stale_examples[:25],
        "n_stale_tag_cases_revenue_or_net_income": len(stale_examples),
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    det.reset_index().to_csv(OUT_DIR / "comparison.csv", index=False, encoding="utf-8-sig")
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "stale_tag_examples"}, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
