"""SEC 추출값을 독립 출처(Sharadar SF1 MRY, data/fundamentals.json)와 대조 (2026-10).

fundamentals.json은 Sharadar SF1의 최신 연간(MRY) 값으로 2026-06-04에 만들어졌다.
같은 회계기간 말(SF1 datekey == SEC 추출 FYE)인 종목만 골라 매출·영업이익을 비교한다.
수정 전 로직 값(comparison.csv의 old_*)도 같은 기준으로 비교해 오류율 차이를 본다.

출력: scripts/validation_outputs/quality_fix_2026-10/crosscheck_sf1.csv, crosscheck_sf1_summary.json
사용: python scripts/crosscheck_quality_vs_sf1.py  (compare_quality_snapshot.py를 먼저 실행)
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "scripts" / "validation_outputs" / "quality_fix_2026-10"
TOL = 0.005  # 0.5% 이내면 일치로 본다(반올림·소폭 정정 허용)


def rel_diff(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or pd.isna(a) or pd.isna(b) or b == 0:
        return None
    return abs(a - b) / abs(b)


def main() -> None:
    sf1 = json.loads((ROOT / "data" / "fundamentals.json").read_text(encoding="utf-8"))
    cmp = pd.read_csv(OUT_DIR / "comparison.csv")
    rows = []
    for r in cmp.itertuples(index=False):
        s = sf1.get(r.ticker) or sf1.get(r.ticker.replace("-", "."))
        if not isinstance(s, dict):
            continue
        a = s.get("annual_latest") or {}
        same_period = str(s.get("datekey")) == str(r.new_fye)
        for item, key in (("revenue", "revenue"), ("op_income", "op_income")):
            ref = a.get(key)
            if ref is None:
                continue
            new_v, old_v = getattr(r, f"new_{item}"), getattr(r, f"old_{item}")
            dn, do = rel_diff(new_v, ref), rel_diff(old_v, ref)
            rows.append({
                "ticker": r.ticker, "item": item, "sf1_period_end": s.get("datekey"), "sec_fye": r.new_fye,
                "same_period": same_period, "sf1_value": ref, "new_value": new_v, "old_value": old_v,
                "new_rel_diff": dn, "old_rel_diff": do,
                "new_match": (dn is not None and dn <= TOL), "old_match": (do is not None and do <= TOL),
            })
    df = pd.DataFrame(rows)
    sp = df[df["same_period"]]

    def stats(sub: pd.DataFrame) -> dict:
        out = {}
        for item in ("revenue", "op_income"):
            x = sub[sub["item"] == item]
            nn, no = x["new_value"].notna(), x["old_value"].notna()
            out[item] = {
                "n_same_period": int(len(x)),
                "new_compared": int(nn.sum()),
                "new_match_within_0.5pct": int(x.loc[nn, "new_match"].sum()),
                "new_match_rate": round(float(x.loc[nn, "new_match"].mean()), 4) if nn.any() else None,
                "old_compared": int(no.sum()),
                "old_match_within_0.5pct": int(x.loc[no, "old_match"].sum()),
                "old_match_rate": round(float(x.loc[no, "old_match"].mean()), 4) if no.any() else None,
            }
        return out

    mism = sp[sp["new_value"].notna() & ~sp["new_match"]].sort_values("new_rel_diff", ascending=False)
    summary = {
        "reference": "Sharadar SF1 MRY via data/fundamentals.json (generated 2026-06-04)",
        "rule": "compare only where SF1 period end == SEC-extracted fiscal year end; match if relative diff <= 0.5%",
        "tickers_in_both": int(df["ticker"].nunique()),
        "tickers_same_period": int(sp["ticker"].nunique()),
        "by_item": stats(sp),
        "new_mismatches": mism[["ticker", "item", "sf1_value", "new_value", "new_rel_diff"]].head(30).to_dict(orient="records"),
    }
    df.to_csv(OUT_DIR / "crosscheck_sf1.csv", index=False, encoding="utf-8-sig")
    (OUT_DIR / "crosscheck_sf1_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
