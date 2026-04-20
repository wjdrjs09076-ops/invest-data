# Quant Validation Report

## What this script checks
- Full-period baseline
- Ablation variants
- Parameter sensitivity grid
- Rolling walk-forward IS/OOS selection
- Untouched holdout period
- Turnover and regime diagnostics
- Block bootstrap for relative return stability

## Current-code caveats
- Historical backtest uses quality_score_map=None, so quality is not included in the actual historical backtest.
- Snapshot scoring applies local news filtering, but historical backtest does not apply the same news filter.
- The snapshot script contains an analyze_news_from_local() path referencing RED_FLAG_REGEX, which is undefined in the uploaded file and should be fixed before relying on that function.

## Output files
See the CSV, JSON-compatible CSV tables, and PNG charts inside validation_outputs/.