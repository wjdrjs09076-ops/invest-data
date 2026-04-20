from __future__ import annotations

import importlib.util
import itertools
import json
import math
import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parent
DEFAULT_BUILD_PATH = ROOT / 'build_score_snapshot.py'
DEFAULT_BACKTEST_PATH = ROOT / 'run_backtest_regime.py'
OUTPUT_DIR = ROOT / 'validation_outputs'
USE_BACKTEST_CACHE = True
PRECOMPUTE_SNAPSHOTS = True



def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f'Unable to load module from {path}')
    module = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


build = load_module('build_score_snapshot_user', DEFAULT_BUILD_PATH)
backtest = load_module('run_backtest_regime_user', DEFAULT_BACKTEST_PATH)


@dataclass
class StrategyConfig:
    rebalance: str = 'monthly'
    selection: str = 'TOP'
    top_n: int = 15
    hold_buffer_n: int = 25
    transaction_cost: float = 0.0015
    benchmark: str = 'SPY'
    vix_ticker: str = '^VIX'
    vix_crash_threshold: float = 40.0
    min_history: int = 220
    regime_ma_window: int = 200
    regime_mom_window: int = 63
    risk_on_exposure: float = 1.00
    mid_exposure: float = 0.85
    risk_off_exposure: float = 0.40
    regime_buffer: float = 0.005
    regime_confirm_days: int = 2
    defensive_tickers: tuple[str, ...] = ('TAIL', 'DBMF')
    defensive_weights: tuple[tuple[str, float], ...] = (('TAIL', 0.3), ('DBMF', 0.7))
    exposure_rebalance: str = 'daily'
    portfolio_weight_method: str = 'score_x_inverse_vol'
    weight_alpha_score: float = 2.5
    min_weight: float = 0.05
    max_weight: float = 0.20
    vol_fallback: float = 0.35
    vol_weight_floor: float = 0.18
    abs_mom_63d_min: float = 0.0
    abs_mom_252d_min: float = 0.0
    sector_max_names: int = 3

    def defensive_weight_map(self) -> dict[str, float]:
        return {k: float(v) for k, v in self.defensive_weights}


BASE_CONFIG = StrategyConfig(
    rebalance=getattr(backtest, 'REBALANCE', 'monthly'),
    selection=getattr(backtest, 'SELECTION', 'TOP'),
    top_n=getattr(backtest, 'TOP_N', 15),
    hold_buffer_n=getattr(backtest, 'HOLD_BUFFER_N', 25),
    transaction_cost=getattr(backtest, 'TRANSACTION_COST', 0.0015),
    benchmark=getattr(backtest, 'BENCHMARK', 'SPY'),
    vix_ticker=getattr(backtest, 'VIX_TICKER', '^VIX'),
    vix_crash_threshold=getattr(backtest, 'VIX_CRASH_THRESHOLD', 40.0),
    min_history=getattr(backtest, 'MIN_HISTORY', 220),
    regime_ma_window=getattr(backtest, 'REGIME_MA_WINDOW', 200),
    regime_mom_window=getattr(backtest, 'REGIME_MOM_WINDOW', 63),
    risk_on_exposure=getattr(backtest, 'RISK_ON_EXPOSURE', 1.0),
    mid_exposure=getattr(backtest, 'MID_EXPOSURE', 0.85),
    risk_off_exposure=getattr(backtest, 'RISK_OFF_EXPOSURE', 0.40),
    regime_buffer=getattr(backtest, 'REGIME_BUFFER', 0.005),
    regime_confirm_days=getattr(backtest, 'REGIME_CONFIRM_DAYS', 2),
    defensive_tickers=tuple(getattr(backtest, 'DEFENSIVE_TICKERS', ['TAIL', 'DBMF'])),
    defensive_weights=tuple((k, float(v)) for k, v in getattr(backtest, 'RISK_OFF_DEFENSIVE_WEIGHTS', {'TAIL':0.3,'DBMF':0.7}).items()),
    exposure_rebalance=getattr(backtest, 'EXPOSURE_REBALANCE', 'daily'),
    portfolio_weight_method=getattr(build, 'PORTFOLIO_WEIGHT_METHOD', 'score_x_inverse_vol'),
    weight_alpha_score=getattr(build, 'WEIGHT_ALPHA_SCORE', 2.5),
    min_weight=getattr(build, 'MIN_WEIGHT', 0.05),
    max_weight=getattr(build, 'MAX_WEIGHT', 0.20),
    vol_fallback=getattr(build, 'VOL_FALLBACK', 0.35),
    vol_weight_floor=getattr(build, 'VOL_WEIGHT_FLOOR', 0.18),
    abs_mom_63d_min=getattr(build, 'ABS_MOM_63D_MIN', 0.0),
    abs_mom_252d_min=getattr(build, 'ABS_MOM_252D_MIN', 0.0),
    sector_max_names=getattr(build, 'SECTOR_MAX_NAMES', 3),
)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def prepare_backtest_inputs(cfg: StrategyConfig = BASE_CONFIG, *, use_cache: bool = USE_BACKTEST_CACHE, precompute_snapshots: bool = PRECOMPUTE_SNAPSHOTS):
    current_universe = backtest.load_current_universe()
    membership_events = backtest.load_membership_events()
    tickers = list(current_universe.keys())

    if hasattr(backtest, 'load_or_download_prices'):
        price_map = backtest.load_or_download_prices(tickers, use_cache=use_cache)
    else:
        price_map = backtest.download_prices(tickers)

    if precompute_snapshots and hasattr(backtest, 'precompute_rebalance_snapshots'):
        benchmark = price_map[cfg.benchmark]
        if cfg.rebalance == 'monthly':
            stock_rebalance_dates = backtest.monthly_rebalance_dates(benchmark.index)
        else:
            stock_rebalance_dates = backtest.weekly_rebalance_dates(benchmark.index)
        backtest.precompute_rebalance_snapshots(
            price_map=price_map,
            rebalance_dates=list(stock_rebalance_dates),
            current_universe=current_universe,
            membership_events=membership_events,
            use_cache=use_cache,
        )

    return price_map, current_universe, membership_events


class GlobalPatcher:
    """Temporarily patch module-level configs so imported helper functions behave consistently."""

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self._saved: list[tuple[Any, str, Any]] = []

    def _set(self, module: Any, attr: str, value: Any) -> None:
        self._saved.append((module, attr, getattr(module, attr)))
        setattr(module, attr, value)

    def __enter__(self):
        cfg = self.cfg
        self._set(backtest, 'REBALANCE', cfg.rebalance)
        self._set(backtest, 'SELECTION', cfg.selection)
        self._set(backtest, 'TOP_N', cfg.top_n)
        self._set(backtest, 'HOLD_BUFFER_N', cfg.hold_buffer_n)
        self._set(backtest, 'TRANSACTION_COST', cfg.transaction_cost)
        self._set(backtest, 'BENCHMARK', cfg.benchmark)
        self._set(backtest, 'VIX_TICKER', cfg.vix_ticker)
        self._set(backtest, 'VIX_CRASH_THRESHOLD', cfg.vix_crash_threshold)
        self._set(backtest, 'MIN_HISTORY', cfg.min_history)
        self._set(backtest, 'REGIME_MA_WINDOW', cfg.regime_ma_window)
        self._set(backtest, 'REGIME_MOM_WINDOW', cfg.regime_mom_window)
        self._set(backtest, 'RISK_ON_EXPOSURE', cfg.risk_on_exposure)
        self._set(backtest, 'MID_EXPOSURE', cfg.mid_exposure)
        self._set(backtest, 'RISK_OFF_EXPOSURE', cfg.risk_off_exposure)
        self._set(backtest, 'REGIME_BUFFER', cfg.regime_buffer)
        self._set(backtest, 'REGIME_CONFIRM_DAYS', cfg.regime_confirm_days)
        self._set(backtest, 'DEFENSIVE_TICKERS', list(cfg.defensive_tickers))
        self._set(backtest, 'RISK_OFF_DEFENSIVE_WEIGHTS', cfg.defensive_weight_map())
        self._set(backtest, 'EXPOSURE_REBALANCE', cfg.exposure_rebalance)
        self._set(backtest, 'PORTFOLIO_WEIGHT_METHOD', cfg.portfolio_weight_method)
        self._set(backtest, 'WEIGHT_ALPHA_SCORE', cfg.weight_alpha_score)
        self._set(backtest, 'MIN_WEIGHT', cfg.min_weight)
        self._set(backtest, 'MAX_WEIGHT', cfg.max_weight)
        self._set(backtest, 'VOL_FALLBACK', cfg.vol_fallback)
        self._set(backtest, 'VOL_WEIGHT_FLOOR', cfg.vol_weight_floor)
        self._set(backtest, 'ABS_MOM_63D_MIN', cfg.abs_mom_63d_min)
        self._set(backtest, 'ABS_MOM_252D_MIN', cfg.abs_mom_252d_min)
        self._set(backtest, 'SECTOR_MAX_NAMES', cfg.sector_max_names)

        self._set(build, 'PORTFOLIO_WEIGHT_METHOD', cfg.portfolio_weight_method)
        self._set(build, 'WEIGHT_ALPHA_SCORE', cfg.weight_alpha_score)
        self._set(build, 'MIN_WEIGHT', cfg.min_weight)
        self._set(build, 'MAX_WEIGHT', cfg.max_weight)
        self._set(build, 'VOL_FALLBACK', cfg.vol_fallback)
        self._set(build, 'VOL_WEIGHT_FLOOR', cfg.vol_weight_floor)
        self._set(build, 'ABS_MOM_63D_MIN', cfg.abs_mom_63d_min)
        self._set(build, 'ABS_MOM_252D_MIN', cfg.abs_mom_252d_min)
        self._set(build, 'SECTOR_MAX_NAMES', cfg.sector_max_names)
        return self

    def __exit__(self, exc_type, exc, tb):
        for module, attr, value in reversed(self._saved):
            setattr(module, attr, value)
        return False


def annualized_turnover(weight_history: list[dict[str, float]], periods_per_year: int = 12) -> float:
    if len(weight_history) < 2:
        return 0.0
    diffs = []
    prev = weight_history[0]
    for cur in weight_history[1:]:
        keys = set(prev) | set(cur)
        diffs.append(sum(abs(float(cur.get(k, 0.0)) - float(prev.get(k, 0.0))) for k in keys))
        prev = cur
    return float(np.mean(diffs) * periods_per_year)


def downside_deviation(returns: pd.Series) -> float:
    neg = returns[returns < 0]
    if neg.empty:
        return 0.0
    return float(neg.std(ddof=0) * np.sqrt(252))


def sortino_ratio(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    dd = downside_deviation(returns)
    if dd <= 0:
        return 0.0
    cagr = (1.0 + returns).prod() ** (252.0 / len(returns)) - 1.0
    return float(cagr / dd)


def calmar_ratio(cagr: float, mdd: float) -> float:
    if mdd >= 0:
        return 0.0
    return float(cagr / abs(mdd))


def information_ratio(strategy_returns: pd.Series, benchmark_returns: pd.Series) -> float:
    aligned = pd.concat([strategy_returns, benchmark_returns], axis=1, join='inner').dropna()
    if aligned.empty:
        return 0.0
    diff = aligned.iloc[:, 0] - aligned.iloc[:, 1]
    te = float(diff.std(ddof=0) * np.sqrt(252))
    if te <= 0:
        return 0.0
    alpha = float(diff.mean() * 252)
    return alpha / te


def max_underwater_days(drawdown: pd.Series) -> int:
    max_run = 0
    run = 0
    for v in drawdown.fillna(0.0):
        if v < 0:
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0
    return int(max_run)


def trailing_equity_metrics(df: pd.DataFrame, benchmark_df: pd.DataFrame | None = None) -> dict[str, float]:
    df = df.copy()
    returns = df['daily_return'].fillna(0.0)
    equity = float(df['equity'].iloc[-1])
    years = max(len(df) / 252.0, 1.0 / 252.0)
    cagr = equity ** (1.0 / years) - 1.0
    vol = float(returns.std(ddof=0) * np.sqrt(252))
    sharpe = float(cagr / vol) if vol > 0 else 0.0
    mdd = float(df['drawdown'].min())
    out = {
        'total_return': float(equity - 1.0),
        'cagr': float(cagr),
        'volatility': float(vol),
        'sharpe': float(sharpe),
        'sortino': float(sortino_ratio(returns)),
        'calmar': float(calmar_ratio(cagr, mdd)),
        'max_drawdown': float(mdd),
        'max_underwater_days': float(max_underwater_days(df['drawdown'])),
        'avg_daily_return': float(returns.mean()),
        'daily_hit_rate': float((returns > 0).mean()),
    }
    if benchmark_df is not None and not benchmark_df.empty:
        bench_ret = benchmark_df['daily_return'].fillna(0.0)

        aligned = pd.concat(
            [
                returns.rename('strategy'),
                bench_ret.rename('benchmark'),
            ],
            axis=1,
            join='inner',
        ).dropna()

        if not aligned.empty:
            strat = aligned['strategy']
            bench = aligned['benchmark']
            rel = strat - bench

            bench_var = float(np.var(bench))
            beta = float(np.cov(strat, bench)[0, 1] / bench_var) if bench_var > 0 else 0.0

            up_mask = bench > 0
            down_mask = bench < 0

            up_capture = 0.0
            if up_mask.any():
                bench_up_mean = float(bench.loc[up_mask].mean())
                strat_up_mean = float(strat.loc[up_mask].mean())
                if abs(bench_up_mean) > 1e-12:
                    up_capture = float(strat_up_mean / bench_up_mean)

            down_capture = 0.0
            if down_mask.any():
                bench_down_mean = float(bench.loc[down_mask].mean())
                strat_down_mean = float(strat.loc[down_mask].mean())
                if abs(bench_down_mean) > 1e-12:
                    down_capture = float(strat_down_mean / bench_down_mean)

            out.update({
                'alpha_daily_mean': float(rel.mean()),
                'information_ratio': float(information_ratio(returns, bench_ret)),
                'beta_to_benchmark': beta,
                'up_capture_proxy': up_capture,
                'down_capture_proxy': down_capture,
            })
    return out


def date_filter(series: pd.Series, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.Series:
    out = series
    if start is not None:
        out = out[out.index >= start]
    if end is not None:
        out = out[out.index <= end]
    return out


def run_single_backtest(
    cfg: StrategyConfig,
    *,
    price_map: dict[str, pd.Series] | None = None,
    current_universe: dict[str, dict[str, Any]] | None = None,
    membership_events: list[dict[str, Any]] | None = None,
    start_date: str | pd.Timestamp | None = None,
    end_date: str | pd.Timestamp | None = None,
    label: str = 'baseline',
    use_cache: bool = USE_BACKTEST_CACHE,
) -> dict[str, Any]:
    with GlobalPatcher(cfg):
        current_universe = current_universe or backtest.load_current_universe()
        membership_events = membership_events or backtest.load_membership_events()
        tickers = list(current_universe.keys())
        if price_map is None:
            if hasattr(backtest, 'load_or_download_prices'):
                price_map = backtest.load_or_download_prices(tickers, use_cache=use_cache)
            else:
                price_map = backtest.download_prices(tickers)

        if cfg.benchmark not in price_map or cfg.vix_ticker not in price_map:
            raise RuntimeError('Benchmark or VIX data missing in price_map')

        benchmark_series = date_filter(price_map[cfg.benchmark], pd.to_datetime(start_date) if start_date is not None else None, pd.to_datetime(end_date) if end_date is not None else None)
        if benchmark_series.empty:
            raise ValueError('Benchmark series is empty for requested window')
        vix_series = date_filter(price_map[cfg.vix_ticker], pd.to_datetime(start_date) if start_date is not None else None, pd.to_datetime(end_date) if end_date is not None else None)
        trading_dates = benchmark_series.index

        if cfg.rebalance == 'monthly':
            stock_rebalance_dates = set(backtest.monthly_rebalance_dates(trading_dates))
            periods_per_year = 12
        else:
            stock_rebalance_dates = set(backtest.weekly_rebalance_dates(trading_dates))
            periods_per_year = 52

        if cfg.exposure_rebalance == 'daily':
            exposure_rebalance_dates = set(trading_dates)
        elif cfg.exposure_rebalance == 'weekly':
            exposure_rebalance_dates = set(backtest.weekly_rebalance_dates(trading_dates))
        else:
            exposure_rebalance_dates = set(backtest.monthly_rebalance_dates(trading_dates))

        current_stock_holdings: dict[str, float] = {}
        current_stock_holdings_list: list[str] = []
        current_stock_exposure: float = cfg.risk_on_exposure
        current_defensive_holdings: dict[str, float] = {}
        current_total_holdings: dict[str, float] = {}

        pending_stock_holdings: dict[str, float] | None = None
        pending_stock_holdings_list: list[str] | None = None
        pending_stock_exposure: float | None = None
        pending_total_holdings: dict[str, float] | None = None
        pending_defensive_holdings: dict[str, float] | None = None
        pending_regime_meta: dict[str, Any] | None = None
        pending_cost: bool = False

        last_regime_meta: dict[str, Any] = {
            'trend_ok': True,
            'momentum_ok': True,
            'regime_bucket': 'risk_on',
            'candidate_bucket': 'risk_on',
            'confirmed': True,
        }

        equity = 1.0
        high = 1.0
        history: list[dict[str, Any]] = []
        rebalance_weight_history: list[dict[str, float]] = []
        turnover_events: list[dict[str, Any]] = []

        for date in trading_dates:
            if pending_total_holdings is not None:
                if pending_cost:
                    equity *= (1.0 - cfg.transaction_cost)

                prev_map = current_total_holdings.copy()
                current_stock_holdings = pending_stock_holdings or {}
                current_stock_holdings_list = pending_stock_holdings_list or []
                current_stock_exposure = float(pending_stock_exposure if pending_stock_exposure is not None else current_stock_exposure)
                current_total_holdings = pending_total_holdings
                current_defensive_holdings = pending_defensive_holdings or {}
                rebalance_weight_history.append(current_total_holdings.copy())

                if pending_regime_meta is not None:
                    last_regime_meta = pending_regime_meta

                keys = set(prev_map) | set(current_total_holdings)
                turnover = sum(abs(float(current_total_holdings.get(k, 0.0)) - float(prev_map.get(k, 0.0))) for k in keys)
                turnover_events.append({
                    'date': str(date.date()),
                    'turnover': float(turnover),
                    'cost_applied': bool(pending_cost),
                    'holdings_count': len(current_stock_holdings),
                    'regime_bucket': str(last_regime_meta.get('regime_bucket', 'risk_on')),
                })

                pending_stock_holdings = None
                pending_stock_holdings_list = None
                pending_stock_exposure = None
                pending_total_holdings = None
                pending_defensive_holdings = None
                pending_regime_meta = None
                pending_cost = False

            daily_ret = backtest.compute_daily_return(price_map, current_total_holdings, date)
            equity *= (1.0 + daily_ret)
            high = max(high, equity)
            dd = equity / high - 1.0

            history.append({
                'date': pd.Timestamp(date),
                'equity': float(equity),
                'daily_return': float(daily_ret),
                'drawdown': float(dd),
                'stock_exposure': float(current_stock_exposure),
                'defensive_exposure': float(1.0 - current_stock_exposure),
                'trend_ok': bool(last_regime_meta.get('trend_ok', True)),
                'momentum_ok': bool(last_regime_meta.get('momentum_ok', True)),
                'candidate_bucket': str(last_regime_meta.get('candidate_bucket', 'risk_on')),
                'regime_bucket': str(last_regime_meta.get('regime_bucket', 'risk_on')),
                'regime_confirmed': bool(last_regime_meta.get('confirmed', True)),
                'vix_crash_active': bool(last_regime_meta.get('is_vix_crash', False)),
                'holdings_count': len(current_stock_holdings),
                'holdings': list(current_stock_holdings_list),
                'stock_weights': dict(current_stock_holdings),
                'defensive_weights': dict(current_defensive_holdings),
                'total_weights': dict(current_total_holdings),
            })

            target_stock_holdings = current_stock_holdings
            target_stock_holdings_list = current_stock_holdings_list
            target_stock_exposure = current_stock_exposure
            target_regime_meta = last_regime_meta

            if date in stock_rebalance_dates:
                if hasattr(backtest, 'get_rebalance_snapshot'):
                    snapshot = backtest.get_rebalance_snapshot(
                        price_map=price_map,
                        date=date,
                        current_universe=current_universe,
                        membership_events=membership_events,
                        use_cache=use_cache,
                    )
                    scored = snapshot['scored']
                    dynamic_floor = float(snapshot['dynamic_floor'])
                else:
                    sliced = backtest.slice_price_map(price_map, date)
                    membership = backtest.reconstruct_membership_as_of(as_of=date, current_universe=current_universe, events=membership_events)
                    eligible_prices = backtest.filter_price_map_by_membership(sliced, membership)
                    if cfg.benchmark in sliced:
                        eligible_prices[cfg.benchmark] = sliced[cfg.benchmark]
                    by_ticker_for_date = {
                        t: current_universe.get(t, {'ticker': t, 'sector': 'Unknown', 'index_flags': ['sp500', 'sp400', 'sp600']})
                        for t in eligible_prices.keys()
                    }
                    metrics = build.build_metrics_for_group(list(eligible_prices.keys()), by_ticker_for_date, eligible_prices)
                    scored = build.score_group(metrics, quality_score_map=None)

                    spy_hist = sliced.get(cfg.benchmark)
                    if spy_hist is not None and len(spy_hist) >= 20:
                        spy_vol20 = build.annualized_volatility(spy_hist, 20)
                        if spy_vol20 is None:
                            spy_vol20 = cfg.vol_weight_floor
                    else:
                        spy_vol20 = cfg.vol_weight_floor
                    dynamic_floor = max(cfg.vol_weight_floor, float(spy_vol20))
                selected_rows = backtest.pick_portfolio(scored, current_holdings=set(current_stock_holdings.keys()), dynamic_floor=dynamic_floor)
                target_stock_holdings = backtest.holdings_to_weight_map(selected_rows)
                target_stock_holdings_list = sorted(target_stock_holdings.keys())

            if date in exposure_rebalance_dates:
                prev_bucket = str(last_regime_meta.get('regime_bucket', 'risk_on'))
                target_stock_exposure, target_regime_meta = backtest.regime_exposure_with_vix(
                    benchmark_series=benchmark_series,
                    vix_series=vix_series,
                    date=date,
                    prev_bucket=prev_bucket,
                )

            target_total_holdings, target_defensive_holdings = backtest.build_total_holdings(
                stock_holdings=target_stock_holdings,
                stock_exposure=target_stock_exposure,
            )

            will_change = not backtest.weight_maps_equal(target_total_holdings, current_total_holdings)
            pending_stock_holdings = target_stock_holdings
            pending_stock_holdings_list = target_stock_holdings_list
            pending_stock_exposure = target_stock_exposure
            pending_total_holdings = target_total_holdings
            pending_defensive_holdings = target_defensive_holdings
            pending_regime_meta = target_regime_meta
            pending_cost = will_change

        df = pd.DataFrame(history)
        if df.empty:
            raise ValueError('Backtest history is empty')
        df['date'] = pd.to_datetime(df['date'])

        bench_df = backtest.build_benchmark_df(benchmark_series)
        bench_df['date'] = pd.to_datetime(bench_df['date'])

        metrics = trailing_equity_metrics(df, bench_df)
        metrics.update({
            'annualized_turnover': annualized_turnover(rebalance_weight_history, periods_per_year=periods_per_year),
            'avg_holdings_count': float(df['holdings_count'].mean()),
            'risk_off_fraction': float((df['regime_bucket'] == 'risk_off').mean()),
            'mid_fraction': float((df['regime_bucket'] == 'mid').mean()),
            'risk_on_fraction': float((df['regime_bucket'] == 'risk_on').mean()),
            'vix_crash_fraction': float(df['vix_crash_active'].mean()),
            'trade_events': float(len(turnover_events)),
        })

        subperiods = []
        for years in [1, 3, 5, 10]:
            n = years * 252
            if len(df) >= max(60, n // 2):
                sub_df = df.tail(n).copy() if len(df) > n else df.copy()
                sub_bench = bench_df.tail(len(sub_df)).copy()
                start_eq = float(sub_df['equity'].iloc[0])
                if start_eq > 0:
                    sub_df['equity'] = sub_df['equity'] / start_eq
                    sub_df['drawdown'] = sub_df['equity'] / sub_df['equity'].cummax() - 1.0
                    subperiods.append({'label': f'{years}y', 'metrics': trailing_equity_metrics(sub_df, sub_bench)})

        return {
            'label': label,
            'config': asdict(cfg),
            'history': df,
            'benchmark': bench_df,
            'metrics': metrics,
            'subperiods': subperiods,
            'turnover_events': pd.DataFrame(turnover_events),
        }


def summarize_result(result: dict[str, Any]) -> dict[str, Any]:
    out = dict(result['metrics'])
    out['label'] = result['label']
    out['start_date'] = str(result['history']['date'].min().date())
    out['end_date'] = str(result['history']['date'].max().date())
    return out


def score_objective(metrics: dict[str, float]) -> float:
    return (
        0.45 * metrics.get('sharpe', 0.0)
        + 0.30 * metrics.get('cagr', 0.0)
        - 0.20 * abs(metrics.get('max_drawdown', 0.0))
        - 0.05 * metrics.get('annualized_turnover', 0.0)
    )


def param_product(grid: dict[str, list[Any]]) -> Iterable[dict[str, Any]]:
    keys = list(grid.keys())
    values = [grid[k] for k in keys]
    for combo in itertools.product(*values):
        yield {k: v for k, v in zip(keys, combo)}


def run_ablation_suite(base_cfg: StrategyConfig, price_map, current_universe, membership_events, start_date=None, end_date=None) -> pd.DataFrame:
    variants: list[tuple[str, StrategyConfig]] = [
        ('Master', base_cfg),
        ('Equal Weight', replace(base_cfg, portfolio_weight_method='equal_weight')),
        ('No Sector Cap', replace(base_cfg, sector_max_names=999)),
        ('Cash In Risk-Off', replace(base_cfg, defensive_tickers=tuple(), defensive_weights=tuple())),
        ('Monthly Regime', replace(base_cfg, exposure_rebalance='monthly')),
        ('Weekly Stock Rebalance', replace(base_cfg, rebalance='weekly')),
        ('No Hysteresis', replace(base_cfg, hold_buffer_n=base_cfg.top_n, regime_buffer=0.0, regime_confirm_days=1)),
        ('Higher Risk-Off Exposure', replace(base_cfg, risk_off_exposure=0.55)),
    ]
    rows = []
    for label, cfg in variants:
        res = run_single_backtest(cfg, price_map=price_map, current_universe=current_universe, membership_events=membership_events, start_date=start_date, end_date=end_date, label=label)
        row = summarize_result(res)
        row['objective'] = score_objective(res['metrics'])
        rows.append(row)
    return pd.DataFrame(rows).sort_values('objective', ascending=False).reset_index(drop=True)


def run_sensitivity_grid(base_cfg: StrategyConfig, price_map, current_universe, membership_events, start_date=None, end_date=None) -> pd.DataFrame:
    grid = {
        'top_n': [10, 15, 20],
        'abs_mom_63d_min': [-0.05, 0.0, 0.05],
        'sector_max_names': [2, 3, 4],
        'risk_off_exposure': [0.40, 0.55],
    }
    rows = []
    for params in param_product(grid):
        cfg = replace(base_cfg, **params)
        label = '|'.join(f'{k}={v}' for k, v in params.items())
        res = run_single_backtest(cfg, price_map=price_map, current_universe=current_universe, membership_events=membership_events, start_date=start_date, end_date=end_date, label=label)
        row = summarize_result(res)
        row.update(params)
        row['objective'] = score_objective(res['metrics'])
        rows.append(row)
    return pd.DataFrame(rows).sort_values('objective', ascending=False).reset_index(drop=True)


def yearly_summary(df: pd.DataFrame, bench_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(df[['date', 'daily_return']], bench_df[['date', 'daily_return']], on='date', how='inner', suffixes=('_strategy', '_benchmark'))
    merged['year'] = merged['date'].dt.year
    rows = []
    for year, group in merged.groupby('year'):
        strat = (1.0 + group['daily_return_strategy']).prod() - 1.0
        bench = (1.0 + group['daily_return_benchmark']).prod() - 1.0
        rows.append({'year': int(year), 'strategy_return': float(strat), 'benchmark_return': float(bench), 'active_return': float(strat - bench)})
    return pd.DataFrame(rows)


def run_walk_forward(
    base_cfg: StrategyConfig,
    price_map,
    current_universe,
    membership_events,
    *,
    train_years: int = 5,
    test_years: int = 1,
    holdout_start: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    benchmark = price_map[base_cfg.benchmark]
    dates = benchmark.index
    start_year = int(dates.min().year)
    end_year = int(dates.max().year)
    rows = []
    candidate_grid = {
        'top_n': [10, 15],
        'sector_max_names': [2, 3],
        'abs_mom_63d_min': [-0.05, 0.0],
        'risk_off_exposure': [0.40, 0.55],
        'portfolio_weight_method': ['equal_weight', 'score_x_inverse_vol'],
    }
    holdout_year = pd.to_datetime(holdout_start).year if holdout_start else end_year + 1

    for test_start_year in range(start_year + train_years, min(holdout_year, end_year + 1), test_years):
        train_start = pd.Timestamp(f'{test_start_year - train_years}-01-01')
        train_end = pd.Timestamp(f'{test_start_year - 1}-12-31')
        test_start = pd.Timestamp(f'{test_start_year}-01-01')
        test_end = pd.Timestamp(f'{min(test_start_year + test_years - 1, end_year)}-12-31')
        if test_start > dates.max():
            break

        best_cfg = None
        best_score = -1e18
        best_train_metrics = None
        for params in param_product(candidate_grid):
            cfg = replace(base_cfg, **params)
            train_res = run_single_backtest(cfg, price_map=price_map, current_universe=current_universe, membership_events=membership_events, start_date=train_start, end_date=train_end, label='train')
            obj = score_objective(train_res['metrics'])
            if obj > best_score:
                best_score = obj
                best_cfg = cfg
                best_train_metrics = train_res['metrics']

        if best_cfg is None:
            continue

        test_res = run_single_backtest(best_cfg, price_map=price_map, current_universe=current_universe, membership_events=membership_events, start_date=test_start, end_date=test_end, label='test')
        row = {
            'train_start': str(train_start.date()),
            'train_end': str(train_end.date()),
            'test_start': str(test_start.date()),
            'test_end': str(test_end.date()),
            'selected_top_n': best_cfg.top_n,
            'selected_sector_max_names': best_cfg.sector_max_names,
            'selected_abs_mom_63d_min': best_cfg.abs_mom_63d_min,
            'selected_risk_off_exposure': best_cfg.risk_off_exposure,
            'selected_weight_method': best_cfg.portfolio_weight_method,
            'train_objective': float(best_score),
            'train_cagr': float(best_train_metrics['cagr']),
            'train_sharpe': float(best_train_metrics['sharpe']),
            'train_mdd': float(best_train_metrics['max_drawdown']),
            'oos_cagr': float(test_res['metrics']['cagr']),
            'oos_sharpe': float(test_res['metrics']['sharpe']),
            'oos_mdd': float(test_res['metrics']['max_drawdown']),
            'oos_turnover': float(test_res['metrics']['annualized_turnover']),
        }
        rows.append(row)

    wf_df = pd.DataFrame(rows)
    summary = pd.DataFrame([{
        'folds': len(wf_df),
        'oos_cagr_mean': float(wf_df['oos_cagr'].mean()) if not wf_df.empty else 0.0,
        'oos_cagr_median': float(wf_df['oos_cagr'].median()) if not wf_df.empty else 0.0,
        'oos_sharpe_mean': float(wf_df['oos_sharpe'].mean()) if not wf_df.empty else 0.0,
        'oos_mdd_mean': float(wf_df['oos_mdd'].mean()) if not wf_df.empty else 0.0,
        'positive_oos_rate': float((wf_df['oos_cagr'] > 0).mean()) if not wf_df.empty else 0.0,
    }])
    return wf_df, summary


def run_holdout(base_cfg: StrategyConfig, price_map, current_universe, membership_events, holdout_start: str) -> dict[str, Any]:
    holdout_start_ts = pd.to_datetime(holdout_start)
    full_dates = price_map[base_cfg.benchmark].index
    holdout_end = full_dates.max()
    return run_single_backtest(base_cfg, price_map=price_map, current_universe=current_universe, membership_events=membership_events, start_date=holdout_start_ts, end_date=holdout_end, label='holdout')


def block_bootstrap_relative_returns(strategy_df: pd.DataFrame, benchmark_df: pd.DataFrame, n_boot: int = 500, block_size: int = 20, seed: int = 42) -> pd.DataFrame:
    merged = pd.merge(strategy_df[['date', 'daily_return']], benchmark_df[['date', 'daily_return']], on='date', suffixes=('_strategy', '_benchmark'))
    rel = (merged['daily_return_strategy'] - merged['daily_return_benchmark']).to_numpy(dtype=float)
    if len(rel) == 0:
        return pd.DataFrame()
    rng = np.random.default_rng(seed)
    stats = []
    for _ in range(n_boot):
        sample = []
        while len(sample) < len(rel):
            start = int(rng.integers(0, max(1, len(rel) - block_size + 1)))
            sample.extend(rel[start:start + block_size].tolist())
        sample = np.array(sample[:len(rel)], dtype=float)
        equity = float(np.prod(1.0 + sample))
        years = max(len(sample) / 252.0, 1.0 / 252.0)
        cagr = equity ** (1.0 / years) - 1.0
        vol = float(np.std(sample, ddof=0) * np.sqrt(252))
        ir = float(cagr / vol) if vol > 0 else 0.0
        stats.append({'relative_total_return': equity - 1.0, 'relative_cagr': cagr, 'relative_ir_like': ir})
    return pd.DataFrame(stats)


def save_plot(fig, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches='tight')
    plt.close(fig)


def plot_equity_curve(strategy_df: pd.DataFrame, benchmark_df: pd.DataFrame, path: Path, title: str) -> None:
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(strategy_df['date'], strategy_df['equity'], label='Strategy')
    ax.plot(benchmark_df['date'], benchmark_df['equity'], label='Benchmark')
    ax.set_title(title)
    ax.set_ylabel('Equity')
    ax.legend()
    save_plot(fig, path)


def plot_ablation(df: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5))
    plot_df = df.sort_values('objective', ascending=True)
    ax.barh(plot_df['label'], plot_df['objective'])
    ax.set_title('Ablation Objective Score')
    ax.set_xlabel('Objective')
    save_plot(fig, path)


def plot_heatmap(df: pd.DataFrame, path: Path, value_col: str = 'objective') -> None:
    pivot = df.pivot_table(index='top_n', columns='abs_mom_63d_min', values=value_col, aggfunc='mean')
    fig, ax = plt.subplots(figsize=(8, 5))
    im = ax.imshow(pivot.values, aspect='auto')
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([str(c) for c in pivot.columns])
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([str(i) for i in pivot.index])
    ax.set_xlabel('ABS_MOM_63D_MIN')
    ax.set_ylabel('TOP_N')
    ax.set_title(f'Sensitivity Heatmap ({value_col})')
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            val = pivot.iloc[i, j]
            ax.text(j, i, f'{val:.2f}', ha='center', va='center', fontsize=8)
    fig.colorbar(im, ax=ax)
    save_plot(fig, path)


def plot_walk_forward(df: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5))
    if not df.empty:
        labels = [f"{a[:4]}→{b[:4]}" for a, b in zip(df['train_end'], df['test_end'])]
        ax.bar(range(len(df)), df['oos_cagr'])
        ax.set_xticks(range(len(df)))
        ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_title('Walk-Forward OOS CAGR by Fold')
    ax.set_ylabel('OOS CAGR')
    save_plot(fig, path)


def plot_yearly_active(yearly_df: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5))
    if not yearly_df.empty:
        ax.bar(yearly_df['year'].astype(str), yearly_df['active_return'])
    ax.set_title('Yearly Active Return vs Benchmark')
    ax.set_ylabel('Strategy - Benchmark')
    save_plot(fig, path)


def plot_bootstrap(boot_df: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    if not boot_df.empty:
        ax.hist(boot_df['relative_cagr'], bins=30)
    ax.set_title('Bootstrap Relative CAGR Distribution')
    ax.set_xlabel('Relative CAGR')
    save_plot(fig, path)


def write_report(path: Path, sections: list[str]) -> None:
    path.write_text('\n\n'.join(sections), encoding='utf-8')


def main() -> None:
    ensure_dir(OUTPUT_DIR)

    price_map, current_universe, membership_events = prepare_backtest_inputs(BASE_CONFIG, use_cache=USE_BACKTEST_CACHE, precompute_snapshots=PRECOMPUTE_SNAPSHOTS)

    # 1) Full-period baseline
    baseline = run_single_backtest(BASE_CONFIG, price_map=price_map, current_universe=current_universe, membership_events=membership_events, label='Full Period Baseline')
    baseline_summary = pd.DataFrame([summarize_result(baseline)])
    baseline_summary.to_csv(OUTPUT_DIR / 'baseline_metrics.csv', index=False)
    baseline['history'].to_csv(OUTPUT_DIR / 'baseline_history.csv', index=False)
    baseline['benchmark'].to_csv(OUTPUT_DIR / 'baseline_benchmark.csv', index=False)
    baseline['turnover_events'].to_csv(OUTPUT_DIR / 'baseline_turnover_events.csv', index=False)

    # 2) Ablation
    ablation_df = run_ablation_suite(BASE_CONFIG, price_map, current_universe, membership_events)
    ablation_df.to_csv(OUTPUT_DIR / 'ablation_results.csv', index=False)

    # 3) Sensitivity
    sensitivity_df = run_sensitivity_grid(BASE_CONFIG, price_map, current_universe, membership_events)
    sensitivity_df.to_csv(OUTPUT_DIR / 'sensitivity_results.csv', index=False)

    # 4) Walk-forward and untouched holdout
    holdout_start = '2024-01-01'
    wf_df, wf_summary = run_walk_forward(BASE_CONFIG, price_map, current_universe, membership_events, train_years=5, test_years=1, holdout_start=holdout_start)
    wf_df.to_csv(OUTPUT_DIR / 'walk_forward_results.csv', index=False)
    wf_summary.to_csv(OUTPUT_DIR / 'walk_forward_summary.csv', index=False)
    holdout = run_holdout(BASE_CONFIG, price_map, current_universe, membership_events, holdout_start=holdout_start)
    pd.DataFrame([summarize_result(holdout)]).to_csv(OUTPUT_DIR / 'holdout_metrics.csv', index=False)

    # 5) Yearly breakdown and bootstrap
    yearly_df = yearly_summary(baseline['history'], baseline['benchmark'])
    yearly_df.to_csv(OUTPUT_DIR / 'yearly_active_returns.csv', index=False)
    boot_df = block_bootstrap_relative_returns(baseline['history'], baseline['benchmark'], n_boot=500, block_size=20)
    boot_df.to_csv(OUTPUT_DIR / 'bootstrap_relative_returns.csv', index=False)

    # Plots
    plot_equity_curve(baseline['history'], baseline['benchmark'], OUTPUT_DIR / 'baseline_equity_curve.png', 'Baseline Equity Curve')
    plot_ablation(ablation_df, OUTPUT_DIR / 'ablation_objective.png')
    plot_heatmap(sensitivity_df, OUTPUT_DIR / 'sensitivity_heatmap.png', value_col='objective')
    plot_walk_forward(wf_df, OUTPUT_DIR / 'walk_forward_oos_cagr.png')
    plot_yearly_active(yearly_df, OUTPUT_DIR / 'yearly_active_return.png')
    plot_bootstrap(boot_df, OUTPUT_DIR / 'bootstrap_relative_cagr.png')
    plot_equity_curve(holdout['history'], holdout['benchmark'], OUTPUT_DIR / 'holdout_equity_curve.png', 'Untouched Holdout Equity Curve')

    report_sections = [
        '# Quant Validation Report',
        '## What this script checks\n- Full-period baseline\n- Ablation variants\n- Parameter sensitivity grid\n- Rolling walk-forward IS/OOS selection\n- Untouched holdout period\n- Turnover and regime diagnostics\n- Block bootstrap for relative return stability',
        '## Current-code caveats\n- Historical backtest uses quality_score_map=None, so quality is not included in the actual historical backtest.\n- Snapshot scoring applies local news filtering, but historical backtest does not apply the same news filter.\n- The snapshot script contains an analyze_news_from_local() path referencing RED_FLAG_REGEX, which is undefined in the uploaded file and should be fixed before relying on that function.',
        '## Output files\nSee the CSV, JSON-compatible CSV tables, and PNG charts inside validation_outputs/.',
    ]
    write_report(OUTPUT_DIR / 'README_validation.md', report_sections)

    summary = {
        'baseline_metrics': summarize_result(baseline),
        'best_ablation_row': ablation_df.iloc[0].to_dict() if not ablation_df.empty else {},
        'best_sensitivity_row': sensitivity_df.iloc[0].to_dict() if not sensitivity_df.empty else {},
        'walk_forward_summary': wf_summary.iloc[0].to_dict() if not wf_summary.empty else {},
        'holdout_metrics': summarize_result(holdout),
        'bootstrap_relative_cagr_p05': float(boot_df['relative_cagr'].quantile(0.05)) if not boot_df.empty else 0.0,
        'bootstrap_relative_cagr_p50': float(boot_df['relative_cagr'].quantile(0.50)) if not boot_df.empty else 0.0,
        'bootstrap_relative_cagr_p95': float(boot_df['relative_cagr'].quantile(0.95)) if not boot_df.empty else 0.0,
    }
    (OUTPUT_DIR / 'validation_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'[OK] Validation outputs saved to: {OUTPUT_DIR}')


if __name__ == '__main__':
    main()