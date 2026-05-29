Goal:

Create a long-running quantitative research pipeline to discover rare, high-upside stock trading events using broker buy/sell daily reports, OHLC data, and warrant-to-underlying-stock mapping.

The goal is not to find a high-frequency strategy with many trades. Instead, the goal is to identify low-frequency, high-conviction event patterns that may occur rarely but can produce large forward returns or large MFE after the signal appears.

The pipeline should search for asymmetric payoff opportunities:
- few but meaningful trades,
- large average upside,
- high MFE,
- controlled downside,
- favorable payoff ratio,
- clear event logic,
- and evidence that the pattern is not purely random or overfit.

The main research question is:

Can extreme broker accumulation, especially when combined with price compression, low prior price reaction, and warrant activity confirmation or divergence, identify rare stock events with unusually high upside potential?

The system should build stock_id/date-level features from:
1. OHLC data,
2. stock broker buy/sell reports,
3. warrant broker buy/sell reports aggregated back to the underlying stock,
4. warrant-to-underlying-stock mapping.

Feature groups should include:
- OHLC returns, volatility, volume ratio, breakout, price position, compression, drawdown,
- stock broker net buy, concentration, persistence, abnormal percentile, top broker coverage,
- warrant activity, warrant net buy, warrant concentration, active warrant count, warrant abnormal percentile,
- cross features comparing stock chip strength and warrant activity.

The pipeline should generate and test rare event candidates such as:
1. extreme broker accumulation before price movement,
2. stealth accumulation where stock chip is strong but warrant activity is not yet hot,
3. stock accumulation followed by warrant confirmation,
4. accumulation during price compression,
5. warrant activity leading stock movement,
6. broker accumulation followed by price breakout.

The search should prefer strict event definitions that produce fewer but higher-quality signals.

A candidate rare-event strategy is considered promising if it satisfies:

Full backtest:
- total trades >= 20
- average net return per trade > 5%
- median net return per trade > 1%
- average MFE_20d >= 8%
- average MFE_40d >= 12%
- at least 20% of trades reach MFE_20d >= 10%
- at least 10% of trades reach MFE_40d >= 20%
- profit factor >= 1.8
- payoff ratio >= 2.0
- average MFE_20d / abs(average MAE_20d) >= 1.5
- no single trade loss worse than -15%

Out-of-sample test:
- test trades >= 5
- average net return per trade > 3%
- average MFE_20d >= 6%
- profit factor >= 1.3
- payoff ratio >= 1.5
- no catastrophic loss worse than -15%

The strategy ranking should prioritize:
- high average MFE,
- high hit rate of MFE >= 10% and MFE >= 20%,
- high payoff ratio,
- controlled MAE,
- low prior price reaction before entry,
- robust behavior across nearby thresholds,
- explainable event logic.

The ranking should not prioritize trade count or total return. A strategy with fewer trades but much better asymmetry should rank above a frequent strategy with small average returns.

Important constraints:
- No look-ahead bias.
- Entry must occur after the signal is known.
- Transaction costs must be included.
- Feature construction must use only data available on or before the signal date.
- Future returns, MFE, and MAE can only be used for labels and evaluation.
- Avoid strategies that only work because of one or two extreme trades.
- Report both successful and failed event families.

The system must output:
- outputs/analysis/rare_event/features_stock_daily.parquet
- outputs/analysis/rare_event/rare_event_signals.parquet
- outputs/analysis/rare_event/trades.parquet
- outputs/analysis/rare_event/rare_event_leaderboard.csv
- outputs/analysis/rare_event/best_rare_event_report.md
- outputs/analysis/rare_event/rejected_rare_events.csv

The best_rare_event_report.md must include:
- event name,
- entry rule,
- exit rule,
- number of trades,
- average net return,
- median net return,
- win rate,
- profit factor,
- payoff ratio,
- average MFE and MAE,
- MFE >= 10% hit rate,
- MFE >= 20% hit rate,
- train/validation/test performance,
- parameter sensitivity,
- examples of the best historical events,
- examples of failed events,
- explanation of why the event may work,
- risks and failure modes.

If no event pattern passes all criteria, report the best partial candidates and clearly explain which criteria failed.
