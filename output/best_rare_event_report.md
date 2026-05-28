# Rare Event Strategy Report

## Best Event: accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p05__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss--0p1__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p5

- profit factor: `4.3033`
- payoff ratio: `2.8402`
- event name: `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p05__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss--0p1__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p5`
- split definition: `train <= 2025-09-30`, `valid = 2025-10-01 ~ 2025-11-15`, `test = 2025-11-16 ~ 2025-12-15`
- entry rule: next open after signal; strict event filter defined by `{"breakout_gap_20_cap": 0.05, "cooldown_days": 15, "family": "accumulation_pre_breakout", "hold_days": 30, "stock_net_buy_days_20": 3, "stock_posnet_pct_cs": 0.98, "stop_loss": -0.1, "volume_ratio_5_20_cap": 1.5, "warrant_posnet_floor": 0.5}`
- exit rule: `stop_loss=-0.1` if hit, otherwise close after `30` trading days
- number of trades: `83`
- average net return: `0.1034`
- median net return: `0.0552`
- win rate: `0.6024`
- average MFE and MAE: `mfe20=0.2012`, `mfe40=0.3060`, `mae20=-0.0660`
- MFE >= 10% hit rate: `0.6747`
- MFE >= 20% hit rate: `0.5301`
- train/validation/test performance: `train=0.0924`, `valid=0.0677`, `test=0.2220`
- parameter sensitivity: neighboring-family robustness not yet quantified; use leaderboard rows around `accumulation_pre_breakout` as first-pass sensitivity check
- examples of the best historical events:
  - `2025-12-12 2337`: `net_ret=1.2571`, `mfe20=0.7312`, `mfe40=1.6644`, `mae20=-0.0303`
  - `2025-09-08 1504`: `net_ret=0.5635`, `mfe20=0.6317`, `mfe40=0.7409`, `mae20=-0.0169`
  - `2025-12-18 2408`: `net_ret=0.5603`, `mfe20=0.5529`, `mfe40=0.8437`, `mae20=-0.0400`
  - `2025-07-10 3167`: `net_ret=0.5175`, `mfe20=0.4000`, `mfe40=0.6410`, `mae20=-0.0005`
  - `2025-12-08 2344`: `net_ret=0.4648`, `mfe20=0.5556`, `mfe40=0.9145`, `mae20=-0.0582`
- examples of failed events:
  - `2025-08-29 2328`: `net_ret=-0.1043`, `mfe20=0.0283`, `mae20=-0.1055`
  - `2025-10-08 2408`: `net_ret=-0.1043`, `mfe20=0.6693`, `mae20=-0.1214`
  - `2025-11-11 2408`: `net_ret=-0.1043`, `mfe20=0.1151`, `mae20=-0.1692`
  - `2025-11-11 2344`: `net_ret=-0.1043`, `mfe20=0.1627`, `mae20=-0.1732`
  - `2025-11-03 2449`: `net_ret=-0.1043`, `mfe20=0.0125`, `mae20=-0.1461`
- explanation of why the event may work:
  - extreme stock-chip accumulation plus price compression can indicate inventory absorption before public price response
  - warrant confirmation or divergence helps separate broad hype from delayed recognition
- risks and failure modes:
  - low sample count can still overstate asymmetry
  - broker behavior may drift when market structure changes
  - some families may depend on a short post-2025 regime
- concentration snapshot:
  - symbol `2330`: `8` trades
  - symbol `2408`: `7` trades
  - symbol `3231`: `7` trades
  - symbol `3706`: `6` trades
  - symbol `2449`: `6` trades
  - month `2025-04`: `1` trades
  - month `2025-05`: `6` trades
  - month `2025-06`: `10` trades
  - month `2025-07`: `11` trades
  - month `2025-08`: `12` trades
  - month `2025-09`: `10` trades
  - month `2025-10`: `20` trades
  - month `2025-11`: `8` trades
  - month `2025-12`: `5` trades

## Nearby Passing Candidates
- `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p05__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss--0p1__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p5`: `full_avg_net=0.1034`, `full_mfe40=0.3060`, `test_avg_net=0.2220`
- `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p02__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss--0p1__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p5`: `full_avg_net=0.1042`, `full_mfe40=0.3049`, `test_avg_net=0.2220`
- `accumulation_pre_breakout__h30__cd20__breakout_gap_20_cap-0p05__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss--0p1__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p5`: `full_avg_net=0.1054`, `full_mfe40=0.3110`, `test_avg_net=0.2357`
- `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p05__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss--0p1__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p7`: `full_avg_net=0.1042`, `full_mfe40=0.3077`, `test_avg_net=0.2220`
- `accumulation_pre_breakout__h30__cd20__breakout_gap_20_cap-0p05__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss--0p1__volume_ratio_5_20_cap-1p8__warrant_posnet_floor-0p5`: `full_avg_net=0.1038`, `full_mfe40=0.3047`, `test_avg_net=0.2357`

## Rejected Event Families
- `accumulation_pre_breakout__h20__cd20__breakout_gap_20_cap-0p05__stock_net_buy_days_20-5__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p7`: `test_trades<5`
- `accumulation_pre_breakout__h20__cd20__breakout_gap_20_cap-0p05__stock_net_buy_days_20-5__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p5`: `test_trades<5`
- `accumulation_pre_breakout__h20__cd15__breakout_gap_20_cap-0p05__stock_net_buy_days_20-5__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p7`: `test_trades<5`
- `accumulation_pre_breakout__h20__cd15__breakout_gap_20_cap-0p05__stock_net_buy_days_20-5__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p5`: `test_trades<5`
- `accumulation_pre_breakout__h20__cd20__breakout_gap_20_cap-0p05__stock_net_buy_days_20-5__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p8__warrant_posnet_floor-0p7`: `test_trades<5`
- `accumulation_pre_breakout__h20__cd20__breakout_gap_20_cap-0p05__stock_net_buy_days_20-5__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p8__warrant_posnet_floor-0p5`: `test_trades<5`
- `accumulation_pre_breakout__h20__cd20__breakout_gap_20_cap-0p02__stock_net_buy_days_20-5__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p8__warrant_posnet_floor-0p7`: `max_loss<=-15%|test_trades<5`
- `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p05__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p5`: `max_loss<=-15%`
- `accumulation_pre_breakout__h20__cd20__breakout_gap_20_cap-0p02__stock_net_buy_days_20-5__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p8__warrant_posnet_floor-0p5`: `max_loss<=-15%|test_trades<5`
- `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p05__stock_net_buy_days_20-3__stock_posnet_pct_cs-0p98__stop_loss-None__volume_ratio_5_20_cap-1p5__warrant_posnet_floor-0p7`: `max_loss<=-15%`