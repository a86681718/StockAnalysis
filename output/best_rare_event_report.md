# Rare Event Strategy Report

## Best Event: warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p1__warrant_posnet_floor-0p98__warrant_posnet_strong_days_20-3

- profit factor: `3.0039`
- payoff ratio: `2.9306`
- event name: `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p1__warrant_posnet_floor-0p98__warrant_posnet_strong_days_20-3`
- split definition: `train <= 2025-09-30`, `valid = 2025-10-01 ~ 2025-11-15`, `test = 2025-11-16 ~ 2025-12-15`
- entry rule: next open after signal; strict event filter defined by `{"cooldown_days": 15, "family": "warrant_leads_stock", "hold_days": 40, "prior_abs_ret_20d": 0.08, "stock_posnet_cap": 0.9, "stock_posnet_floor": 0.7, "stop_loss": -0.1, "warrant_posnet_floor": 0.98, "warrant_posnet_strong_days_20": 3}`
- exit rule: `stop_loss=-0.1` if hit, otherwise close after `40` trading days
- number of trades: `81`
- average net return: `0.0913`
- median net return: `0.0287`
- win rate: `0.5062`
- average MFE and MAE: `mfe20=0.1508`, `mfe40=0.2395`, `mae20=-0.0813`
- MFE >= 10% hit rate: `0.5926`
- MFE >= 20% hit rate: `0.4321`
- train/validation/test performance: `train=0.1145`, `valid=0.0527`, `test=0.0321`
- parameter sensitivity: neighboring-family robustness not yet quantified; use leaderboard rows around `warrant_leads_stock` as first-pass sensitivity check
- examples of the best historical events:
  - `2025-08-06 6515`: `net_ret=0.8930`, `mfe20=0.3695`, `mfe40=0.9947`, `mae20=-0.0771`
  - `2025-09-26 3443`: `net_ret=0.7710`, `mfe20=0.3458`, `mfe40=0.7944`, `mae20=-0.0044`
  - `2025-12-18 2383`: `net_ret=0.5820`, `mfe20=0.1200`, `mfe40=0.6279`, `mae20=-0.0038`
  - `2025-09-30 6805`: `net_ret=0.5696`, `mfe20=0.4918`, `mfe40=0.6957`, `mae20=-0.0154`
  - `2025-08-06 6223`: `net_ret=0.5171`, `mfe20=0.3060`, `mfe40=0.5637`, `mae20=-0.0227`
- examples of failed events:
  - `2025-10-17 3563`: `net_ret=-0.1043`, `mfe20=0.0506`, `mae20=-0.2276`
  - `2025-12-17 6669`: `net_ret=-0.1043`, `mfe20=0.1723`, `mae20=-0.1032`
  - `2025-06-05 3293`: `net_ret=-0.1043`, `mfe20=0.0538`, `mae20=-0.0248`
  - `2025-12-08 3131`: `net_ret=-0.1043`, `mfe20=0.2610`, `mae20=-0.1064`
  - `2025-12-03 3529`: `net_ret=-0.1043`, `mfe20=0.0556`, `mae20=-0.2150`
- explanation of why the event may work:
  - warrant-side activity can move earlier than the underlying when informed or anticipatory flow reaches leverage instruments first
  - keeping stock-chip strength in a mid-high but not fully crowded zone helps avoid already overreacted names
- risks and failure modes:
  - low sample count can still overstate asymmetry
  - broker behavior may drift when market structure changes
  - some families may depend on a short post-2025 regime
- concentration snapshot:
  - symbol `2454`: `9` trades
  - symbol `3661`: `8` trades
  - symbol `6669`: `7` trades
  - symbol `3131`: `7` trades
  - symbol `3443`: `5` trades
  - month `2025-05`: `3` trades
  - month `2025-06`: `9` trades
  - month `2025-07`: `10` trades
  - month `2025-08`: `13` trades
  - month `2025-09`: `14` trades
  - month `2025-10`: `12` trades
  - month `2025-11`: `9` trades
  - month `2025-12`: `11` trades

## Nearby Passing Candidates
- `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p1__warrant_posnet_floor-0p98__warrant_posnet_strong_days_20-3`: `full_avg_net=0.0913`, `full_mfe40=0.2395`, `test_avg_net=0.0321`
- `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p1__warrant_posnet_floor-0p98__warrant_posnet_strong_days_20-5`: `full_avg_net=0.0913`, `full_mfe40=0.2395`, `test_avg_net=0.0321`
- `warrant_leads_stock__h30__cd15__prior_abs_ret_20d-0p12__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p12__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-1`: `full_avg_net=0.0830`, `full_mfe40=0.2556`, `test_avg_net=0.0658`
- `warrant_leads_stock__h30__cd15__prior_abs_ret_20d-0p12__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p12__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-3`: `full_avg_net=0.0830`, `full_mfe40=0.2556`, `test_avg_net=0.0658`
- `warrant_leads_stock__h30__cd15__prior_abs_ret_20d-0p12__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p12__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-5`: `full_avg_net=0.0830`, `full_mfe40=0.2556`, `test_avg_net=0.0658`

## Rejected Event Families
- `stock_then_warrant_confirmation__h30__cd15__prior_abs_ret_20d-0p08__stock_posnet_pct_cs-0p98__stock_posnet_strong_days_20-5__stop_loss-None__warrant_posnet_floor-0p7__warrant_posnet_strong_days_20-3`: `max_loss<=-15%`
- `stock_then_warrant_confirmation__h30__cd15__prior_abs_ret_20d-0p08__stock_posnet_pct_cs-0p98__stock_posnet_strong_days_20-5__stop_loss-None__warrant_posnet_floor-0p7__warrant_posnet_strong_days_20-1`: `max_loss<=-15%`
- `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p02__stock_posnet_pct_cs-0p98__stock_posnet_strong_days_20-3__stop_loss-None__volume_ratio_5_20_cap-1p8__warrant_posnet_floor-0p7`: `max_loss<=-15%|test_max_loss<=-15%`
- `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p1__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-1`: `test_avg_net_ret<=3%|test_profit_factor<1.3|test_payoff_ratio<1.5`
- `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p1__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-3`: `test_avg_net_ret<=3%|test_profit_factor<1.3|test_payoff_ratio<1.5`
- `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p1__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-5`: `test_avg_net_ret<=3%|test_profit_factor<1.3|test_payoff_ratio<1.5`
- `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p12__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-1`: `test_avg_net_ret<=3%|test_profit_factor<1.3|test_payoff_ratio<1.5`
- `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p12__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-3`: `test_avg_net_ret<=3%|test_profit_factor<1.3|test_payoff_ratio<1.5`
- `warrant_leads_stock__h40__cd15__prior_abs_ret_20d-0p08__stock_posnet_cap-0p9__stock_posnet_floor-0p7__stop_loss--0p12__warrant_posnet_floor-0p99__warrant_posnet_strong_days_20-5`: `test_avg_net_ret<=3%|test_profit_factor<1.3|test_payoff_ratio<1.5`
- `accumulation_pre_breakout__h30__cd15__breakout_gap_20_cap-0p02__stock_posnet_pct_cs-0p98__stock_posnet_strong_days_20-5__stop_loss-None__volume_ratio_5_20_cap-1p8__warrant_posnet_floor-0p7`: `max_loss<=-15%|test_max_loss<=-15%`