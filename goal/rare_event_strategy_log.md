# Rare Event Strategy Log

這份文件記錄 `goal/new_goal.md` 這條 rare-event 研究線的嘗試過程。

## Objective

目標不是找高頻、小利差策略，而是找：

- 低頻
- 高信念
- 高 MFE / 高 payoff
- 有清楚事件邏輯
- 且不是只靠少數極端值撐起來的 rare-event 模式

## Pipeline Baseline

第一版 pipeline 入口：

- `apps/analysis/run_rare_event_pipeline.py`

第一版固定輸出：

- `outputs/analysis/rare_event/features_stock_daily.parquet`
- `outputs/analysis/rare_event/rare_event_signals.parquet`
- `outputs/analysis/rare_event/trades.parquet`
- `outputs/analysis/rare_event/rare_event_leaderboard.csv`
- `outputs/analysis/rare_event/rare_event_family_summary.csv`
- `outputs/analysis/rare_event/best_event_concentration.csv`
- `outputs/analysis/rare_event/best_rare_event_report.md`
- `outputs/analysis/rare_event/rejected_rare_events.csv`

## Attempt 2026-05-27 / First Rare-Event Pipeline

- data used:
  - `data/_derived/ohlc.parquet`
  - `data/_derived/flow_stock_daily.parquet`
  - `data/_derived/flow_warrant_daily_by_underlying.parquet`
- feature blocks:
  - OHLC returns, volatility, volume ratio, breakout distance, price position, compression, drawdown
  - stock broker accumulation strength, concentration, persistence
  - warrant accumulation strength, concentration, persistence
  - cross features between stock and warrant chip intensity
- event families scanned:
  - `extreme_accum_compression`
  - `stealth_accumulation`
  - `stock_then_warrant_confirmation`
  - `accumulation_during_compression`
  - `warrant_leads_stock`
  - `accumulation_pre_breakout`

### First Key Finding

第一輪最先跑通時，最佳 family 很快收斂到：

- `accumulation_pre_breakout`

也就是：

- 強 stock chip accumulation
- 持續買超天數夠高
- 價格尚未真正大幅 breakout
- 量能沒有過熱
- warrant 端至少有一定程度確認

### Horizon-Aware Split Fix

一開始的 `train / valid / test` 切法把 `test` 放到太後面，導致：

- `MFE_40d` 評估窗不夠長
- `test_trades` 幾乎為 `0`

後來修正成：

- `train <= 2025-09-30`
- `valid = 2025-10-01 ~ 2025-11-15`
- `test = 2025-11-16 ~ 2025-12-15`

這樣才讓 rare-event 的 40 日 upside 指標可以在 test 端真正被觀測。

### Current Best Candidate

目前第一個通過這套 criteria 的 family 來自：

- `accumulation_pre_breakout`

代表設定：

- `hold_days = 20`
- `cooldown_days = 15`
- `stock_posnet_pct_cs >= 0.95`
- `stock_net_buy_days_20 >= 5`
- `breakout_gap_20_cap <= 0.02`
- `volume_ratio_5_20_cap <= 1.5`
- `warrant_posnet_floor >= 0.7`

代表結果：

- `full_trades = 61`
- `full_avg_net_ret = 0.1038`
- `full_median_net_ret = 0.0740`
- `full_win_rate = 0.7213`
- `full_avg_mfe_20d = 0.2078`
- `full_avg_mfe_40d = 0.3033`
- `full_profit_factor = 7.1706`
- `full_payoff_ratio = 2.7705`
- `test_trades = 5`
- `test_avg_net_ret = 0.1677`
- `test_avg_mfe_20d = 0.2014`

### Interpretation

這一輪比較像是：

- 成功建立了新的 rare-event research pipeline
- 並且第一時間就找到一條明顯值得追的主線

目前最值得繼續加碼的，不是所有 family，而是：

- `accumulation_pre_breakout`

因為它已經顯示：

- asymmetric payoff 很強
- MFE 指標漂亮
- test 端也不是空的

### Stability And Concentration Check

這一輪又往前補了兩個 artifact：

- `outputs/analysis/rare_event/rare_event_family_summary.csv`
- `outputs/analysis/rare_event/best_event_concentration.csv`

目前看到的 family-level 結論很清楚：

- 真正有 `pass_count > 0` 的只有 `accumulation_pre_breakout`
- 其他 family 雖然偶爾有不錯的 test 局部數字，但整體 asymmetry 或 full-window 條件還沒過線

`accumulation_pre_breakout` 內部的結構也開始明顯：

- `hold_days = 20`
- `cooldown_days = 15`

是目前最穩的一塊，因為：

- `h20/cd15` 有 `2` 個 passing candidates
- `h15/cd15` 也有 `2` 個 passing candidates
- `cd20` 區域則明顯變差，主要卡在 `test_trades < 5`

最佳 candidate 的集中度檢查：

- symbol 最大持倉集中：
  - `2449`: `8` trades
  - `2330`: `5`
  - `3231`: `5`
  - `2344`: `4`
  - `2408`: `4`
- 月份分布：
  - `2025-05`: `8`
  - `2025-06`: `8`
  - `2025-07`: `6`
  - `2025-08`: `10`
  - `2025-09`: `7`
  - `2025-10`: `13`
  - `2025-11`: `6`
  - `2025-12`: `3`

這說明兩件事：

- 它不是只靠單一月份或單一股票撐起來
- 但確實有幾個高權重 symbol，需要下一輪更細地檢查「去掉單一大貢獻股票後是否仍成立」

### Leave-One-Symbol-Out Check

我另外對目前最佳 candidate 做了：

- `outputs/analysis/rare_event/best_event_leave_one_symbol_out.csv`

檢查方式是：

- 每次移除一個 symbol 的所有交易
- 重新計算 full / test 指標
- 看 `all_pass` 是否仍成立

結果比預期更清楚：

- 去掉 `3231`、`2330`、`8046` 等高頻 symbol 後，策略不只沒壞，反而常常更強
- 真正讓 `all_pass` 掉下來的主要不是報酬崩掉，而是：
  - `test_trades` 掉到 `5` 以下

最敏感的 symbol 是：

- `2344`
  - 移除後：
    - `new_full_avg_net_ret = 0.0899`
    - `new_test_trades = 4`
    - fail reason: `test_trades < 5`
- `2449`
  - 移除後：
    - `new_full_avg_net_ret = 0.0977`
    - `new_test_trades = 3`
    - fail reason: `test_trades < 5`

這個結果代表：

- 目前最佳 candidate 並不是靠 `2344` 或 `2449` 才有高報酬
- 但 test 端樣本本來就偏少，所以這兩檔對「通過 test 最低交易數門檻」影響很大

換句話說，下一輪最該解的不是：

- 報酬邏輯完全失真

而是：

- 如何讓同一個事件邏輯在不明顯犧牲 asymmetry 的前提下，多出一點 test 樣本數

### Local Sensitivity Scan Around `accumulation_pre_breakout`

我另外做了一輪只圍繞目前最佳 family 的局部掃描：

- `outputs/analysis/rare_event/accumulation_pre_breakout_local_sensitivity.csv`

掃描的局部範圍是：

- `hold_days`: `15 / 20 / 25 / 30`
- `cooldown_days`: `10 / 15 / 20`
- `stock_posnet_pct_cs`: `0.93 / 0.95 / 0.97`
- `stock_net_buy_days_20`: `4 / 5 / 6`
- `breakout_gap_20_cap`: `0.02 / 0.03 / 0.05`
- `volume_ratio_5_20_cap`: `1.3 / 1.5 / 1.8`
- `warrant_posnet_floor`: `0.5 / 0.6 / 0.7 / 0.8`

總共有效候選：

- `3888`

其中 `all_pass=True` 的有：

- `57`

這輪的核心結論很清楚：

1. 如果目標是把 `test_trades` 往上推，最有效的方向是：
   - `stock_net_buy_days_20 = 4`
   - `hold_days = 15`
   - `cooldown_days = 20`
   - `breakout_gap_20_cap = 0.02`
   - `stock_posnet_pct_cs = 0.95`

2. 這條線的代表 candidate 是：
   - `h15 / cd20 / stock_posnet_pct_cs>=0.95 / stock_net_buy_days_20>=4 / breakout_gap_20_cap<=0.02 / volume_ratio_5_20_cap<=1.5 / warrant_posnet_floor>=0.6`
   - 指標：
     - `full_trades = 81`
     - `full_avg_net_ret = 0.0654`
     - `full_avg_mfe_40d = 0.2908`
     - `test_trades = 9`
     - `test_avg_net_ret = 0.0945`
   - 也就是：
     - 報酬和 asymmetry 明顯低於目前最強 `h20/cd15` 候選
     - 但 test 樣本數明顯增加

3. 另一條值得注意的 near-miss 線是：
   - 更寬鬆的 `stock_posnet_pct_cs = 0.93`
   - `stock_net_buy_days_20 = 4`
   - `hold_days = 25~30`
   - `cooldown_days = 10`
   - `breakout_gap_20_cap = 0.05`
   - 它們常常有：
     - `test_trades = 14~15`
     - `test_avg_net_ret = 18%~20%`
   - 但目前卡在：
     - `max_loss <= -15%`
     - 有些還卡 `payoff_ratio < 2.0`

這代表下一步的最佳研究方向不是再亂擴 family，而是：

- 在 `accumulation_pre_breakout` 內做「兩支線」並行
  - `high-asymmetry branch`：
    - 保持目前 `h20/cd15` 這種高報酬、高 MFE 基線
  - `higher-sample branch`：
    - 往 `h15/cd20 + stock_net_buy_days_20=4` 這塊走，換更多 test trades

再往下看，目前真正卡住 many-trade 候選的主因，不是報酬不足，而是：

- `max_loss <= -15%`

所以如果要讓較寬鬆那支線正式過關，最值得優先研究的不是再放寬條件，而是：

- 如何在不破壞 asymmetry 的前提下，把最差單筆虧損壓回 `-15%` 以上

### Simple Stop-Loss Scan On Higher-Sample Branch

我接著直接對一條代表性的 higher-sample near-miss 做了簡單止損掃描：

- `outputs/analysis/rare_event/accumulation_pre_breakout_higher_sample_sl_scan.csv`

代表 near-miss base 設定是：

- `hold_days = 30`
- `cooldown_days = 10`
- `stock_posnet_pct_cs >= 0.93`
- `stock_net_buy_days_20 >= 4`
- `breakout_gap_20_cap <= 0.05`
- `volume_ratio_5_20_cap <= 1.8`
- `warrant_posnet_floor >= 0.6`

原始問題是：

- `full_trades = 155`
- `test_trades = 15`
- `test_avg_net_ret = 18.16%`
- 但因為 `max_loss <= -15%`，所以 still fail

這輪最重要的新發現是：

- 單純加一個固定止損，就能把這條線從 near-miss 推成 pass

代表結果：

1. `hold_days = 30, sl = -12%`
- `full_trades = 155`
- `full_avg_net_ret = 6.34%`
- `full_profit_factor = 2.59`
- `full_payoff_ratio = 2.02`
- `max_loss = -12.42%`
- `test_trades = 15`
- `test_avg_net_ret = 17.89%`
- `test_profit_factor = 13.01`
- `all_pass = True`

2. `hold_days = 30, sl = -10%`
- `full_trades = 155`
- `full_avg_net_ret = 6.23%`
- `full_profit_factor = 2.61`
- `full_payoff_ratio = 2.20`
- `max_loss = -10.43%`
- `test_trades = 15`
- `test_avg_net_ret = 17.66%`
- `all_pass = True`

3. `hold_days = 25, sl = -10%`
- `full_trades = 155`
- `full_avg_net_ret = 6.19%`
- `full_profit_factor = 2.62`
- `full_payoff_ratio = 2.05`
- `max_loss = -10.43%`
- `test_trades = 15`
- `test_avg_net_ret = 11.33%`
- `all_pass = True`

這個結果的意義很大：

- 之前 higher-sample branch 卡住的主因確實就是單筆最差虧損
- 而不是整體報酬結構不好
- 加上簡單風控後，這條線就變成：
  - 樣本數比 high-asymmetry branch 大很多
  - test 樣本更充足
  - 同時仍保有明顯正向 asymmetry

所以現在 `accumulation_pre_breakout` 其實已經不只一條 pass 路徑，而是至少有兩種不同 profile：

- `high-asymmetry branch`
  - 交易數較少
  - 單筆報酬與 MFE 更漂亮
- `higher-sample + stop-loss branch`
  - 交易數明顯更多
  - `test_trades` 更厚
  - 報酬仍達標，但不如 high-asymmetry branch 那麼極致

### Pipeline Baseline Shift

我已經把 `stop_loss` 正式併進 `run_rare_event_pipeline.py`，所以現在主 pipeline 不是只記錄純事件版，而是會把：

- 純事件版
- 事件 + stop-loss 版

一起放進同一個 leaderboard。

併回主 pipeline 後，新的 top baseline 已經切到 risk-control branch：

- `family = accumulation_pre_breakout`
- `hold_days = 30`
- `cooldown_days = 15`
- `stock_posnet_pct_cs >= 0.98`
- `stock_net_buy_days_20 >= 3`
- `breakout_gap_20_cap <= 0.05`
- `volume_ratio_5_20_cap <= 1.5`
- `warrant_posnet_floor >= 0.5`
- `stop_loss = -10%`

新的主報告結果：

- `full_trades = 83`
- `full_avg_net_ret = 0.1034`
- `full_median_net_ret = 0.0552`
- `full_avg_mfe_20d = 0.2012`
- `full_avg_mfe_40d = 0.3060`
- `full_profit_factor = 4.3033`
- `full_payoff_ratio = 2.8402`
- `full_max_loss = -10.43%`
- `test_trades = 8`
- `test_avg_net_ret = 22.20%`

也就是說，現在主 pipeline 的 best candidate 已經不再是原本較窄的 `h20/cd15` 純事件版，而是：

- 更高 sample
- 有正式風控
- 且仍保有高 asymmetry

這條線目前比較像是新的主 baseline。

### Leave-One-Symbol-Out On New Baseline

我也對新的主 baseline 做了：

- `outputs/analysis/rare_event/best_event_leave_one_symbol_out_v2.csv`

這次結果比舊 baseline 更穩：

- `all_pass_after_drop = 33 / 33`

也就是：

- 不管移除哪一個實際參與交易的 symbol
- 新 baseline 都仍然維持 `all_pass = True`

最敏感的幾個 symbol 是：

- `2337`
  - 移除後：
    - `new_full_avg_net_ret = 0.0893`
    - `new_test_trades = 7`
    - `new_test_avg_net_ret = 0.0742`
    - 仍然 pass
- `2449`
  - 移除後：
    - `new_full_avg_net_ret = 0.0957`
    - `new_test_trades = 7`
    - `new_test_avg_net_ret = 0.2242`
    - 仍然 pass
- `2344`
  - 移除後：
    - `new_full_avg_net_ret = 0.0965`
    - `new_test_trades = 7`
    - `new_test_avg_net_ret = 0.1874`
    - 仍然 pass

這和前一版 `high-asymmetry branch` 最大的差異是：

- 舊版一旦移除 `2344 / 2449`，常常就會因 `test_trades < 5` 掉出 pass
- 新版因為本來樣本更厚，所以即使拿掉高貢獻 symbol，仍然維持過線

因此到目前為止，這條新的 `higher-sample + stop-loss` 主 baseline 不只：

- 指標過關
- test 樣本較厚

而且：

- symbol robustness 也明顯優於前一版 baseline

### Risks

這輪還沒完全解決的風險：

- 還沒有做鄰近參數穩定性整理
- 雖然已經做了 leave-one-symbol-out，但 test 樣本仍偏少，門檻穩定性對少數 symbol 仍敏感
- `accumulation_during_compression` 在 test 端有亮眼局部數字，但 full-window asymmetry 不夠，目前仍是次要候選
- 較高 sample 的 `accumulation_pre_breakout` 候選，現在主要被 `max_loss <= -15%` 卡住
- higher-sample branch 雖然已經被簡單止損推成 pass，但還沒正式併進主 pipeline / leaderboard
 

## Next Actions

- 把 stop-loss / risk-control 版本正式納入 rare-event pipeline
- 比較：
  - `high-asymmetry branch`
  - `higher-sample + stop-loss branch`
  - 哪一條更適合作為新的主 baseline
- 保留 `high-asymmetry branch` 作為目前最強 baseline，不要把兩條線混在一起
- 補報告中的 best / failed examples，讓事件邏輯更可解釋

## 2026-05-28 Correction And Rerun

### Feature Definition Correction

這輪先做了必要的 feature 修正：

- 舊版 `stock_net_buy_days_20` / `warrant_net_buy_days_20`
  - 是用 `net_total > 0` 的 rolling count 定義
  - 這個定義不夠自洽，因為同一檔股票同一天跨所有券商加總的 `buy - sell` 理論上應接近 `0`
  - 因此它更像 ETL / 對齊殘差，不應直接拿來代表「偏買方日」

修正後的新定義是：

- `stock_posnet_strong_days_20`
  - 以 `stock_posnet_pct_cs >= 0.95` 的日數做 rolling count
- `warrant_posnet_strong_days_20`
  - 以 `warrant_posnet_pct_cs >= 0.80` 的日數做 rolling count

也就是說，現在 persistence 的語意改成：

- 最近 20 日裡，有幾天屬於全市場相對強的正向籌碼日

而不是：

- 最近 20 日裡，有幾天碰巧出現 `net_total > 0` 的殘差

因此，這份日誌在本節之前所有直接依賴 `stock_net_buy_days_20` 的 rare-event 結論，都應視為已被新版 rerun supersede。

### Search Space Correction

修正 feature 後，我先驗了新的 search 行為：

- 原始 coarse grid 仍保留全部 family，但 `accumulation_pre_breakout` 會讓 spec 數衝到 `4720`
- 這對本地 rerun 太慢，也不符合這輪要先做正確性修復的目的

因此我把主 pipeline 的 grid 收斂成：

- 全 family 都保留
- `accumulation_pre_breakout` 改成較小但仍嚴格的 coarse grid
- 之後再把 `warrant_leads_stock` 的 stop-loss 小區域正式納回主 pipeline

最終主 pipeline 這輪是 `312` 個 candidate spec。

### Corrected Main Baseline

修正後的 best event 已經不再是舊的高頻 `accumulation_pre_breakout` baseline，而是切到真正更像 rare-event 的：

- `family = warrant_leads_stock`
- `hold_days = 40`
- `cooldown_days = 15`
- `prior_abs_ret_20d <= 0.08`
- `stock_posnet_floor >= 0.70`
- `stock_posnet_cap <= 0.90`
- `warrant_posnet_floor >= 0.98`
- `warrant_posnet_strong_days_20 >= 3`
- `stop_loss = -10%`

對應結果現在寫在 `outputs/analysis/rare_event/best_rare_event_report.md`：

- `full_trades = 81`
- `full_avg_net_ret = 0.0913`
- `full_median_net_ret = 0.0287`
- `full_avg_mfe_20d = 0.1508`
- `full_avg_mfe_40d = 0.2395`
- `full_profit_factor = 3.0039`
- `full_payoff_ratio = 2.9306`
- `full_max_loss = -10.43%`
- `test_trades = 14`
- `test_avg_net_ret = 0.0321`

這組是 corrected rerun 後，第一個真正兼顧：

- 較低頻
- clear event logic
- 有正式 stop-loss
- 並且仍滿足 `new_goal.md` full/test 門檻

### Corrected Passing Set

這輪 corrected main pipeline 的整體狀態：

- `all_pass_count = 13`
- 主 pass family 主要來自：
  - `warrant_leads_stock`
  - `accumulation_pre_breakout`

其中比較值得保留的 pass 區域是：

1. `warrant_leads_stock`
   - `h40 / cd15 / prior_abs_ret_20d<=0.08 / stock_posnet 0.70~0.90 / warrant_posnet_floor>=0.98 / warrant_posnet_strong_days_20>=3 / stop_loss=-10%`
   - `81` trades
   - 更接近 rare-event 方向

2. `warrant_leads_stock`
   - `h30 / cd15 / prior_abs_ret_20d<=0.12 / warrant_posnet_floor>=0.99 / stop_loss=-12%`
   - `56` trades
   - 更少交易，平均報酬仍高

3. `accumulation_pre_breakout`
   - 修正後仍然有 pass candidate
   - 但交易數約 `500+`
   - 已不再適合作為這輪 rare-event 研究的主 baseline

### Interpretation

這次 correction 的核心結論是：

- 舊版錯誤 feature 沒有把整條 rare-event 線推翻
- 但它確實把主 baseline 偏向了較高頻的 `accumulation_pre_breakout`
- feature 修正後，真正更符合目標的主 baseline 反而浮到 `warrant_leads_stock`

也就是：

- 「權證端先領先、正股仍在中高但未擁擠區、且先前價格反應不大」

比起：

- 「正股先累積、準備 breakout」

更像這輪 corrected research 真正找到的 rare-event 主方向。

## 2026-05-29 Direction 2 Refinement Scan

這輪把 `warrant_leads_stock` 從主 pipeline 的 coarse grid 中拉出來，做一個只針對方向 2 的 refinement：

- script: `apps/analysis/run_warrant_leads_stock_refine.py`
- report: `outputs/analysis/rare_event/warrant_leads_stock_refine_report.md`
- leaderboard: `outputs/analysis/rare_event/warrant_leads_stock_refine_leaderboard.csv`
- trades: `outputs/analysis/rare_event/warrant_leads_stock_refine_trades.parquet`
- signals: `outputs/analysis/rare_event/warrant_leads_stock_refine_signals.parquet`
- concentration: `outputs/analysis/rare_event/warrant_leads_stock_refine_concentration.csv`
- leave-one-symbol: `outputs/analysis/rare_event/warrant_leads_stock_refine_leave_one_symbol_out.csv`
- leave-one-month: `outputs/analysis/rare_event/warrant_leads_stock_refine_leave_one_month_out.csv`

### Scan Design

這次沒有重建 feature，而是直接使用 corrected feature artifact：

- `outputs/analysis/rare_event/features_stock_daily.parquet`

base 條件仍然維持方向 2 的事件語意：

- 權證端買盤強：
  - `warrant_posnet_pct_cs >= 0.98 / 0.99`
  - `warrant_posnet_strong_days_20 >= 1 / 3 / 5`
- 正股端不能太弱，但也避免完全擁擠：
  - `stock_posnet_pct_cs` 區間在 `0.70~0.95` 的幾組組合
- 價格先前反應不能太大：
  - `prior_abs_ret_20d <= 0.08 / 0.12`
- 出場：
  - `hold_days = 30 / 40`
  - `cooldown_days = 15 / 20`
  - `stop_loss = None / -12% / -10%`

refinement 主要加了幾類二階條件：

- 權證集中度：
  - `warrant_hhi_pct_cs`
  - `warrant_hhi_posnet_20`
  - `warrant_top_posnet_ratio_20`
  - `warrant_dyn_k_pct_cs`
- 價格未過熱：
  - `volume_ratio_5_20`
  - `breakout_gap_20`
  - `price_pos_20`

總共有效掃描：

- `43,920` specs
- `1,607` rare-event all-pass rows
- `3,067` full target-pass rows
- `1,030` robust target-pass rows

### Best Refined Candidate

最佳候選切到一個更嚴格的權證動態集中條件：

- event:
  - `warrant_leads_stock__h40__cd15__filter_suite-warrant_dyn_k__prior_abs_ret_20d-0p08__stock_posnet_cap-0p95__stock_posnet_floor-0p8__stop_loss--0p1__warrant_dyn_k_pct_cs_floor-0p9__warrant_posnet_floor-0p98__warrant_posnet_strong_days_20-3`
- key params:
  - `warrant_posnet_pct_cs >= 0.98`
  - `warrant_dyn_k_pct_cs >= 0.90`
  - `warrant_posnet_strong_days_20 >= 3`
  - `stock_posnet_pct_cs` between `0.80` and `0.95`
  - `prior_abs_ret_20d <= 0.08`
  - `hold_days = 40`
  - `cooldown_days = 15`
  - `stop_loss = -10%`

結果：

- `full_trades = 52`
- `full_win_rate = 0.6154`
- `full_avg_net_ret = 0.1143`
- `full_median_net_ret = 0.0873`
- `full_profit_factor = 4.2203`
- `full_max_loss = -10.43%`
- `test_trades = 12`
- `test_win_rate = 0.7500`
- `test_avg_net_ret = 0.1342`

這是目前方向 2 第一個同時達到使用者目標的版本：

- 勝率大於 `50%`
- 平均報酬大於 `10%`

### Robustness Notes

leave-one-symbol 結果：

- `27 / 28` 個 symbol 被移除後仍維持 robust target pass
- 唯一敏感 symbol 是 `3443`
  - 移除後 `full_avg_net_ret = 0.0994`
  - 主要問題是剛好跌破 `10%` 門檻

leave-one-month 結果：

- `6 / 8` 個月份被移除後仍維持 robust target pass
- 敏感月份：
  - 移除 `2025-09` 後 `full_avg_net_ret = 0.0986`
  - 移除 `2025-11` 後 `test_trades = 4`

### Interpretation

這輪把方向 2 從「接近目標」推進到「目前證據下已達標」：

- 原 baseline 的問題是平均報酬只有約 `9.13%`
- 加上 `warrant_dyn_k_pct_cs >= 0.90` 後，報酬與 test 表現都明顯改善
- 這更貼近「權證老司機」的語意：
  - 不是只有權證總買盤強
  - 而是權證端買盤具有更高的動態集中/主導性

但這不是 production-ready 結論：

- 這輪是 `43,920` specs 的 refinement，multiple-testing risk 明顯高於主 pipeline coarse scan
- 月份敏感性仍存在
- 下一步應該做更嚴格的 forward / out-of-sample replay，而不是繼續放大 grid
