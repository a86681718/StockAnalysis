# BsReport Strategy Discovery Log

這份文件用來累積記錄：

- 基於 `Analysis_BsReport_v1~v4` 核心精神延伸出的模型與策略嘗試
- 每次嘗試的資料來源、設定、驗證方式
- 關鍵結果
- 當下判斷：可保留 / 需補驗證 / 可淘汰

目標不是保存所有雜訊，而是保留真正對後續研究有價值的搜尋軌跡。

## Success Criteria

目前想找的是一個同時滿足下列條件的候選策略：

- 勝率 `> 70%`
- 平均報酬 `> 10%`
- 不是只來自訓練內結果，而是能經得起較可信的長期驗證

長期驗證在這份文件裡，優先級如下：

1. `walk-forward / OOF`
2. 明確 train / valid 分段，且驗證區間與訓練區間分離
3. 單純全樣本回測或條件過濾後的高表現，只能算 weak evidence

## Current Snapshot

截至目前從既有輸出中整理出的重點：

### Candidate A

- artifact:
  - `data/_derived/ml_runs/topn_summary_feat2_top10_days5.csv`
  - `data/_derived/ml_runs/topn_trades_feat2_top10_days5.csv`
- result:
  - win rate: `0.8308`
  - mean return: `0.1134`
  - n_exec: `532`
  - skip_rate: `0.7087`
- interpretation:
  - 這是目前最接近目標，甚至表面上已達標的結果。
  - 但從交易檔內容看，這條線是建立在 `days_above` 過濾後的子樣本。
  - 目前還未確認這是不是 OOF / walk-forward 產物，因此只能列為高優先級候選，不能直接當作最終答案。
- status:
  - `Needs provenance check`

### Candidate B

- artifact:
  - `data/_derived/ml_runs/strategy_search_oof.csv`
  - `data/_derived/ml_runs/strategy_search_oof_best.csv`
- best high-win examples:
  - `topk=3`, `hold_days=15`, `tp=0.10`, `sl=-0.10`
  - valid win: `0.9048`
  - valid mean net: `0.0576`
  - valid trades: `21`
- best high-mean examples:
  - `topk=3`, `hold_days=13`, `sl=-0.05`
  - valid win: `0.6667`
  - valid mean net: `0.0951`
  - valid trades: `21`
- interpretation:
  - 這條線代表已有一批交易規則搜索確實把 win rate 推到很高。
  - 但目前 high-win 與 high-mean 還沒同時重疊在同一組參數上。
  - 距離目標已經很近，但還差最後一段局部搜索或更好的 prediction source。
- status:
  - `Primary search line`

### Candidate C

- artifact:
  - `data/_derived/ml_runs/strategy_AB_backtest_days_above_oof.csv`
- result:
  - `strategy_B`
  - label hit rate: `0.8535`
  - mean_ret_10d: `0.0423`
  - win_rate_ret_10d: `0.7962`
  - n_picks: `157`
- interpretation:
  - 這條線在 OOF 檢驗下很穩，勝率漂亮。
  - 但平均報酬太低，離 `10%` 很遠。
  - 值得保留作為穩定基準，不像是最終目標策略。
- status:
  - `Strong baseline, insufficient return`

### Candidate D

- artifact:
  - `data/_derived/ml_runs/rebound_chip_factor_combo_best_valid.csv`
  - `data/_derived/ml_runs/rebound_chip_factor_combo_best_stable.csv`
- best valid example:
  - valid ret10 mean: `0.0644`
  - valid ret10 win: `0.7358`
  - valid net10 mean: `0.0570`
  - valid net10 win: `0.6792`
- interpretation:
  - 這條線代表 `rebound + chip factor filter` 是有訊號的。
  - 但目前無論毛報酬或淨報酬都還不夠高。
  - 可以視為次要分支，不是目前最該優先加碼的方向。
- status:
  - `Secondary branch`

### Candidate E

- artifact:
  - `data/_derived/ml_runs/_tmp_breakout10_wf_local_refine.csv`
  - `data/_derived/ml_runs/_tmp_breakout10_wf_local_refine_best.csv`
- best distinct examples:
  - `topk=4`, `gap_th=0.010`, `hold_days=15`, `max_positions=5`
  - valid trades: `20`
  - valid win: `0.8000`
  - valid mean net: `0.1587`
  - valid total net: `0.7806`
  - `topk=5`, `gap_th=0.005`, `hold_days=15`, `max_positions=5`
  - valid trades: `20`
  - valid win: `0.7500`
  - valid mean net: `0.1592`
  - valid total net: `0.7693`
  - `topk=4`, `gap_th=0.005`, `hold_days=15`, `max_positions=6`
  - valid trades: `22`
  - valid win: `0.8182`
  - valid mean net: `0.1415`
  - valid total net: `0.5974`
- interpretation:
  - 這是目前第一批在較可信 `breakout10_predictions_wf.csv` 上，同時滿足：
    - `valid_win > 0.70`
    - `valid_mean_net > 0.10`
    - `valid_trades >= 20`
  - 而且不是單一尖點，已經形成一小塊相鄰參數區域。
  - 主要共同結構是：
    - `topk 4~6`
    - `gap_th 0.5%~1.0%`
    - `hold_days 15`
    - `max_positions 5~6`
    - 不加 `tp/sl`
- status:
  - `Primary validated candidate`

## Working Hypothesis

從現有結果推測，目前最可能達成目標的路徑有兩種：

1. `feat2 + days_above` 這條高報酬高勝率線，若能證明不是 in-sample 幻象，可能直接成為候選。
2. `breakout10_predictions_wf` 這條線若把搜尋集中在 `topk 4~6 / gap 0.5%~1.0% / hold 15 / max_positions 5~6`，已經能進入目標區。
3. `strategy_search_oof` 這條舊線已有很高勝率與接近 10% 的平均淨報酬，仍然值得當成次要搜索線。

## Next Actions

- 查清 `topn_summary_feat2_top10_days5.csv` 的來源與驗證方式。
- 對 `strategy_search_oof` 做局部搜索，不再跑大範圍全格點。
- 優先測試與 `days_above` / `feat2` / `weighted return` 相關的 prediction source。
- 對 `Candidate E` 做更長驗證窗與更嚴格穩定性檢查，確認不是區間偶然。
- 以 `apps/analysis/run_strategy_replay.py` 固定輸出 `Candidate E` 的 trades / summary，避免後續只能靠 grid 結果回推。
- 每次新增嘗試都追加到本文件底部。

## Attempt Log

### Attempt 2026-05-26 / Existing Output Triage

- scope:
  - 只盤點現有 `_derived/ml_runs` 與 `_derived` 內結果，不新增模型訓練。
- findings:
  - `topn_summary_feat2_top10_days5.csv` 是目前唯一已知同時超過 `70%` 勝率與 `10%` 平均報酬的結果。
  - 但該結果目前缺少明確 provenance，不能直接宣稱為長期可驗證策略。
  - `strategy_search_oof.csv` 是最值得局部再搜索的線，因為已存在：
    - 高勝率但較低報酬
    - 高報酬但略低勝率
  - 代表這條搜索空間很可能已逼近目標邊界。
- decision:
  - 下一輪優先：
    - 補驗 `feat2_top10_days5`
    - 局部重搜 `strategy_search_oof`

### Attempt 2026-05-26 / Feat2 Provenance Check

- scope:
  - 嘗試重現 `topn_summary_feat2_top10_days5.csv`，確認這條已達標結果是否可重做、可解釋。
- commands:
  - `run_ml_topn_backtest.py --preds data/_derived/ml_runs/wret10_feat2_tune20_predictions_all.csv --topn 10 --horizon 10 --entry next_open --min-days-above 5`
  - `run_ml_topn_backtest.py --preds data/_derived/ml_runs/wret10_feat2_tune20_predictions_all.csv --topn 10 --horizon 5 --entry next_open --min-days-above 5`
- findings:
  - `horizon=10` 時，重跑結果約為：
    - win rate: `0.7552`
    - mean return: `0.1091`
    - n_exec: `1107`
  - `horizon=5` 時，重跑結果約為：
    - win rate: `0.8276`
    - mean return: `0.0952`
    - n_exec: `673`
  - 都無法精確重現既有 artifact 的：
    - win rate: `0.8308`
    - mean return: `0.1134`
    - n_exec: `532`
- interpretation:
  - `feat2_top10_days5` 這條線仍然很強，但目前 provenance 不明。
  - 在來源與生成條件沒釐清前，不能把它視為可長期驗證的正式候選。
- status:
  - `Strong but unverified`

### Attempt 2026-05-26 / Strategy Search On wret10_feat2 Predictions

- scope:
  - 對 `wret10_feat2_tune20_predictions.csv` 做小範圍 `strategy_search_oof`，確認 weighted-return prediction 線是否能在獨立驗證區間內同時達標。
- findings:
  - 沒有任何 `valid_trades >= 20` 的組合，同時滿足：
    - `valid_win > 0.70`
    - `valid_mean_net > 0.10`
  - 最接近的組合之一：
    - `topk=3`
    - `hold_days=13`
    - `gap_th=0.00`
    - `max_positions=5`
    - `valid_trades=13`
    - `valid_win=0.7692`
    - `valid_mean_net=0.0858`
  - 另一個高報酬組合：
    - `topk=3`
    - `hold_days=15`
    - `max_positions=3`
    - `valid_trades=8`
    - `valid_win=0.7500`
    - `valid_mean_net=0.1160`
- interpretation:
  - 這條線確實有 alpha，但要嘛交易數太少，要嘛平均淨報酬還沒過 `10%`。
- status:
  - `Near target, insufficient evidence`

### Attempt 2026-05-26 / Strategy Search On breakout10 wf Predictions

- scope:
  - 對 `breakout10_predictions_wf.csv` 做較可信的 walk-forward 驗證搜索，並與 `breakout10_predictions_all_wf.csv` 的較弱證據結果區隔。
- findings:
  - 在 `breakout10_predictions_wf.csv` 上：
    - 沒有任何 `valid_trades >= 20` 的組合同時滿足 `valid_win > 0.70` 與 `valid_mean_net > 0.10`
    - 小樣本最亮眼的組合曾出現：
      - `topk=3`
      - `hold_days=15`
      - `gap_th=0.05`
      - `valid_trades=8`
      - `valid_win=1.0000`
      - `valid_mean_net=0.1682`
    - 但交易數太少，不能採信為長期候選。
  - 在更嚴格聚焦的第二輪 `wf` 搜索上，最接近門檻的是：
    - `topk=5`
    - `hold_days=14`
    - `gap_th=0.01`
    - `max_positions=5`
    - `valid_trades=20`
    - `valid_win=0.7000`
    - `valid_mean_net=0.1329`
  - 這組平均報酬已過門檻，但勝率只到 `70.00%`，沒有超過 `70%`。
  - 相對地，在 `breakout10_predictions_all_wf.csv` 上曾找到多組漂亮結果，例如：
    - `topk=5`
    - `hold_days=14`
    - `gap_th=0.05`
    - `tp=0.15`
    - `sl=-0.10`
    - `max_positions=5`
    - `valid_trades=30`
    - `valid_win=0.9000`
    - `valid_mean_net=0.1149`
- interpretation:
  - `breakout10` 這條線在較弱證據的 `all_wf` 預測上，已經非常接近可交易答案。
  - 但在較可信的 `wf` 預測上，還沒有跨過最後門檻。
  - 目前這條線最合理的結論是：
    - `all_wf` 結果可作為高優先級假說
    - `wf` 結果才是是否真正成立的主要裁判
- status:
  - `Promising but not yet validated`

### Attempt 2026-05-26 / breakout10 wf Local Refine

- scope:
  - 針對先前最接近門檻的 `wf` 區域做局部搜索，只保留：
    - `topk 4~6`
    - `gap_th 0.5%~1.5%`
    - `hold_days 13~15`
    - `max_positions 4~6`
    - `tp/sl` 只作輕量比較
- findings:
  - 在 `breakout10_predictions_wf.csv` 上，首次找到多組 `valid_trades >= 20` 且同時滿足：
    - `valid_win > 0.70`
    - `valid_mean_net > 0.10`
  - 代表性組合：
    - `topk=4`, `gap_th=0.010`, `hold_days=15`, `max_positions=5`
    - `valid_trades=20`
    - `valid_win=0.8000`
    - `valid_mean_net=0.1587`
    - `valid_total_net=0.7806`
    - `topk=5`, `gap_th=0.005`, `hold_days=15`, `max_positions=5`
    - `valid_trades=20`
    - `valid_win=0.7500`
    - `valid_mean_net=0.1592`
    - `valid_total_net=0.7693`
    - `topk=4`, `gap_th=0.005`, `hold_days=15`, `max_positions=6`
    - `valid_trades=22`
    - `valid_win=0.8182`
    - `valid_mean_net=0.1415`
    - `valid_total_net=0.5974`
  - 這批成功組合的共同特徵是：
    - `hold_days=15`
    - `gap filter` 需存在，但不宜太大
    - 不加 `tp/sl` 反而更強
    - `max_positions` 放到 `5~6` 比 `4` 更穩
- interpretation:
  - 這是目前第一條在較可信 `wf` 預測上真正跨過目標門檻的主線。
  - 但訓練端交易數仍偏少，且驗證窗仍然有限。
  - 所以目前可以稱為：
    - `strong validated candidate`
  - 還不能直接升格成：
    - `long-term verified strategy`
- decision:
  - 下一輪優先不是再盲目擴 grid，而是：
    - 拉長驗證期間
    - 檢查不同切窗是否仍成立
    - 比較 `topk=4/5/6` 與 `max_positions=5/6` 的穩定性

### Attempt 2026-05-26 / Candidate E Stability Check On wf Windows

- scope:
  - 固定 `Candidate E` 周邊參數區，只在較可信的 `breakout10_predictions_wf.csv` 上切不同驗證窗，不重新擴張搜索空間。
  - 驗證窗：
    - `2025-11-01 ~ 2025-11-30`
    - `2025-12-01 ~ 2025-12-31`
    - `2026-01-01 ~ 2026-02-03`
- findings:
  - `2025-12` 是最強區段，多組組合同時達標，例如：
    - `topk=4`, `gap_th=0.010`, `hold_days=15`, `max_positions=5`
    - `valid_trades=10`
    - `valid_win=0.8000`
    - `valid_mean_net=0.1664`
  - `2025-11` 與 `2026-01 ~ 2026-02-03` 仍有正報酬，但穩定度下降：
    - 同一組 `topk=4`, `gap_th=0.010`, `hold_days=15`, `max_positions=5`
    - `2025-11`: `valid_win=0.7000`, `valid_mean_net=0.0799`, `valid_trades=10`
    - `2026-01 ~ 2026-02-03`: `valid_win=0.6667`, `valid_mean_net=0.1281`, `valid_trades=6`
  - 另一組 `topk=4`, `gap_th=0.005`, `hold_days=15`, `max_positions=6`：
    - `2025-11`: `0.6667 / 0.0770 / 9 trades`
    - `2025-12`: `0.7000 / 0.1451 / 10 trades`
    - `2026-01 ~ 2026-02-03`: `0.8571 / 0.1687 / 7 trades`
- interpretation:
  - `Candidate E` 在 `wf` 切窗上不是完全失效，但表現明顯受區段影響。
  - 整段驗證窗過門檻，不代表每個月都穩定過門檻。
  - 目前比較合理的判斷是：
    - `有訊號`
    - `具備可交易潛力`
    - `尚未達到長期穩定`
- status:
  - `Validated but regime-sensitive`

### Attempt 2026-05-26 / Candidate E Support Check On all_wf Windows

- scope:
  - 用較弱證據的 `breakout10_predictions_all_wf.csv` 做較長期佐證，確認 `Candidate E` 參數區是否只在最近區間才有效。
  - 驗證窗：
    - `2025-08-01 ~ 2025-11-30`
    - `2025-12-01 ~ 2026-02-26`
- findings:
  - 在前半段 `2025-08 ~ 2025-11`，`Candidate E` 代表組合表現仍強：
    - `topk=4`, `gap_th=0.010`, `hold_days=15`, `max_positions=5`
    - `valid_trades=25`
    - `valid_win=0.7600`
    - `valid_mean_net=0.1798`
    - `topk=4`, `gap_th=0.005`, `hold_days=15`, `max_positions=6`
    - `valid_trades=30`
    - `valid_win=0.8000`
    - `valid_mean_net=0.2065`
  - 在後半段 `2025-12 ~ 2026-02-26`，相同區域也非常強，但交易數較少：
    - `topk=4`, `gap_th=0.010`, `hold_days=15`, `max_positions=5`
    - `valid_trades=13`
    - `valid_win=1.0000`
    - `valid_mean_net=0.2635`
    - `topk=4`, `gap_th=0.005`, `hold_days=15`, `max_positions=6`
    - `valid_trades=14`
    - `valid_win=1.0000`
    - `valid_mean_net=0.2321`
- interpretation:
  - 雖然 `all_wf` 不是最嚴格的 OOF 證據，但它提供了一個重要訊號：
    - `Candidate E` 的參數區不是只在單一短期區段有效
    - 相同結構在更長時間範圍內也能維持高報酬高勝率
  - 這提升了該策略區域的可信度，但不能取代 `wf` 的主驗證地位。
- status:
  - `Supportive long-range evidence`

### Attempt 2026-05-26 / Candidate E Replay Artifact

- scope:
  - 新增單一策略回放工具，把已確認的候選參數直接輸出成 `summary + trades` artifact，方便後續長窗檢查與人工檢閱。
- tool:
  - `apps/analysis/run_strategy_replay.py`
- replayed config:
  - preds: `data/_derived/ml_runs/breakout10_predictions_wf.csv`
  - window: `2025-11-01 ~ 2026-02-03`
  - `topk=4`
  - `gap_th=0.010`
  - `hold_days=15`
  - `max_positions=5`
  - no `tp/sl/trail`
- artifacts:
  - `data/_derived/ml_runs/candidate_e_wf_summary.json`
  - `data/_derived/ml_runs/candidate_e_wf_trades.csv`
- findings:
  - replay summary 精確對上先前 grid 搜索結果：
    - `trades=20`
    - `win_net_ret=0.8000`
    - `mean_net_ret=0.1587`
    - `total_net_ret=0.7806`
    - `signals=14`
  - 代表這組候選已不只是 grid 表格上的一列，而是可重播、可審閱的正式基線。
- interpretation:
  - 之後若要做更長窗、換切窗、或人工檢查單筆交易結構，應以這支 replay 工具為主，而不是再從搜索腳本反推。

### Attempt 2026-05-27 / Candidate E Rolling Window Stability

- scope:
  - 不再只看月切窗，改用固定長度、互相重疊的 rolling windows 檢查 `Candidate E` 區域內三組代表配置：
    - `A = topk=4, gap_th=0.010, hold_days=15, max_positions=5`
    - `B = topk=5, gap_th=0.005, hold_days=15, max_positions=5`
    - `C = topk=4, gap_th=0.005, hold_days=15, max_positions=6`
  - 使用 `breakout10_predictions_wf.csv`
- artifact:
  - `data/_derived/ml_runs/candidate_e_wf_rolling_windows.csv`
- rolling windows:
  - `2025-10-01 ~ 2025-11-15`
  - `2025-10-15 ~ 2025-11-30`
  - `2025-11-01 ~ 2025-12-15`
  - `2025-11-15 ~ 2025-12-31`
  - `2025-12-01 ~ 2026-01-15`
  - `2025-12-15 ~ 2026-02-03`
- findings:
  - `A` 的平均表現最好：
    - average win: `0.7167`
    - average mean_net: `0.1047`
    - strict pass windows (`win > 0.70 and mean_net > 0.10`): `2 / 6`
  - `C` 次之：
    - average win: `0.6667`
    - average mean_net: `0.1016`
    - strict pass windows: `2 / 6`
  - `B` 最弱：
    - average win: `0.6167`
    - average mean_net: `0.0946`
    - strict pass windows: `0 / 6`
  - 三組配置都在同一段窗口失效：
    - `2025-10-15 ~ 2025-11-30`
    - `A`: `0.4000 / 0.0168`
    - `B`: `0.2000 / -0.0277`
    - `C`: `0.3333 / -0.0190`
- interpretation:
  - 問題看起來不是單一參數點選錯，而是整個 `Candidate E` 區域都對某個市場區段敏感。
  - 這比較像缺少 regime filter，而不是單純還沒把 `topk/gap/max_positions` 調到最漂亮。
  - 目前三者裡面，`A` 仍是最合理的正式基線。

### Attempt 2026-05-27 / Candidate E With days_above Cross Filter

- scope:
  - 測試是否能用第二條較偏結構強勢的訊號 `days_above_ge5_predictions.csv` 當交叉濾網，改善 `Candidate E` 的壞窗。
  - 方法：
    - 保持 `Candidate A` 交易規則不變
    - 只保留 `days_above` 預測分數高於門檻的 `breakout10` 候選
  - 門檻：
    - `0.45`
    - `0.50`
    - `0.55`
- artifact:
  - `data/_derived/ml_runs/candidate_e_crossfilter_daysabove_scan.csv`
- findings:
  - 不加濾網時：
    - full window: `0.8000 / 0.1587`
    - bad window: `0.4000 / 0.0168`
  - `days_pred >= 0.45`：
    - full window 降為 `0.5500 / 0.1316`
    - bad window 變成 `0.3000 / -0.0497`
  - `days_pred >= 0.50`：
    - full window 降為 `0.5000 / 0.0441`
    - bad window 仍只有 `0.4000 / 0.0067`
  - `days_pred >= 0.55`：
    - full window 降為 `0.4500 / 0.0179`
    - bad window `0.5000 / 0.0080`
- interpretation:
  - `days_above` 這條交叉濾網沒有改善壞窗，反而明顯破壞整段表現。
  - 這代表：
    - `breakout10` 與 `days_above` 的強勢結構不一定是互補
    - 至少用這種硬門檻交叉方式，沒有形成更好的策略
- decision:
  - 暫時排除「用 `days_above` 門檻當第二層濾網」這條路。

### Attempt 2026-05-27 / Candidate A Simple Market Regime Filters

- scope:
  - 針對目前最好的正式基線 `Candidate A`：
    - `topk=4`
    - `gap_th=0.010`
    - `hold_days=15`
    - `max_positions=5`
  - 不改個股模型，只從 `ohlc.parquet` 4 碼股票 universe 建立簡單市場 regime 指標，測試是否能切掉 `2025-10-15 ~ 2025-11-30` 這段壞窗。
- market features:
  - `breadth_ma20`
    - 全市場 4 碼股票中，收盤價高於 20MA 的比例
  - `median_ret20`
    - 全市場 4 碼股票的 20 日報酬中位數
- artifacts:
  - `data/_derived/ml_runs/candidate_e_regime_filter_scan.csv`
- observations before filtering:
  - 壞日確實偏向低 breadth、負 20 日中位報酬，例如：
    - `2025-11-04`: `breadth_ma20=0.2611`, `median_ret20=-0.0285`
    - `2025-11-05`: `breadth_ma20=0.2566`, `median_ret20=-0.0280`
  - 但也存在低 regime 下的好交易日，例如：
    - `2025-11-03`: `breadth_ma20=0.3579`, `median_ret20=-0.0208`, mean trade ret `0.2244`
- filters tested:
  - `breadth_ma20 >= 0.30`
  - `breadth_ma20 >= 0.35`
  - `breadth_ma20 >= 0.40`
  - `median_ret20 >= -0.02`
  - `median_ret20 >= -0.01`
  - `median_ret20 >= 0.00`
  - 組合條件：
    - `breadth_ma20 >= 0.35 and median_ret20 >= -0.01`
    - `breadth_ma20 >= 0.30 and median_ret20 >= -0.01`
- findings:
  - 原始 baseline：
    - full window: `0.8000 / 0.1587`
    - bad window: `0.4000 / 0.0168`
  - `breadth_ma20 >= 0.35`：
    - full window 降為 `0.6500 / 0.1179`
    - bad window 仍只有 `0.3000 / 0.0226`
  - `median_ret20 >= 0.00`：
    - bad window 完全被切掉
    - 但 full window 也掉到 `0.7692 / 0.0890`
  - 其餘 `ret20` 與 `breadth + ret20` 組合也都類似：
    - 壞窗改善有限或直接清空
    - 同時把整段優勢一起削弱到低於目標
- interpretation:
  - 簡單市場 regime 確實與壞窗有關，但它不夠精準，無法區分：
    - 壞市場裡的不該做 breakout 訊號
    - 壞市場裡仍能成功的少數強訊號
  - 換句話說：
    - `Candidate A` 的問題不是單靠單一 market breadth / medium-term momentum 門檻就能解掉
  - 目前不適合把這類簡單 regime filter 直接加進正式策略。
- decision:
  - 暫時排除「單一市場 breadth / ret20 門檻」作為正式 regime filter。

### Attempt 2026-05-27 / Candidate A Chip Filters On Early Window

- scope:
  - 回到 `Analysis_BsReport_v4` 精神，嘗試用 `features.parquet` 中的籌碼特徵當第二層個股 filter，而不是再疊新的市場濾網。
  - 因 `features.parquet` 目前只到 `2025-12-11`，本輪只驗：
    - `early_full = 2025-11-01 ~ 2025-12-11`
    - `bad = 2025-10-15 ~ 2025-11-30`
    - `recovery = 2025-11-25 ~ 2025-12-11`
- artifacts:
  - `data/_derived/ml_runs/candidate_e_chip_filter_scan_early.csv`
  - `data/_derived/ml_runs/candidate_e_chip_filter_scan_early_v2.csv`
- features tested:
  - `dyn_k`
  - `hhi_posnet`
  - `stock_dyn_k_10`
  - `trend_strength_20`
  - `stock_net_buy_days_10`
  - `stock_net_buy_days_20`
  - `warrant_hhi_posnet_20`
  - `top_posnet_ratio`
- findings:
  - baseline 在 `early_full` 只有：
    - `win=0.7000`
    - `mean_net=0.0799`
  - `dyn_k >= 20` 能把 `early_full` 拉到：
    - `win=0.7000`
    - `mean_net=0.1732`
    - 但 `bad` 仍為：
    - `win=0.5000`
    - `mean_net=-0.0136`
  - `top_posnet_ratio <= 0.605` 的結果和 `dyn_k >= 20` 幾乎相同，表示這兩者在樣本內高度重疊。
  - `stock_net_buy_days_20 >= 1` 是本輪另一個值得保留的次佳訊號：
    - `early_full`:
      - `win=0.6000`
      - `mean_net=0.1013`
    - `bad`:
      - `win=0.6000`
      - `mean_net=0.0491`
    - 它沒有達標，但確實改善了壞窗。
  - `warrant_hhi_posnet_20 <= 0.75` 對 `early_full` 有幫助：
    - `win=0.8000`
    - `mean_net=0.1507`
    - 但對 `bad` 幾乎沒改善：
    - `win=0.4000`
    - `mean_net=0.0089`
  - `hhi_posnet` 上限、`trend_strength_20`、`stock_dyn_k_10` 等條件都沒有形成真正更好的策略。
- interpretation:
  - 單一籌碼特徵還不夠，但這輪至少確認兩件事：
    - `dyn_k / top_posnet_ratio` 這類主力結構特徵，確實能強化好區間
    - `stock_net_buy_days_20` 這類持續性特徵，對壞窗有部分修復能力
  - 這表示下一步若要走 `BsReport_v4 + breakout10` 融合線，最合理的方向不是單一硬門檻，而是：
    - `dyn_k` 類強度
    - `net_buy_days` 類持續性
    - possibly `warrant_hhi` 類權證結構
    - 做小型二階交互條件
- status:
  - `Promising signals, no thresholded solution yet`

### Attempt 2026-05-27 / Candidate A Chip Interaction Filters

- scope:
  - 延續上一輪結論，不再測單一籌碼欄位，改做小型二階交互條件。
  - 只使用前面已顯示有訊號的欄位：
    - `dyn_k`
    - `top_posnet_ratio`
    - `stock_net_buy_days_20`
    - `warrant_hhi_posnet_20`
  - 驗證窗仍限於 `features.parquet` 有覆蓋的早期區間。
- artifact:
  - `data/_derived/ml_runs/candidate_e_chip_interaction_scan_early.csv`
- conditions tested:
  - `dyn_k >= 20 and stock_net_buy_days_20 >= 1`
  - `dyn_k >= 20 and stock_net_buy_days_20 >= 1 and warrant_hhi_posnet_20 <= 0.75`
  - `dyn_k >= 20 and top_posnet_ratio <= 0.605 and stock_net_buy_days_20 >= 1`
  - `stock_net_buy_days_20 >= 1 and warrant_hhi_posnet_20 <= 0.75`
  - `dyn_k >= 40 or stock_net_buy_days_20 >= 1`
  - `dyn_k >= 20 and warrant_hhi_posnet_20 <= 0.75`
- findings:
  - `dyn_k` 類交互條件大多仍偏向放大好區間，但對壞窗沒有真正修復力。
  - 最值得保留的是：
    - `stock_net_buy_days_20 >= 1 and warrant_hhi_posnet_20 <= 0.75`
    - `early_full`:
      - `win=0.6000`
      - `mean_net=0.1013`
    - `bad`:
      - `win=0.6000`
      - `mean_net=0.0464`
    - `recovery`:
      - `win=0.6000`
      - `mean_net=0.1802`
  - 這組合不是最強，但它是目前少數同時做到：
    - 保留 `early_full` 的 `>10%` 平均報酬
    - 又把 `bad` window 從接近零或負值拉回明顯正值
  - 相對地：
    - `dyn_k >= 20 and warrant_hhi_posnet_20 <= 0.75`
    - 雖然 `early_full` 很強：`0.7000 / 0.1732`
    - 但 `bad` 仍為負：`0.5000 / -0.0343`
- interpretation:
  - 這輪透露一個重要方向：
    - `dyn_k` 類強度特徵比較像放大 breakout 已經有效的區間
    - `net_buy_days + warrant_hhi` 這類持續性 / 結構特徵，比較像用來修補壞窗
  - 也就是說，若後續要做真正的 second-stage filter，可能不該是單一路徑，而是：
    - 用 `dyn_k` 決定強度
    - 用 `net_buy_days / warrant_hhi` 決定是否避開脆弱訊號
- status:
  - `Interaction signal found, still below final target`

### Attempt 2026-05-27 / Candidate A Dual-Layer Threshold Grid

- scope:
  - 針對剛剛拆出的兩種功能線做小格點：
    - `strong`:
      - `dyn_k`
    - `repair`:
      - `stock_net_buy_days_20`
      - `warrant_hhi_posnet_20`
  - 不做大範圍盲搜，只掃有限閾值：
    - `dyn_k`: `0, 10, 20, 30, 40`
    - `stock_net_buy_days_20`: `0, 1, 2`
    - `warrant_hhi_posnet_20`: `0.70, 0.75, 0.80, 1.00`
    - 組合方式：
      - `or`
      - `and`
- artifact:
  - `data/_derived/ml_runs/candidate_e_dual_layer_grid_early.csv`
- acceptance rule for this scan:
  - `early_full mean_net >= 0.10`
  - `bad mean_net >= 0.00`
  - `early_full trades >= 8`
  - `bad trades >= 8`
- findings:
  - 合格組合共有 `22` 組，但大致分成兩類：
    - `warrant_hhi` 主導的寬鬆組合
    - `stock_net_buy_days_20 + warrant_hhi` 主導的修補組合
  - 表現最強的一組是：
    - `warrant_hhi_posnet_20 <= 0.75`
    - 這其實等價於 `dyn_th=0, buy20_th=0, whhi_th=0.75, mode=and`
    - `early_full`:
      - `win=0.8000`
      - `mean_net=0.1507`
    - `bad`:
      - `win=0.4000`
      - `mean_net=0.0089`
    - 它的優點是保住了整體報酬，缺點是壞窗勝率仍然很差。
  - 最像「修補壞窗」的一小塊區域是：
    - `stock_net_buy_days_20 >= 1`
    - `warrant_hhi_posnet_20 <= 0.80` 或 `1.00`
    - `mode=and`
    - `early_full`:
      - `win=0.6000`
      - `mean_net=0.1013`
    - `bad`:
      - `win=0.6000`
      - `mean_net=0.0491`
    - 這是目前最乾淨的「early_full 仍過 10%，bad window 也維持正值」解。
  - 中間型組合例如：
    - `dyn_k >= 30` with loose repair `or`
    - `early_full = 0.1374`
    - `bad = 0.0206`
    - 代表也存在一些兼顧型區域，但仍未達到高勝率。
- interpretation:
  - 這輪最重要的進展不是找到最終策略，而是把 dual-layer 方向分成兩個可操作分支：
    - `high-return branch`
      - 以 `warrant_hhi` 為主
      - 優先保住整體 alpha
      - 接受壞窗勝率不佳
    - `repair branch`
      - 以 `stock_net_buy_days_20 + warrant_hhi` 為主
      - 優先讓壞窗回正
      - 接受整體勝率暫時不足
  - 下一步若要真的推向最終策略，應該不是再疊更多硬條件，而是：
    - 把這兩個 branch 做成切換邏輯
    - 或引入 day-level quality / regime classifier 來決定該用哪一支
- status:
  - `Best dual-layer threshold region so far, still not final`

### Attempt 2026-05-27 / Repair Branch Full-Range Validation

- scope:
  - 不再停留在早期窗口，直接用完整日期重建 `repair branch` 所需特徵：
    - `stock_net_buy_days_20`
    - `warrant_hhi_posnet_20`
  - 原始來源：
    - `flow_stock_daily.parquet`
    - `flow_warrant_daily_by_underlying.parquet`
  - rolling 定義沿用 `Analysis_BsReport_v1`：
    - window=`20`
    - `min_periods=6`
- artifacts:
  - `data/_derived/ml_runs/candidate_e_full_repair_branch_validation.csv`
  - `data/_derived/ml_runs/breakout10_predictions_wf_repair_branch.csv`
- repaired branch definition:
  - `stock_net_buy_days_20 >= 1`
  - `warrant_hhi_posnet_20 <= 0.80`
- findings:
  - 這條 `repair branch` 在完整 `2025-11-01 ~ 2026-02-03` 上，首次明確同時達標：
    - `trades=20`
    - `win=0.7500`
    - `mean_net=0.1410`
    - `total_net=0.7081`
  - 分段看：
    - `2025-11`: `0.6000 / 0.1052`
    - `2025-12`: `0.9000 / 0.2624`
    - `2026-01-01 ~ 2026-02-03`: `1.0000 / 0.4074`
- interpretation:
  - 這是目前第一條在較長驗證窗上，明確跨過目標門檻的 `BsReport_v4 + breakout10` 融合線。
  - 但單月 / rolling 穩定性還需要再驗，不能只靠這個 full-window 成績就宣稱完成。

### Attempt 2026-05-27 / Repair Branch Rolling Windows

- scope:
  - 對完整日期重建後的 `repair branch` 做 rolling window 驗證，避免只看整段平均。
- artifacts:
  - `data/_derived/ml_runs/repair_branch_rolling_windows.csv`
- findings:
  - rolling windows 結果：
    - `2025-10-01 ~ 2025-11-15`: `0.4000 / 0.0690`
    - `2025-10-15 ~ 2025-11-30`: `0.6000 / 0.0491`
    - `2025-11-01 ~ 2025-12-15`: `0.6000 / 0.1052`
    - `2025-11-15 ~ 2025-12-31`: `0.6000 / 0.1137`
    - `2025-12-01 ~ 2026-01-15`: `0.9000 / 0.2624`
    - `2025-12-15 ~ 2026-02-03`: `1.0000 / 0.2925`
  - strict pass windows (`win > 0.70 and mean_net > 0.10`)：
    - `2 / 6`
- interpretation:
  - full-window 已達標，但 rolling 穩定度還不足，尤其早段仍弱。
  - 這表示 repair filter 雖然成功把策略推進到「候選可交易解」，但還沒到「長期驗證完成」。

### Attempt 2026-05-27 / Repair Branch Strategy Search

- scope:
  - 固定 `repair branch` universe，直接在 branch 內做局部策略搜索。
  - 搜索空間：
    - `topk / top_pct`
    - `gap_th`
    - `hold_days 13~16`
    - `max_positions 4~6`
    - 少量 `tp/sl`
- artifacts:
  - `data/_derived/ml_runs/_tmp_repair_branch_strategy_search.csv`
  - `data/_derived/ml_runs/_tmp_repair_branch_strategy_search_best.csv`
  - `data/_derived/ml_runs/repair_branch_best_summary.json`
  - `data/_derived/ml_runs/repair_branch_best_trades.csv`
  - `data/_derived/ml_runs/repair_branch_best_rolling_windows.csv`
- strongest candidate:
  - `select_mode=top_pct`
  - `topk=3` (cap)
  - `top_pct=0.02`
  - `gap_th=0.010`
  - `hold_days=16`
  - `max_positions=6`
  - no `tp/sl`
- full-window result:
  - `trades=20`
  - `win=0.8500`
  - `mean_net=0.2288`
  - `total_net=0.9540`
- rolling windows:
  - `2025-10-01 ~ 2025-11-15`: `0.5833 / 0.0789`
  - `2025-10-15 ~ 2025-11-30`: `0.6667 / 0.0472`
  - `2025-11-01 ~ 2025-12-15`: `0.7500 / 0.1614`
  - `2025-11-15 ~ 2025-12-31`: `0.7500 / 0.1398`
  - `2025-12-01 ~ 2026-01-15`: `0.9167 / 0.2627`
  - `2025-12-15 ~ 2026-02-03`: `0.9091 / 0.2226`
  - strict pass windows:
    - `4 / 6`
- comparison vs other top repair-branch candidates:
  - 在我抽查的前五個 distinct 候選中，這組同時擁有：
    - 最高 `rolling_pass`
    - 最高 `full_mean`
    - 最高 `rolling_avg_mean`
- interpretation:
  - 這是目前最強、最接近最終答案的候選。
  - 它已經不只是 full-window 漂亮，而是 rolling 穩定度也明顯優於先前 baseline 與其他 branch 內候選。
  - 但因為仍有 `2 / 6` 視窗未過嚴格門檻，所以目前最合理的定位是：
    - `strongest current candidate`
    - 不是 `long-term verified strategy`

### Attempt 2026-05-27 / Best Candidate Day-Level Quality Filters

- scope:
  - 只針對目前最強候選：
    - `repair branch`
    - `top_pct=0.02`
    - `gap_th=0.010`
    - `hold_days=16`
    - `max_positions=6`
  - 不再動 strategy grid，只掃描 day-level 品質門檻，判斷當天是否交易。
- day quality features:
  - 以當天實際會選入的候選集計算：
    - `mean_pred`
    - `min_pred`
    - `pred_spread`
    - `picks`
- artifact:
  - `data/_derived/ml_runs/repair_branch_day_quality_filter_scan.csv`
- scan result:
  - 有 `74` 組條件能保住：
    - `full_win > 70%`
    - `full_mean > 10%`
  - 但沒有任何一組把 rolling strict pass 從 `4 / 6` 提升到更高。
  - 最佳類型的條件大致是：
    - `min_pred >= 0.62`
    - `pred_spread <= 0.08 ~ 0.12`
  - 代表例子：
    - `mean_th >= 0.58`
    - `min_th >= 0.62`
    - `pred_spread <= 0.12`
    - `full`:
      - `trades=13`
      - `win=0.8462`
      - `mean_net=0.2038`
    - rolling strict pass:
      - `4 / 6`
  - 但這些條件的共通問題是：
    - 沒有修復兩個最弱的早期視窗
    - 只是把後段更強的交易保留下來
    - 同時犧牲交易數
- interpretation:
  - 這代表目前的問題不是「少數低品質交易日混進來」而已。
  - 更像是早期兩個弱窗整體 market/label 結構就不同，單靠簡單的 day-level score quality 門檻無法解掉。
  - 因此：
    - day-level quality filter 可作為保守版風控工具
    - 但不是把策略推向長期穩定的關鍵突破點
- status:
  - `No improvement over 4/6 rolling pass`

### Attempt 2026-05-27 / Repair Branch Signal-Level Chip Grid

- scope:
  - 直接在目前最強候選的 `repair branch` universe 上做完整 signal-level chip filter 格點。
  - 使用 4 個欄位：
    - `dyn_k`
    - `top_posnet_ratio`
    - `stock_net_buy_days_20`
    - `warrant_hhi_posnet_20`
  - 目的：
    - 不是再找新 branch
    - 而是看能不能在 `repair branch` 內部再提高 rolling 穩定度
- artifact:
  - `data/_derived/ml_runs/repair_branch_signal_chip_grid.csv`
- best finding:
  - 單純加入：
    - `top_posnet_ratio <= 0.610`
  - 就能把先前 `4 / 6` rolling strict pass 提升到 `5 / 6`
  - 同時保住很強的 full-window 成績：
    - `full_trades=20`
    - `full_win=0.8500`
    - `full_mean=0.2215`
  - 代表性 rolling 結果：
    - `w1`: `0.7273 / 0.1242`
    - `w2`: `0.6667 / 0.0447`
    - `w3`: `0.7500 / 0.1171`
    - `w4`: `0.7500 / 0.1156`
    - `w5`: `0.9167 / 0.2948`
    - `w6`: `0.9091 / 0.2226`
- interpretation:
  - `top_posnet_ratio` 這個欄位在目前階段比想像中更關鍵。
  - 它不像 `dyn_k` 那樣只是在好區間放大 alpha，而是能把最早那個最差窗口中的其中一個弱窗拉回可接受區。
  - 現在唯一仍明顯卡住的是：
    - `w2 = 2025-10-15 ~ 2025-11-30`

### Attempt 2026-05-27 / Refined Universe Strategy Search

- scope:
  - 把上一步得到的 refined universe 固定成：
    - `repair branch`
    - `top_posnet_ratio <= 0.610`
  - 再在此 universe 內做局部策略搜索。
- artifacts:
  - `data/_derived/ml_runs/breakout10_predictions_wf_repair_branch_topratio0610.csv`
  - `data/_derived/ml_runs/_tmp_repair_branch_topratio0610_strategy_search.csv`
  - `data/_derived/ml_runs/_tmp_repair_branch_topratio0610_strategy_search_best.csv`
  - `data/_derived/ml_runs/repair_branch_topratio0610_best_summary.json`
  - `data/_derived/ml_runs/repair_branch_topratio0610_best_trades.csv`
  - `data/_derived/ml_runs/repair_branch_topratio0610_best_rolling_windows.csv`
- strongest refined candidate:
  - `select_mode=top_pct`
  - `top_pct=0.015`
  - `gap_th=0.005`
  - `hold_days=16`
  - `max_positions=6`
  - no `tp/sl`
- full-window result:
  - `trades=20`
  - `win=0.9000`
  - `mean_net=0.2261`
  - `total_net=0.9386`
- rolling windows:
  - `w1`: `0.8000 / 0.1443`
  - `w2`: `0.5833 / 0.0871`
  - `w3`: `0.8333 / 0.1641`
  - `w4`: `0.7500 / 0.1156`
  - `w5`: `1.0000 / 0.2896`
  - `w6`: `1.0000 / 0.2400`
  - strict pass:
    - `5 / 6`
- comparison vs prior strongest candidate:
  - 前一版 strongest candidate：
    - `win=0.8500`
    - `mean_net=0.2288`
    - rolling `4 / 6`
  - 新 refined candidate：
    - `win=0.9000`
    - `mean_net=0.2261`
    - rolling `5 / 6`
  - 雖然 full mean 略低，但穩定度明顯更好，因此目前應把它視為新的最強候選。
- distinct-top candidate rolling comparison:
  - 我另外把 refined search 的前 20 個 distinct 候選拿去做 rolling 檢查。
  - 結果沒有任何一組達到 `6 / 6`。
  - 最好的幾組都卡在同一個弱窗：
    - `w2 = 2025-10-15 ~ 2025-11-30`
  - 其中目前最強的是：
    - `top_pct=0.015`
    - `gap_th=0.010`
    - `hold_days=17`
    - `max_positions=6`
    - full: `0.8947 / 0.2382`
    - rolling: `5 / 6`
    - 弱窗仍是 `w2: 0.583 / 0.029`
- interpretation:
  - 這代表在現有訊號與 filter 框架下，`w2` 幾乎是共同瓶頸，而不是單一規則選錯。
  - 目前已經相當接近長期穩定，但還差最後一個弱窗無法通過嚴格門檻。
- status:
  - `New strongest current candidate`
