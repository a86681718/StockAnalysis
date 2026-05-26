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
