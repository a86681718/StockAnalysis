# Key Broker Anomaly Detection Research Log

## Purpose

Build detection-only broker-branch event families before studying future returns.
Each direction must be completed and documented before work begins on the next
direction. Price appreciation, holding periods, and backtest performance must
not be used to define an event.

## Research Order

1. Single local-branch persistent accumulation (completed 2026-06-21)
2. Multiple branches used by one possible capital source (pending)
3. Regional branch clustering near company locations (pending)
4. Stock and warrant joint accumulation (pending)
5. Warrant-leading-stock accumulation (pending)

Do not combine pending directions into the first detector. They require separate
hypotheses, features, validation cases, and output artifacts.

## Direction 1: Single Local-Branch Persistent Accumulation

### Hypothesis

A local broker branch that changes from low participation to persistent,
high-purity, economically meaningful buying represents an abnormal accumulation
episode. The episode is interesting regardless of subsequent stock returns.

Calibration case: `1532` / `779U` / `國票-長城`.

### Fixed Scope

- Stock-side broker transactions only.
- One underlying stock and one broker branch per episode.
- Local branch names only. Headquarters, institutional desks, proprietary desks,
  underwriting desks, and names without a branch suffix are excluded.
- Unknown broker codes are excluded because locality cannot be audited.
- ETF symbols are excluded.
- Future prices and returns are excluded from all feature and ranking logic.
- Warrant transactions and multi-branch aggregation are excluded from Direction 1.

### User Market Priors

- A controlling capital source may split orders across multiple broker branches.
  This is recorded for Direction 2 and must not be inferred in Direction 1.
- Influential branch behavior is less likely in mega-cap weighted stocks such as
  `2330` and `2317` because a local branch is less likely to represent meaningful
  market participation.
- The relevant branch is usually a local branch such as `新光-新竹`, not a central
  or institutional trading desk.

The data identifies executing broker branches, not the beneficial owner or
investor category. It cannot prove that a trade belongs to an investment trust,
insider, or single controlling capital source.

### Calibration Facts Verified on 2026-06-21

Source: `data/bs_report/parquet_twse/1532.parquet`, latest local row date
`2026-06-17`.

For `2026-02-23` through `2026-06-17`:

- stock report dates: `80`
- active `國票-長城` dates: `61`
- buy dates: `58`
- positive-net dates: `57`
- total buy volume: `5,569,000` shares
- total sell volume: `187,000` shares
- cumulative net buy: `5,382,000` shares
- net-buy purity: `96.64%`
- branch share of all reported stock buy volume: `10.90%`

Before the episode, from `2025-07-01` through `2026-02-22`, the branch had only
two buy dates and cumulative net selling of `56,042` shares in its active rows.
The important behavior is the low-participation to persistent-buyer state change.

A simple five-session prospective screen using net buy at least `100,000`, at
least three positive-net sessions, branch buy share at least `5%`, and net-buy
purity at least `80%` first identifies the case on `2026-03-03`. These values are
a calibration reference, not optimized production thresholds.

### Data Quality Findings

- The raw 1532 broker file contains `2026-03-28`, which is not a normal trading
  session.
- The raw file contains `2026-04-15`, but the current derived OHLC table has no
  matching 1532 row for that date.
- Broker codes must be normalized case-insensitively: the broker list uses
  `779u`, while newer outputs may use `779U`.

Direction 1 must use stock broker-report dates as its primary event calendar and
report invalid/non-OHLC dates separately. Missing OHLC must not silently remove a
broker event because price is outside this detector's scope.

### Detector Design

The detector unit is an episode, not a daily signal. Daily features are retained
for audit, but adjacent qualifying dates for the same stock and branch are
merged into one episode.

Required dimensions:

- Surprise: recent activity relative to the branch-stock pair's trailing history.
- Persistence: active, buy, and positive-net sessions within short and long windows.
- Purity: cumulative net buy divided by cumulative buy volume.
- Market significance: branch buy volume divided by all reported stock buy volume.
- Accumulation size: cumulative net buy and average positive-day net buy.
- Retention: cumulative sell volume relative to cumulative buy volume.
- Acceleration: short-window net buying relative to the longer-window pace.
- Episode age and status: onset, confirmed, accelerating, cooling, or unwinding.

Avoid unstable ratios with a near-zero historical denominator. Dormant-to-active
pairs need a separate novelty measure rather than requiring prior buy activity.

### Initial Gate Philosophy

Use several interpretable gates instead of one opaque anomaly score. A candidate
must show:

- more than a one-day burst;
- high directional purity;
- meaningful participation in the stock's reported buy volume;
- enough absolute net buying to avoid tiny illiquid prints;
- a local branch identity that passes the desk exclusion rules.

Mega-cap behavior should primarily fail the market-significance gate. An explicit
symbol exclusion list may be used for known examples, but it must be visible in
the output and must not substitute for a market-cap data source that the repo
currently lacks.

### Direction 1 Acceptance Checks

- Reproduce `1532 / 國票-長城` as one persistent episode.
- Detect onset close to early March 2026 rather than only after the later price move.
- Show all component metrics and exact gate pass/fail reasons.
- Produce full-market candidates without future-return columns.
- Prevent one-day block trades from ranking as persistent accumulation.
- Exclude central/non-local desk names and unknown broker identities.
- Keep mega-cap exclusions and reasons auditable.
- Include deterministic unit tests for episode construction and exclusion rules.

### Current Status

`2026-06-21`: completed, calibrated, and scanned across the locally available
market data through `2026-06-17`.

### Resume Point

1. Read this file completely.
2. Inspect `src/stockanalysis/analysis/broker_branch_accumulation.py` only for
   reusable loading and normalization functions.
3. Implement Direction 1 in a separate detection-only module and output folder.
4. Run the 1532 calibration case first.
5. Run the full market only after calibration checks pass.
6. Append commands, findings, rejected assumptions, and output paths below.

## Execution Log

### 2026-06-21

- Confirmed that the existing key-broker strategy feature table contains 62
  `1532 / 國票-長城` rows and the selected signal table contains dates
  `2026-03-16`, `2026-05-11`, and `2026-06-10`.
- Confirmed that the existing general broker-branch accumulation top output has
  no exact `1532 / 國票-長城` row. It emphasizes short stock/warrant windows and
  is not the Direction 1 episode detector.
- Decided not to modify either existing pipeline because their output contracts
  include other event types and strategy-oriented assumptions.

#### Direction 1 Implementation

- Added `src/stockanalysis/analysis/single_branch_persistent_accumulation.py`.
- Added deterministic tests in
  `tests/test_single_branch_persistent_accumulation.py`.
- Kept the new detector independent from existing stock/warrant and trading
  strategy pipelines.
- Used broker-report dates as the event calendar and did not load OHLC or price
  returns.
- Implemented five-session onset, twenty-session confirmation, sixty-session
  trailing baseline, and session-gap episode merging.
- Added local-branch desk exclusions. Names containing `總公司`, `營業`,
  `經紀部`, `法人部`, `自營`, `承銷`, `國際部`, `金融交易部`, or `債券部`
  are not treated as local branches.
- Added visible default symbol exclusions for `2317` and `2330`. This is a
  temporary explicit market prior, not a complete market-cap filter.
- Added confidence levels: `preliminary`, `developing`, `confirmed`, and
  `established`.
- Added `broad_broker_flag` when one broker appears in at least 20 ongoing stock
  episodes. Broad brokers remain in the audit output but do not enter the manual
  review list.

#### Calibration Run

Command:

```bash
PYTHONPATH=src .venv/bin/python -m stockanalysis.analysis.single_branch_persistent_accumulation \
  --symbols 1532 \
  --end-date 2026-06-17 \
  --excluded-symbols '' \
  --output-dir outputs/analysis/single_branch_persistent_accumulation_calibration
```

Final calibration result for `1532 / 779U / 國票-長城`:

- episode start: `2026-03-03`
- last qualifying date: `2026-06-17`
- status: `confirmed`
- confidence: `established`
- observed sessions after first trigger: `75`
- positive-net sessions: `54`
- cumulative buy: `5,469,000` shares
- cumulative sell: `187,000` shares
- cumulative net buy: `5,282,000` shares
- cumulative purity: `96.58%`
- cumulative branch buy share: `8.98%`
- qualifying dates: `69`

The cumulative value is lower than the raw `2026-02-23` case total because the
episode intentionally begins at the first prospective trigger on `2026-03-03`.

#### First Full-Market Run and Rejected Ranking

The first full run produced 10,326 historical episodes and 868 ongoing episodes.
It was not accepted as the final review list because:

- `凱基-台北` appeared in 64 ongoing stocks and dominated the candidate surface;
- `元富-營業` was incorrectly treated as a local branch;
- recent high-share events ranked above long-established accumulation episodes;
- 4,228 ended episodes had fewer than three qualifying dates.

The complete audit rows were useful, but the first ranking was too broad for
manual review. No price or return results were consulted when making this
decision.

#### Final Full-Market Run

Command:

```bash
PYTHONPATH=src .venv/bin/python -m stockanalysis.analysis.single_branch_persistent_accumulation \
  --end-date 2026-06-17
```

Final local-data results:

- scanned stock symbols: `1,981`
- historical episodes retained for audit: `10,290`
- ongoing episodes: `850`
- review-eligible ongoing episodes: `276`
- daily qualifying rows: `58,941`
- review confidence counts: `44 established`, `124 confirmed`, `108 developing`
- review status counts: `33 accelerating`, `87 confirmed`, `42 onset`,
  `90 cooling`, `24 unwinding`
- unique review symbols: `230`
- unique review brokers: `200`
- broad broker removed from review: `9268 / 凱基-台北`, with 64 ongoing symbols
- `1532 / 國票-長城`: final review rank `9`
- excluded `2317` and `2330` rows in output: `0`
- branch names containing `營業` in output: `0`

Outputs:

- `outputs/analysis/single_branch_persistent_accumulation/single_branch_persistent_report.md`
- `outputs/analysis/single_branch_persistent_accumulation/single_branch_persistent_summary.json`
- `outputs/analysis/single_branch_persistent_accumulation/single_branch_persistent_episodes.csv`
- `outputs/analysis/single_branch_persistent_accumulation/single_branch_persistent_episodes.parquet`
- `outputs/analysis/single_branch_persistent_accumulation/single_branch_persistent_daily_triggers.parquet`

#### Direction 1 Known Limitations

- The explicit `2317,2330` exclusion is incomplete. A maintained market-cap or
  index-constituent data source is needed before claiming systematic mega-cap
  exclusion.
- A broker branch identifies an execution channel, not a beneficial owner.
- `broad_broker_flag` is based on detector outputs, not raw cross-market client
  breadth. It is an auditable noise control, not proof that a broker is
  institutional.
- The detector may split one economic episode when qualifying gaps exceed ten
  stock-report sessions.
- Absolute net-buy gates can favor higher-liquidity stocks. The branch share and
  purity gates reduce but do not eliminate this scale effect.
- The 276-row review list is a behavior-review queue, not a trade list.

#### Direction 1 Verification

```bash
.venv/bin/python -m py_compile \
  src/stockanalysis/analysis/single_branch_persistent_accumulation.py \
  tests/test_single_branch_persistent_accumulation.py
.venv/bin/python -m unittest tests.test_single_branch_persistent_accumulation
```

Result: three tests passed. Coverage includes local desk exclusion, a dormant
pair becoming one persistent episode, and rejection of a one-day block-buy burst.

#### Next Resume Point

Direction 1 is complete. Start Direction 2 only after reading this log. Direction
2 must detect multiple branches that may represent one distributed capital
source. It must not alter Direction 1 outputs or infer common ownership merely
from synchronous buying.
