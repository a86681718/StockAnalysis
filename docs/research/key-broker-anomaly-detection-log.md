# Key Broker Anomaly Detection Research Log

## Purpose

Build detection-only broker-branch event families before studying future returns.
Each direction must be completed and documented before work begins on the next
direction. Price appreciation, holding periods, and backtest performance must
not be used to define an event.

## Research Order

1. Single local-branch persistent accumulation (in progress)
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

`2026-06-21`: research scope fixed; implementation not yet started.

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
