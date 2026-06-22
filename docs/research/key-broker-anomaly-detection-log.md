# Key Broker Anomaly Detection Research Log

## Purpose

Build detection-only broker-branch event families before studying future returns.
Each direction must be completed and documented before work begins on the next
direction. Price appreciation, holding periods, and backtest performance must
not be used to define an event.

## Research Order

1. Single local-branch persistent accumulation (completed 2026-06-21)
2. Multiple branches used by one possible capital source (completed 2026-06-21)
3. Regional branch clustering near company locations (completed 2026-06-21)
4. Stock and warrant joint accumulation (pending)
5. Warrant-leading-stock accumulation (pending)

Cross-cutting work may add a general stock-side anomaly framework, but it must
remain separate from these hypothesis-specific directions and may not use
future returns to choose cases.

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
  `經紀部`, `法人`, `自營`, `承銷`, `國際部`, `國際證券`, `金融交易部`,
  `債券部`, or `網路` are not treated as local branches.
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
- historical episodes retained for audit: `10,175`
- ongoing episodes: `843`
- review-eligible ongoing episodes: `274`
- daily qualifying rows: `58,343`
- review confidence counts: `44 established`, `122 confirmed`, `108 developing`
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
- The 274-row review list is a behavior-review queue, not a trade list.

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

## Direction 2: Distributed Multi-Branch Accumulation

### Status

`2026-06-21`: completed, tested, and scanned across the locally available market
data through `2026-06-17`.

### Hypothesis

Several independently identified local broker branches can begin unusual buying
of the same stock within a short interval and continue accumulating together.
The observable event is coordinated-looking execution across branches. The data
cannot establish that the branches belong to one beneficial owner or controlling
capital source.

### Fixed Scope

- Stock-side broker transactions only.
- Multiple local branches for one underlying stock.
- No company-location or geographic-distance requirement; that belongs to
  Direction 3.
- No warrants; stock/warrant convergence belongs to Direction 4.
- No prices, returns, entries, exits, or parameter selection based on outcomes.
- Reuse Direction 1 local-desk and explicit mega-cap exclusions.
- Do not modify or merge Direction 1 episode outputs.

### Required Member Evidence

Each branch included in a distributed cluster must independently show recent
behavioral change relative to its own branch-stock history. Lower per-branch
market-share gates may be used than Direction 1 because distributed execution is
expected to split size, but a branch cannot qualify only because other branches
are buying.

Candidate member dimensions:

- recent positive-net sessions;
- branch-level net-buy purity;
- recent buying relative to trailing branch-stock activity;
- first unusual-buy date;
- local branch identity and parent broker name;
- broad cross-stock broker flag.

### Required Cluster Evidence

- At least three qualifying local branches.
- At least two distinct parent brokerage firms.
- Member onset dates concentrated within a short synchronization interval.
- Meaningful combined net buying and combined share of stock buy volume.
- High combined net-buy purity.
- The largest branch must not dominate the cluster.
- Removing the largest branch must leave economically meaningful net buying and
  market participation.
- Persistent evidence across several stock-report sessions, not one block day.

Initial calibration references, subject to behavior-only review:

- short window: 5 sessions;
- long window: 20 sessions;
- minimum member positive sessions: 2 of 5;
- minimum cluster branches: 3;
- minimum parent brokers: 2;
- maximum largest-branch share of cluster net buying: 60%;
- cluster net-buy purity: at least 80%;
- cluster buy share of stock reported buy volume: at least 10%.

These are interpretable starting gates, not return-optimized parameters.

### Main False Positives to Reject

- One dominant Direction 1 branch plus several trivial buyers.
- Multiple branches of one broker responding to the same internal sales campaign.
- A generally active broad-market branch appearing in many stocks.
- Tiny illiquid prints that create high percentages with little economic size.
- Broad market participation where many unrelated branches are mechanically on
  both sides without unusual branch-level behavior.
- Consecutive daily rows from one cluster incorrectly counted as separate events.

### Direction 2 Acceptance Checks

- Synthetic coordinated cluster passes.
- Synthetic one-dominant-branch cluster fails after largest-branch removal.
- Three branches from only one parent brokerage fail.
- One-day distributed block buying fails persistence.
- Output exposes member branches, parent brokers, first trigger dates, combined
  metrics, dominance, and exact gate values.
- Full-market output remains a behavior-review queue and contains no future-return
  columns.

### Direction 2 Resume Point

1. Read Direction 1 implementation for loading and branch-name normalization.
2. Build a separate module and output folder for Direction 2.
3. Write synthetic tests before scanning real data.
4. Run a small symbol sample and inspect member-level evidence.
5. Run the full market only after the dominance and parent-broker tests pass.

### Direction 2 Implementation

- Added `src/stockanalysis/analysis/distributed_branch_accumulation.py`.
- Added deterministic tests in
  `tests/test_distributed_branch_accumulation.py`.
- Each member branch must independently pass short-window persistence, purity,
  market-share, and trailing-history novelty gates before cluster aggregation.
- Cluster gates enforce branch count, parent-broker count, combined purity,
  combined market share, maximum member dominance, and residual evidence after
  removing the largest member.
- Reused Direction 1 broad-broker output to exclude `9268 / 凱基-台北` from
  cluster membership. This dependency is visible in the summary output.
- Adjacent cluster triggers merge only when they are close in stock-report
  sessions and at least 50% of the smaller member set overlaps. This prevents
  unrelated rotating branch groups from becoming one long episode.

### Direction 2 Validation Failures and Fixes

#### Rotating member sets

The first six-symbol sample merged different branch groups whenever their dates
were close. One 9136 episode accumulated dozens of unrelated member names. This
was rejected because a long sequence of changing groups is not evidence of one
distributed execution cluster. The 50% adjacent member-overlap rule was added,
and a regression test now requires disjoint member sets to form separate
episodes.

#### Empty member table

The first full-market run stopped before 250 symbols with:

```text
KeyError: 'member_qualifies'
```

Cause: stocks without any eligible local branches returned a columnless empty
member table. `build_cluster_daily` now returns an empty cluster table before
column access. A regression test covers this path.

#### Institutional and non-local names

The first completed full scan exposed `國票-敦北法人`, `第一金-國際證券`, and
`犇亞-網路` in review candidates. This violated the local-branch hypothesis.
The shared Direction 1 branch classifier was extended with `法人`, `國際證券`,
and `網路`, then both Direction 1 and Direction 2 were rerun. Final review
outputs contain zero names matching those terms.

### Direction 2 Sample Run

```bash
PYTHONPATH=src .venv/bin/python -m stockanalysis.analysis.distributed_branch_accumulation \
  --symbols 1532,2107,2705,1455,1460,9136 \
  --end-date 2026-06-17 \
  --output-dir outputs/analysis/distributed_branch_accumulation_sample
```

The final sample produced three review-eligible ongoing groups. The 1532 group
was separate from the long single-branch event and met the distributed controls.

### Direction 2 Final Full-Market Run

```bash
PYTHONPATH=src .venv/bin/python -m stockanalysis.analysis.distributed_branch_accumulation \
  --end-date 2026-06-17
```

Final local-data results:

- scanned stock symbols: `1,981`
- historical episodes retained for audit: `4,681`
- ongoing episodes: `306`
- review-eligible ongoing episodes: `149`
- daily qualifying cluster rows: `17,490`
- review confidence counts: `4 established`, `28 confirmed`, `117 developing`
- unique review symbols: `142`
- excluded broad brokers: `9268 / 凱基-台北`
- excluded `2317` and `2330` rows: `0`
- review member names containing `法人`, `國際證券`, or `網路`: `0`

Latest 1532 distributed episode:

- rank: `111`
- episode: `2026-06-15` through `2026-06-17`
- confidence: `developing`
- max branches: `5`
- max parent brokers: `5`
- latest cluster net buy: `1,781,113` shares
- latest cluster buy share: `32.21%`
- latest largest-member net share: `51.93%`
- latest residual net buy after largest member: `856,213` shares

This is evidence of a recent multi-branch accumulation pattern. It is not proof
that the branches represent one beneficial owner.

Outputs:

- `outputs/analysis/distributed_branch_accumulation/distributed_branch_report.md`
- `outputs/analysis/distributed_branch_accumulation/distributed_branch_summary.json`
- `outputs/analysis/distributed_branch_accumulation/distributed_branch_episodes.csv`
- `outputs/analysis/distributed_branch_accumulation/distributed_branch_episodes.parquet`
- `outputs/analysis/distributed_branch_accumulation/distributed_branch_daily_triggers.parquet`

### Direction 2 Known Limitations

- Member overlap preserves a changing cluster core but does not identify account
  ownership or order routing relationships.
- Parent brokerage diversity reduces one-firm campaign noise but does not prove
  independent information sources.
- Member lists in long episodes are unions across qualifying dates and may be
  larger than the latest active set. Daily trigger parquet is the authoritative
  source for exact date membership.
- Absolute member and cluster net-buy gates still introduce liquidity scale
  effects.
- The 149-row review list is a behavior-review queue, not a trade list.

### Direction 2 Verification

```bash
.venv/bin/python -m py_compile \
  src/stockanalysis/analysis/distributed_branch_accumulation.py \
  tests/test_distributed_branch_accumulation.py
.venv/bin/python -m unittest \
  tests.test_single_branch_persistent_accumulation \
  tests.test_distributed_branch_accumulation
```

Result: nine tests passed across the two completed directions. Direction 2
coverage includes valid independent clusters, dominant-member rejection,
single-parent rejection, one-day burst rejection, member-set episode splitting,
and empty-member handling.

### Next Resume Point

Directions 1 and 2 are complete. Direction 3 is regional branch clustering near
company locations. Before implementation, locate or build auditable company and
broker address data. Do not infer geography from branch names alone when an
address is available.

## Direction 3: Regional Branch Clustering

### Status

`2026-06-21`: completed as a same-registered-city detector using official company
address snapshots and broker-list addresses.

### Fixed Interpretation

The detector answers whether independently abnormal branches in the company's
registered city are accumulating together more strongly than that city's own
historical participation in the stock. It does not claim physical proximity,
employee trading, local information, insider behavior, or common ownership.

Registered address is not necessarily headquarters, factory, or principal
operating location. Exact 10/25/50 km detection remains a later extension that
requires geocoded operating-location data.

### Address Sources

- TWSE official OpenAPI:
  `https://openapi.twse.com.tw/v1/opendata/t187ap03_L`
- TPEx official OpenAPI:
  `https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O`
- Local broker addresses: `data/broker_list.csv`

Saved snapshots:

- `data/reference/company_profiles_twse.json`
- `data/reference/company_profiles_tpex.json`

Refresh command:

```bash
.venv/bin/python apps/etl/refresh_company_profiles.py
```

The TWSE snapshot had 1,090 rows and Chinese address fields. The TPEx snapshot
had 890 rows and English address fields. City normalization handles `台/臺`, old
county names, and English city/county names.

### Implementation

- Added `src/stockanalysis/analysis/regional_branch_accumulation.py`.
- Added tests in `tests/test_regional_branch_accumulation.py`.
- Reused Direction 2 member-level anomaly, parent-broker, dominance, residual,
  broad-broker, and episode-overlap controls.
- Restricted members using broker address city, not branch-name text.
- Required at least two branches from two parent brokerages.
- Added city buy-share lift: the company's city must have at least 1.5 times its
  trailing 60-session stock-specific baseline and at least 3 percentage points
  of absolute share increase.

### 2474 Calibration Finding

Official registered address: `台南市永康區仁愛街398號`.

The local data contains 61 identifiable Tainan branches trading 2474. From
October through November 2025, `富邦-台南` showed a strong single-branch event:

- five-session net buying rose above 4 million shares at its peak;
- net-buy purity was near 100%;
- branch buy share exceeded 30% at its peak.

However, only `永豐金-永康` briefly joined on `2025-10-20`. The two-branch group
had 4.38% stock buy share, below the 5% regional gate, while `富邦-台南` supplied
74.23% of group net buying. The event therefore remains a single local-branch
case and does not pass the multi-branch regional detector. Thresholds were not
relaxed to force the expected example to pass.

### Rejected First Full-Market Result

The first same-city scan produced 46 review candidates, including 32 registered
in Taipei City. This was rejected because same-city matching alone inherited the
large Taipei broker population and did not demonstrate a location-specific
increase.

The city-specific historical baseline was then added. A regression test verifies
that current city participation must rise above its own prior share.

### Final Full-Market Run

```bash
PYTHONPATH=src .venv/bin/python -m stockanalysis.analysis.regional_branch_accumulation \
  --end-date 2026-06-17
```

Final local-data results:

- historical episodes retained for audit: `576`
- ongoing episodes: `37`
- review-eligible ongoing episodes: `9`
- review confidence: `1 confirmed`, `8 developing`
- city distribution: `台中市 3`, `台北市 3`, `台南市 1`, `高雄市 1`, `新竹市 1`

Review queue:

1. `7721 微程式 / 台中市 / confirmed`
2. `4166 友霖 / 台北市 / developing`
3. `6598 ABC-KY / 台北市 / developing`
4. `8201 無敵 / 台北市 / developing`
5. `2017 官田鋼 / 台南市 / developing`
6. `8933 愛地雅 / 台中市 / developing`
7. `1432 大魯閣 / 台中市 / developing`
8. `1436 華友聯 / 高雄市 / developing`
9. `2480 敦陽科 / 新竹市 / developing`

Outputs:

- `outputs/analysis/regional_branch_accumulation/regional_branch_report.md`
- `outputs/analysis/regional_branch_accumulation/regional_branch_summary.json`
- `outputs/analysis/regional_branch_accumulation/regional_branch_episodes.parquet`
- `outputs/analysis/regional_branch_accumulation/regional_branch_daily_triggers.parquet`

### Plain-Language Overview

Added `apps/analysis/build_key_broker_anomaly_overview.py`, which combines the
three completed detectors into:

- `outputs/analysis/key_broker_anomaly_overview.md`

It explains detector meanings, confidence labels, 1532, 2474, current counts,
top candidates, and conclusions that cannot be drawn from the data.

### Direction 3 Verification

```bash
.venv/bin/python -m py_compile \
  src/stockanalysis/analysis/regional_branch_accumulation.py \
  apps/analysis/build_key_broker_anomaly_overview.py
.venv/bin/python -m unittest \
  tests.test_single_branch_persistent_accumulation \
  tests.test_distributed_branch_accumulation \
  tests.test_regional_branch_accumulation
```

Tests cover city parsing, old county normalization, English TPEx addresses,
company-city member filtering, missing-city handling, and city-share baseline
lift. Existing Direction 1 and 2 tests remain part of the verification set.

### Next Resume Point

Directions 1 through 3 are complete. Direction 4 is stock and warrant joint
accumulation. Before implementation, define warrant-side exclusions for issuers,
market makers, expiry effects, call/put direction, and delta-equivalent exposure.
Do not add warrant volume directly to stock shares.

## Cross-Cutting General Broker-Flow Anomaly Framework

### Status

`2026-06-22`: implemented, tested, scanned across the full local stock universe,
and validated against raw broker parquet.

### Why This Framework Was Added

The first three completed directions are useful but hypothesis-specific. They
encode assumptions about one persistent local branch, several synchronized
local branches, or branches in the company's registered city. A general detector
must not require any of those structures.

The new framework therefore:

- accepts mapped and unmapped execution branches without a broker allowlist;
- does not require a local branch suffix or company-location match;
- does not exclude mega-cap symbols by name;
- allows concentrated, distributed, mixed, and breadth-expansion behavior;
- compares each stock only with its own trailing broker-flow history;
- does not load OHLC, future prices, returns, entries, exits, or labels.

### Market-Structure Correction During Smoke Testing

The first implementation incorrectly gated on net buying summed over every
broker in one stock. That quantity is structurally near zero because broker-side
buying and selling are the two sides of the same market. A 100-symbol smoke run
correctly produced zero cases and exposed the invalid aggregation.

The implementation was corrected instead of weakening thresholds. For every
stock and five-report-session window, transactions are now aggregated by broker
first. The detector then measures only brokers whose five-session net flow is
positive:

- positive broker pressure: sum of positive broker window net flows;
- buyer retention: positive pressure divided by those brokers' buy volume;
- buy participation: positive pressure divided by all reported stock buy volume;
- persistence: maximum positive-net sessions among current positive brokers;
- concentration: top positive broker share and positive-flow HHI;
- breadth: count of positive brokers and change from the trailing baseline.

### Prospective Event Gate

For one stock-date to qualify, all conditions must hold:

- five-session positive pressure is at or above its prior 60-session 95th percentile;
- pressure is at least 1.5 times the prior rolling median;
- at least one current positive broker bought net on three of five sessions;
- positive-broker retention is at least 50%;
- positive pressure is at least 8% of total reported stock buy volume.

Historical thresholds are shifted by one report session. The current observation
cannot alter its own baseline. Adjacent triggers within five report sessions are
merged, and an episode requires at least three qualifying dates. The formal
review queue keeps only the highest-severity episode per symbol, so case counts
cannot be inflated by repeated dates or several episodes in the same stock.

### Automatic Behavior Types

- `concentrated_surge`: one positive broker supplies at least 55% of pressure.
- `distributed_surge`: top share is at most 35% and at least five positive brokers participate.
- `breadth_expansion`: HHI falls by at least 0.05 while buyer breadth rises at least 25%.
- `mixed_accumulation`: a valid anomaly that does not fit the three shapes above.

These are descriptions of observable execution shape, not claims about common
ownership, information advantage, or future returns.

### Full-Market Run

```bash
PYTHONPATH=src .venv/bin/python -m stockanalysis.analysis.general_broker_flow_anomaly \
  --end-date 2026-06-19
```

Results:

- scanned stock symbols: `1,983`
- distinct episodes: `3,812`
- unique symbols with an episode: `1,607`
- one-per-symbol review cases: `1,607`
- daily qualifying rows: `23,982`
- dominant episode types: `2,831 distributed`, `437 breadth expansion`,
  `319 mixed`, and `225 concentrated`

The requested minimum of 50 source-backed cases is satisfied by the first 50
rows of the one-per-symbol review output. They represent 50 different stocks,
not 50 daily observations.

Outputs:

- `outputs/analysis/general_broker_flow_anomaly/general_broker_flow_anomaly_report.md`
- `outputs/analysis/general_broker_flow_anomaly/general_broker_flow_anomaly_summary.json`
- `outputs/analysis/general_broker_flow_anomaly/general_broker_flow_anomaly_review_cases.csv`
- `outputs/analysis/general_broker_flow_anomaly/general_broker_flow_anomaly_review_cases.parquet`
- `outputs/analysis/general_broker_flow_anomaly/general_broker_flow_anomaly_episodes.parquet`
- `outputs/analysis/general_broker_flow_anomaly/general_broker_flow_anomaly_daily_triggers.parquet`

### Verification

- two deterministic unit tests pass: shifted historical baseline plus sustained
  surge detection, and adjacent-trigger episode deduplication;
- all `23,982` saved triggers pass every configured gate;
- all `1,607` review rows have unique symbols and at least three qualifying dates;
- top 10 review cases were rebuilt directly from raw parquet; positive pressure,
  buyer retention, and participation match saved trigger rows exactly;
- no price or return column exists in detector inputs or ranking outputs.

### Known Limitations

- Broker codes identify execution channels, not beneficial owners.
- A 95th-percentile self-history gate intentionally finds anomalies in both
  liquid and illiquid stocks; review rank is not a liquidity or tradability rank.
- Unmapped broker codes remain visible instead of being silently excluded.
- The framework detects broad positive-flow regimes and may surface normal
  institutional rebalancing. Event interpretation still requires manual review.
- The 1,607-case review queue is intentionally broad. The top 50 are evidence
  that the framework finds real source-backed cases, not a claim that all 1,607
  cases are equally important.
