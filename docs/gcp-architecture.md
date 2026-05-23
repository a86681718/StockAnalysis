# GCP Architecture

This diagram summarizes the current production flow deployed on GCP.

```mermaid
flowchart TD
    scheduler[Cloud Scheduler]

    scheduler --> prepareTwse[prepare-twse-list<br/>Cloud Run service]
    scheduler --> prepareTpex[prepare-tpex-list<br/>Cloud Run service]

    prepareTwse --> twseExchange[TWSE APIs<br/>stock list / warrant list]
    prepareTpex --> tpexExchange[TPEX APIs<br/>stock list / warrant list]

    prepareTwse --> fsTwse[Firestore<br/>twse_crawl_status_yyyymmdd]
    prepareTpex --> fsTpex[Firestore<br/>tpex_crawl_status_yyyymmdd]

    prepareTwse --> twseQueue[Cloud Tasks<br/>twse-crawl-queue]
    prepareTpex --> tpexQueue[Cloud Tasks<br/>tpex-crawl-queue]

    twseQueue --> triggerTwse[trigger-twse-job<br/>Cloud Run service]
    tpexQueue --> triggerTpex[trigger-tpex-job<br/>Cloud Run service]

    triggerTwse --> fsTwse
    triggerTpex --> fsTpex

    triggerTwse --> twseJob[twse-crawler<br/>Cloud Run Job]
    triggerTpex --> tpexJob[tpex-crawler<br/>Cloud Run Job]

    twseJob --> twseSource[TWSE broker pages<br/>bsMenu / bsContent]
    tpexJob --> tpexSource[TPEX broker API / page]

    twseJob --> fsTwse
    tpexJob --> fsTpex

    twseJob --> gcs[(GCS bucket<br/>bs_report/twse/yyyymmdd/*.csv)]
    tpexJob --> gcs2[(GCS bucket<br/>bs_report/tpex/yyyymmdd/*.csv)]

    gcs --> localSync[Local rsync]
    gcs2 --> localSync

    localSync --> etl[Local ETL]
    etl --> parquet[Local parquet outputs]
    parquet --> analysis[Local analysis / dashboard]

    classDef cloud fill:#e8f0fe,stroke:#5f87ff,color:#1f2a44;
    classDef data fill:#eaf7ea,stroke:#4b9b4b,color:#183018;
    classDef local fill:#fff4e5,stroke:#d48a1f,color:#4a3210;
    classDef ext fill:#f3e8ff,stroke:#8b5cf6,color:#342056;

    class scheduler,prepareTwse,prepareTpex,twseQueue,tpexQueue,triggerTwse,triggerTpex,twseJob,tpexJob cloud;
    class fsTwse,fsTpex,gcs,gcs2 data;
    class localSync,etl,parquet,analysis local;
    class twseExchange,tpexExchange,twseSource,tpexSource ext;
```

## Notes

- `deployment/` is the primary source of the production GCP services.
- Firestore is used as the per-day crawl status store for symbol batches.
- GCS is the handoff point between GCP crawlers and local ETL / analysis.

## Firestore Status Flow

```mermaid
stateDiagram-v2
    [*] --> initialized
    initialized --> pending: prepare-* initializes symbol docs
    pending --> running: trigger-* dispatches batch
    running --> completed: crawler uploads csv and deletes doc
    running --> pending: retry / rerun path
    pending --> skipped: symbol missing or filtered out
    completed --> [*]
    skipped --> [*]
```

## Local Data Flow

```mermaid
flowchart LR
    gcs[(GCS bucket<br/>bs_report/twse|tpex/yyyyMMdd/*.csv)]
    rsync[gcloud storage rsync]
    rawBs[Local raw bs_report folders<br/>data/bs_report/twse|tpex/yyyyMMdd]
    etl[ETL scripts<br/>apps/etl]
    brokerParquet[(Broker parquet<br/>data/bs_report/parquet_twse|parquet_tpex)]

    ohlcCsv[Local OHLC CSV<br/>data/ohlc/twse-yyyymmdd.csv<br/>data/ohlc/tpex-yyyymmdd.csv]
    brokerList[Broker list CSV<br/>data/broker_list.csv]

    analysis[Analysis scripts<br/>apps/analysis and src/stockanalysis/analysis]
    derived[(Derived datasets<br/>data/_derived/ohlc.parquet<br/>data/_derived/scored.parquet<br/>other derived outputs)]
    reports[Outputs<br/>HTML / PNG / CSV]
    viz[Dash visualization<br/>apps/visualization/app.py]

    gcs --> rsync
    rsync --> rawBs
    rawBs --> etl
    etl --> brokerParquet

    brokerParquet --> analysis
    ohlcCsv --> analysis
    analysis --> derived
    analysis --> reports

    brokerParquet --> viz
    derived --> viz
    ohlcCsv --> viz
    brokerList --> viz
```
