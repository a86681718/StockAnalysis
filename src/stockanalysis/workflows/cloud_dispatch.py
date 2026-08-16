from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Any, Callable

from stockanalysis.contracts.crawl_jobs import (
    CrawlJobPayload,
    STATUS_COLLECTION_CREATED,
    STATUS_PENDING,
    STATUS_RUNNING,
    normalize_batch_size,
    normalize_date,
    parse_crawl_job_payload,
)


@dataclass(frozen=True)
class TriggerConfig:
    market: str
    project_id: str
    location: str
    job_name: str
    max_batch_size: int
    warn_for_missing_symbol: bool = False


@dataclass(frozen=True)
class TriggerResult:
    symbols: tuple[str, ...]
    collection_name: str
    operation_id: str | None


@dataclass(frozen=True)
class PrepareConfig:
    market: str
    project_id: str
    location: str
    queue_name: str
    function_url: str
    default_batch_size: int
    maximum_batch_size: int
    initialization_status: str
    exclude_dummy_document: bool = False


@dataclass(frozen=True)
class PrepareResult:
    date: str
    batch_size: int
    total_symbols: int
    batches_created: int


def trigger_crawl_job(
    data: object,
    *,
    config: TriggerConfig,
    firestore_client: Any,
    run_client: Any,
    run_job_request_type: Any,
) -> TriggerResult:
    payload = parse_crawl_job_payload(data, max_symbols=config.max_batch_size)
    collection_name = f"{config.market}_crawl_status_{payload.compact_date}"
    runnable_symbols: list[str] = []

    for symbol in payload.symbols:
        doc_ref = firestore_client.collection(collection_name).document(symbol)
        if not doc_ref.get().exists:
            log = logging.warning if config.warn_for_missing_symbol else logging.debug
            log(
                "Symbol %s not found in Firestore collection %s; skipping before job trigger.",
                symbol,
                collection_name,
            )
            continue
        doc_ref.update({"status": STATUS_RUNNING})
        logging.info("[%s] Updated Firestore status to 'running'", symbol)
        runnable_symbols.append(symbol)

    if not runnable_symbols:
        return TriggerResult((), collection_name, None)

    job_path = (
        f"projects/{config.project_id}/locations/{config.location}/jobs/{config.job_name}"
    )
    args = CrawlJobPayload(
        symbols=tuple(runnable_symbols),
        date=payload.date,
        run_id=payload.run_id,
        image_revision=payload.image_revision,
    ).to_job_args()
    run_request = run_job_request_type(
        name=job_path,
        overrides=run_job_request_type.Overrides(
            container_overrides=[
                run_job_request_type.Overrides.ContainerOverride(args=args)
            ]
        ),
    )
    logging.info("[%s] Start to run job", runnable_symbols)
    operation = run_client.run_job(request=run_request)
    operation_id = operation.operation.name
    logging.info(
        "[%s] Job triggered successfully. Operation: %s",
        runnable_symbols,
        operation_id,
    )
    return TriggerResult(tuple(runnable_symbols), collection_name, operation_id)


def prepare_crawl_tasks(
    data: dict[str, object],
    *,
    config: PrepareConfig,
    firestore_client: Any,
    tasks_client: Any,
    fetch_symbols: Callable[[str], list[str]],
    server_timestamp: Any,
    http_method_post: Any,
    duration_type: Any,
    timestamp_type: Any,
) -> PrepareResult:
    data_date = (
        normalize_date(data["date"])
        if "date" in data
        else datetime.now().strftime("%Y/%m/%d")
    )
    batch_size = normalize_batch_size(
        data.get("batch_size"),
        default=config.default_batch_size,
        maximum=config.maximum_batch_size,
    )
    collection_name = f"{config.market}_crawl_status_{data_date.replace('/', '')}"
    collection_ref = firestore_client.collection(collection_name)
    symbols = [document.id for document in collection_ref.stream()]
    if config.exclude_dummy_document:
        symbols = [symbol for symbol in symbols if symbol != "dummy"]

    if not symbols:
        symbols = fetch_symbols(data_date)
        _ensure_collection(firestore_client, collection_name)
        for symbol in symbols:
            firestore_client.collection(collection_name).document(symbol).set(
                {
                    "status": config.initialization_status,
                    "updatedAt": server_timestamp,
                }
            )

    batches_created = 0
    for start in range(0, len(symbols), batch_size):
        batch = symbols[start : start + batch_size]
        for symbol in batch:
            firestore_client.collection(collection_name).document(symbol).set(
                {"status": STATUS_PENDING, "updatedAt": server_timestamp}
            )
        create_crawl_task(
            batch,
            data_date,
            run_id=data.get("run_id"),
            image_revision=data.get("image_revision"),
            config=config,
            tasks_client=tasks_client,
            http_method_post=http_method_post,
            duration_type=duration_type,
            timestamp_type=timestamp_type,
        )
        batches_created += 1

    return PrepareResult(data_date, batch_size, len(symbols), batches_created)


def _ensure_collection(firestore_client: Any, collection_name: str) -> None:
    dummy = firestore_client.collection(collection_name).document("dummy")
    try:
        if not dummy.get().exists:
            dummy.set({"status": STATUS_COLLECTION_CREATED})
    except Exception as exc:
        logging.info("Error checking collection %s: %s", collection_name, exc)


def create_crawl_task(
    symbols: list[str],
    data_date: str,
    *,
    run_id: object,
    image_revision: object,
    config: PrepareConfig,
    tasks_client: Any,
    http_method_post: Any,
    duration_type: Any,
    timestamp_type: Any,
) -> None:
    payload = parse_crawl_job_payload(
        {
            "symbols": symbols,
            "date": data_date,
            "run_id": run_id,
            "image_revision": image_revision,
        },
        max_symbols=len(symbols),
    ).to_task_body()
    task = {
        "http_request": {
            "http_method": http_method_post,
            "url": config.function_url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode(),
            "oidc_token": {
                "service_account_email": (
                    f"cloud-run@{config.project_id}.iam.gserviceaccount.com"
                )
            },
        },
        "dispatch_deadline": duration_type(seconds=1800),
    }
    schedule_time = timestamp_type()
    schedule_time.FromDatetime(datetime.now(timezone.utc) + timedelta(seconds=5))
    task["schedule_time"] = schedule_time
    parent = tasks_client.queue_path(
        config.project_id,
        config.location,
        config.queue_name,
    )
    response = tasks_client.create_task(parent=parent, task=task)
    logging.info("Created task for %s: %s", symbols, response.name)
