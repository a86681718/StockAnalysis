from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

from stockanalysis.contracts.crawl_jobs import (
    CrawlJobPayload,
    STATUS_RUNNING,
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
