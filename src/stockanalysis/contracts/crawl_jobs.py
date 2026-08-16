from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


STATUS_COLLECTION_CREATED = "collection_created"
STATUS_INITIALIZED = "initialized"
STATUS_PENDING = "pending"
STATUS_RUNNING = "running"


class PayloadValidationError(ValueError):
    """Raised when a crawl-job payload does not satisfy the public contract."""


@dataclass(frozen=True)
class CrawlJobPayload:
    symbols: tuple[str, ...]
    date: str
    run_id: str | None = None
    image_revision: str | None = None

    @property
    def compact_date(self) -> str:
        return self.date.replace("/", "")

    def to_task_body(self) -> dict[str, object]:
        body: dict[str, object] = {"symbols": list(self.symbols), "date": self.date}
        if self.run_id:
            body["run_id"] = self.run_id
        if self.image_revision:
            body["image_revision"] = self.image_revision
        return body

    def to_job_args(self) -> list[str]:
        return [str(list(self.symbols)), self.compact_date]


def normalize_date(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PayloadValidationError("date is required")

    raw = value.strip()
    date_format = "%Y/%m/%d" if "/" in raw else "%Y%m%d"
    try:
        parsed = datetime.strptime(raw, date_format)
    except ValueError as exc:
        raise PayloadValidationError("date must be YYYY/MM/DD or YYYYMMDD") from exc
    return parsed.strftime("%Y/%m/%d")


def normalize_batch_size(value: object, *, default: int, maximum: int) -> int:
    if isinstance(value, bool):
        raise PayloadValidationError("batch_size must be an integer")
    if value is None:
        value = default
    try:
        batch_size = int(value)
    except (TypeError, ValueError) as exc:
        raise PayloadValidationError("batch_size must be an integer") from exc
    if batch_size < 1 or batch_size > maximum:
        raise PayloadValidationError(f"batch_size must be between 1 and {maximum}")
    return batch_size


def parse_crawl_job_payload(
    data: object,
    *,
    max_symbols: int,
) -> CrawlJobPayload:
    if not isinstance(data, Mapping):
        raise PayloadValidationError("JSON body must be an object")

    raw_symbols = data.get("symbols")
    if not isinstance(raw_symbols, list) or not raw_symbols:
        raise PayloadValidationError("symbols must be a non-empty list")
    if len(raw_symbols) > max_symbols:
        raise PayloadValidationError(f"symbols exceeds maximum batch size {max_symbols}")

    symbols: list[str] = []
    for raw_symbol in raw_symbols:
        if not isinstance(raw_symbol, str) or not raw_symbol.strip():
            raise PayloadValidationError("every symbol must be a non-empty string")
        symbols.append(raw_symbol.strip())

    return CrawlJobPayload(
        symbols=tuple(symbols),
        date=normalize_date(data.get("date")),
        run_id=_optional_string(data, "run_id"),
        image_revision=_optional_string(data, "image_revision"),
    )


def _optional_string(data: Mapping[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise PayloadValidationError(f"{key} must be a non-empty string when provided")
    return value.strip()
