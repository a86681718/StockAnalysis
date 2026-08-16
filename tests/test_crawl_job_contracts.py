from __future__ import annotations

import unittest

from stockanalysis.contracts.crawl_jobs import (
    CrawlJobPayload,
    PayloadValidationError,
    STATUS_COLLECTION_CREATED,
    STATUS_INITIALIZED,
    STATUS_PENDING,
    STATUS_RUNNING,
    normalize_batch_size,
    normalize_date,
    parse_crawl_job_payload,
)


class CrawlJobContractTests(unittest.TestCase):
    def test_accepts_both_legacy_date_forms_and_preserves_symbol_order(self):
        slash = parse_crawl_job_payload(
            {"symbols": ["2330", "2317"], "date": "2026/08/15"},
            max_symbols=2,
        )
        compact = parse_crawl_job_payload(
            {"symbols": ["6488", "00679B"], "date": "20260815"},
            max_symbols=2,
        )

        self.assertEqual(slash.symbols, ("2330", "2317"))
        self.assertEqual(compact.symbols, ("6488", "00679B"))
        self.assertEqual(compact.date, "2026/08/15")
        self.assertEqual(compact.to_job_args(), ["['6488', '00679B']", "20260815"])

    def test_optional_metadata_round_trips_to_task_body(self):
        payload = parse_crawl_job_payload(
            {
                "symbols": ["2330"],
                "date": "20260815",
                "run_id": "run-123",
                "image_revision": "sha256:abc",
            },
            max_symbols=1,
        )

        self.assertEqual(
            payload.to_task_body(),
            {
                "symbols": ["2330"],
                "date": "2026/08/15",
                "run_id": "run-123",
                "image_revision": "sha256:abc",
            },
        )

    def test_rejects_missing_empty_invalid_and_oversized_symbols(self):
        invalid_payloads = (
            {},
            {"symbols": [], "date": "20260815"},
            {"symbols": [""], "date": "20260815"},
            {"symbols": [2330], "date": "20260815"},
            {"symbols": ["2330", "2317"], "date": "20260815"},
        )

        for payload in invalid_payloads:
            with self.subTest(payload=payload), self.assertRaises(PayloadValidationError):
                parse_crawl_job_payload(payload, max_symbols=1)

    def test_rejects_missing_or_invalid_date(self):
        for value in (None, "", "2026-08-15", "20260230"):
            with self.subTest(value=value), self.assertRaises(PayloadValidationError):
                normalize_date(value)

    def test_batch_size_is_bounded_and_bool_is_not_an_integer(self):
        self.assertEqual(normalize_batch_size(None, default=5, maximum=10), 5)
        self.assertEqual(normalize_batch_size("10", default=5, maximum=10), 10)
        for value in (True, 0, 11, "bad"):
            with self.subTest(value=value), self.assertRaises(PayloadValidationError):
                normalize_batch_size(value, default=5, maximum=10)

    def test_status_vocabulary_remains_compatible(self):
        self.assertEqual(
            (STATUS_COLLECTION_CREATED, STATUS_INITIALIZED, STATUS_PENDING, STATUS_RUNNING),
            ("collection_created", "initialized", "pending", "running"),
        )

    def test_dataclass_can_serialize_legacy_task_body_without_metadata(self):
        payload = CrawlJobPayload(symbols=("2330",), date="2026/08/15")

        self.assertEqual(payload.to_task_body(), {"symbols": ["2330"], "date": "2026/08/15"})


if __name__ == "__main__":
    unittest.main()
