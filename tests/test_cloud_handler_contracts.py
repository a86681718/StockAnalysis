from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

import google.cloud as google_cloud_module


ROOT = Path(__file__).resolve().parents[1]


class FakeDocument:
    def __init__(self, exists: bool):
        self.exists = exists
        self.updates: list[dict[str, object]] = []

    def get(self):
        return self

    def update(self, values):
        self.updates.append(values)


class FakeCollection:
    def __init__(self, existing_symbols=()):
        self.documents = {symbol: FakeDocument(True) for symbol in existing_symbols}

    def document(self, symbol):
        return self.documents.setdefault(symbol, FakeDocument(False))


class FakeFirestore:
    def __init__(self, existing_symbols=()):
        self.existing_symbols = tuple(existing_symbols)
        self.collections: dict[str, FakeCollection] = {}

    def collection(self, name):
        return self.collections.setdefault(name, FakeCollection(self.existing_symbols))


class FakeOperation:
    def __init__(self):
        self.operation = types.SimpleNamespace(name="operations/test-operation")
        self.result_calls = 0

    def result(self):
        self.result_calls += 1
        raise AssertionError("asynchronous trigger must not wait for operation.result()")


class FakeRunClient:
    def __init__(self):
        self.requests = []
        self.operations = []

    def run_job(self, request):
        self.requests.append(request)
        operation = FakeOperation()
        self.operations.append(operation)
        return operation


class FakeTasksClient:
    def __init__(self):
        self.created = []

    def queue_path(self, project, location, queue):
        return f"projects/{project}/locations/{location}/queues/{queue}"

    def create_task(self, *, parent, task):
        self.created.append((parent, task))
        return mock.Mock(name="tasks/test-task")


class FakeRequest:
    def __init__(self, payload, *, is_json=True):
        self.payload = payload
        self.is_json = is_json
        self.headers = {}
        self.content_type = "application/json" if is_json else "text/plain"

    def get_json(self, silent=True):
        return self.payload

    def get_data(self):
        return b""


class FakeContainerOverride:
    def __init__(self, *, args):
        self.args = args


class FakeOverrides:
    ContainerOverride = FakeContainerOverride

    def __init__(self, *, container_overrides):
        self.container_overrides = container_overrides


class FakeRunJobRequest:
    Overrides = FakeOverrides

    def __init__(self, *, name, overrides):
        self.name = name
        self.overrides = overrides


def load_deployment_module(relative_path, module_name, firestore_client, run_client=None, tasks_client=None):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    firestore_module = types.ModuleType("google.cloud.firestore")
    firestore_module.Client = mock.Mock(return_value=firestore_client)
    firestore_module.SERVER_TIMESTAMP = object()
    run_module = types.ModuleType("google.cloud.run_v2")
    run_module.JobsClient = mock.Mock(return_value=run_client)
    run_types_module = types.ModuleType("google.cloud.run_v2.types")
    run_types_module.RunJobRequest = FakeRunJobRequest
    tasks_module = types.ModuleType("google.cloud.tasks_v2")
    tasks_module.CloudTasksClient = mock.Mock(return_value=tasks_client)
    tasks_module.HttpMethod = types.SimpleNamespace(POST="POST")
    functions_framework_module = types.ModuleType("functions_framework")
    functions_framework_module.http = lambda function: function
    pandas_module = types.ModuleType("pandas")
    fake_modules = {
        "functions_framework": functions_framework_module,
        "google.cloud.firestore": firestore_module,
        "google.cloud.run_v2": run_module,
        "google.cloud.run_v2.types": run_types_module,
        "google.cloud.tasks_v2": tasks_module,
        "pandas": pandas_module,
    }
    with (
        mock.patch.dict(sys.modules, fake_modules),
        mock.patch.dict(os.environ, {"GOOGLE_CLOUD_PROJECT": "test-project"}),
        mock.patch.object(google_cloud_module, "firestore", firestore_module, create=True),
        mock.patch.object(google_cloud_module, "run_v2", run_module, create=True),
        mock.patch.object(google_cloud_module, "tasks_v2", tasks_module, create=True),
        mock.patch("requests.get", return_value=mock.Mock(text="test-project")),
    ):
        spec.loader.exec_module(module)
    return module


class TriggerHandlerContractTests(unittest.TestCase):
    def load_trigger(self, market, existing_symbols=()):
        firestore = FakeFirestore(existing_symbols)
        run_client = FakeRunClient()
        module = load_deployment_module(
            f"deployment/trigger-{market}-job/main.py",
            f"phase4_trigger_{market}",
            firestore,
            run_client=run_client,
        )
        return module, firestore, run_client

    def test_missing_date_is_rejected_before_external_calls(self):
        module, _, run_client = self.load_trigger("twse", ("2330",))

        _, status = module.trigger_run_job(FakeRequest({"symbols": ["2330"]}))

        self.assertEqual(status, 400)
        self.assertEqual(run_client.requests, [])

    def test_no_runnable_symbols_is_acknowledged_without_starting_job(self):
        for market in ("twse", "tpex"):
            with self.subTest(market=market):
                module, _, run_client = self.load_trigger(market)
                body, status = module.trigger_run_job(
                    FakeRequest({"symbols": ["2330"], "date": "20260815"})
                )
                self.assertEqual(status, 200)
                self.assertIn("No runnable symbols", body)
                self.assertEqual(run_client.requests, [])

    def test_job_args_preserve_order_and_use_compact_date(self):
        module, firestore, run_client = self.load_trigger("twse", ("2330", "2317"))

        _, status = module.trigger_run_job(
            FakeRequest({"symbols": ["2330", "2317"], "date": "2026/08/15"})
        )

        self.assertEqual(status, 200)
        request = run_client.requests[0]
        self.assertEqual(list(request.overrides.container_overrides[0].args), ["['2330', '2317']", "20260815"])
        collection = firestore.collections["twse_crawl_status_20260815"]
        self.assertEqual(collection.documents["2330"].updates, [{"status": "running"}])

    def test_each_market_targets_its_configured_job(self):
        for market, symbol in (("twse", "2330"), ("tpex", "6488")):
            with self.subTest(market=market):
                module, _, run_client = self.load_trigger(market, (symbol,))

                _, status = module.trigger_run_job(
                    FakeRequest({"symbols": [symbol], "date": "20260815"})
                )

                self.assertEqual(status, 200)
                self.assertEqual(
                    run_client.requests[0].name,
                    f"projects/test-project/locations/asia-east1/jobs/{market}-crawler",
                )

    def test_tpex_acknowledges_unresolved_operation_without_waiting(self):
        module, _, run_client = self.load_trigger("tpex", ("6488",))

        body, status = module.trigger_run_job(
            FakeRequest({"symbols": ["6488"], "date": "20260815"})
        )

        self.assertEqual(status, 200)
        self.assertIn("Operation ID: operations/test-operation", body)
        self.assertEqual(run_client.operations[0].result_calls, 0)

    def test_tpex_run_client_exception_returns_500(self):
        module, _, run_client = self.load_trigger("tpex", ("6488",))
        run_client.run_job = mock.Mock(side_effect=RuntimeError("run API unavailable"))

        body, status = module.trigger_run_job(
            FakeRequest({"symbols": ["6488"], "date": "20260815"})
        )

        self.assertEqual(status, 500)
        self.assertIn("run API unavailable", body)


class PrepareTaskContractTests(unittest.TestCase):
    def test_prepare_task_emits_canonical_body_with_optional_metadata(self):
        firestore = FakeFirestore()
        tasks = FakeTasksClient()
        module = load_deployment_module(
            "deployment/prepare-twse-list/main.py",
            "phase4_prepare_twse",
            firestore,
            tasks_client=tasks,
        )

        module.create_task(["2330"], "20260815", "run-1", "image-1")

        _, task = tasks.created[0]
        body = json.loads(task["http_request"]["body"].decode())
        self.assertEqual(
            body,
            {
                "symbols": ["2330"],
                "date": "2026/08/15",
                "run_id": "run-1",
                "image_revision": "image-1",
            },
        )

    def test_each_prepare_task_preserves_queue_oidc_deadline_and_delay(self):
        for market, symbol in (("twse", "2330"), ("tpex", "6488")):
            with self.subTest(market=market):
                firestore = FakeFirestore()
                tasks = FakeTasksClient()
                module = load_deployment_module(
                    f"deployment/prepare-{market}-list/main.py",
                    f"phase7_prepare_{market}",
                    firestore,
                    tasks_client=tasks,
                )
                module.QUEUE_NAME = "crawl-queue"
                module.FUNCTION_URL = "https://trigger.example.test"

                module.create_task([symbol], "20260815")

                parent, task = tasks.created[0]
                self.assertEqual(
                    parent,
                    "projects/test-project/locations/asia-east1/queues/crawl-queue",
                )
                request = task["http_request"]
                self.assertEqual(request["url"], "https://trigger.example.test")
                self.assertEqual(
                    request["oidc_token"]["service_account_email"],
                    "cloud-run@test-project.iam.gserviceaccount.com",
                )
                self.assertEqual(task["dispatch_deadline"].seconds, 1800)
                self.assertIn("schedule_time", task)


if __name__ == "__main__":
    unittest.main()
