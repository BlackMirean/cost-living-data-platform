"""Fail when live cloud resources no longer match repository manifests."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_yaml_documents(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return [doc for doc in yaml.safe_load_all(handle) if isinstance(doc, dict)]


def load_yaml(path: Path) -> dict[str, Any]:
    docs = load_yaml_documents(path)
    if len(docs) != 1:
        raise ValueError(f"expected one YAML document in {path}, got {len(docs)}")
    return docs[0]


def run_json(args: list[str]) -> dict[str, Any]:
    result = subprocess.run(args, cwd=REPO_ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def first_container(resource: dict[str, Any]) -> dict[str, Any]:
    return resource["spec"]["template"]["spec"]["containers"][0]


def compact_container(container: dict[str, Any]) -> dict[str, Any]:
    return {
        "image": container.get("image"),
        "command": container.get("command", []),
        "args": container.get("args", []),
        "envFrom": container.get("envFrom", []),
    }


def manifest_by_kind_name(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    items: dict[tuple[str, str], dict[str, Any]] = {}
    for doc in load_yaml_documents(path):
        items[(doc.get("kind", ""), doc.get("metadata", {}).get("name", ""))] = doc
    return items


def check_deployment(name: str, namespace: str, manifest_path: Path, failures: list[str]) -> None:
    expected = load_yaml(manifest_path)
    live = run_json(["kubectl", "-n", namespace, "get", "deployment", name, "-o", "json"])
    expected_container = compact_container(first_container(expected))
    live_container = compact_container(first_container(live))
    if live_container != expected_container:
        failures.append(f"deployment/{name} container does not match {manifest_path.relative_to(REPO_ROOT)}")


def check_configmap(namespace: str, manifest_path: Path, failures: list[str]) -> None:
    expected = load_yaml(manifest_path)
    live = run_json(["kubectl", "-n", namespace, "get", "configmap", expected["metadata"]["name"], "-o", "json"])
    expected_data = expected.get("data", {})
    live_data = live.get("data", {})
    for key, value in expected_data.items():
        if key == "FISSION_PACKAGE_URL":
            if not live_data.get(key):
                failures.append("configmap/cost-living-platform-api-config has empty FISSION_PACKAGE_URL")
            continue
        if live_data.get(key) != value:
            failures.append(f"configmap/cost-living-platform-api-config key {key} differs")


def check_fission_functions(namespace: str, manifest_path: Path, failures: list[str]) -> None:
    expected_docs = load_yaml_documents(manifest_path)
    live = run_json(["kubectl", "-n", namespace, "get", "function", "-o", "json"])
    expected_names = sorted(doc["metadata"]["name"] for doc in expected_docs)
    live_names = sorted(item["metadata"]["name"] for item in live.get("items", []))
    if live_names != expected_names:
        failures.append(f"fission functions differ: expected {expected_names}, live {live_names}")
        return
    for doc in expected_docs:
        live_item = next(item for item in live["items"] if item["metadata"]["name"] == doc["metadata"]["name"])
        expected_spec = doc.get("spec", {})
        live_spec = live_item.get("spec", {})
        keys = ["functionTimeout", "idletimeout", "requestsPerPod"]
        for key in keys:
            if live_spec.get(key) != expected_spec.get(key):
                failures.append(f"function/{doc['metadata']['name']} field {key} differs")
        expected_entry = expected_spec.get("package", {}).get("functionName")
        live_entry = live_spec.get("package", {}).get("functionName")
        if live_entry != expected_entry:
            failures.append(f"function/{doc['metadata']['name']} entrypoint differs")


def check_fission_timers(namespace: str, manifest_path: Path, failures: list[str]) -> None:
    expected_docs = load_yaml_documents(manifest_path)
    live = run_json(["kubectl", "-n", namespace, "get", "timetrigger", "-o", "json"])
    expected = {
        doc["metadata"]["name"]: (doc["spec"]["cron"], doc["spec"]["functionref"]["name"])
        for doc in expected_docs
    }
    live_map = {
        item["metadata"]["name"]: (item["spec"]["cron"], item["spec"]["functionref"]["name"])
        for item in live.get("items", [])
    }
    if live_map != expected:
        failures.append(f"fission timers differ: expected {expected}, live {live_map}")


def check_no_fission_http_routes(namespace: str, failures: list[str]) -> None:
    live = run_json(["kubectl", "-n", namespace, "get", "httptrigger", "-o", "json"])
    names = [item["metadata"]["name"] for item in live.get("items", [])]
    if names:
        failures.append(f"unexpected Fission HTTP routes exist: {names}")


def check_redis_pvc(failures: list[str]) -> None:
    docs = manifest_by_kind_name(REPO_ROOT / "deployment" / "redis" / "redis.yaml")
    if ("PersistentVolumeClaim", "redis-data") not in docs:
        failures.append("deployment/redis/redis.yaml does not define redis-data PVC")
        return
    live = run_json(["kubectl", "-n", "redis", "get", "pvc", "redis-data", "-o", "json"])
    if live.get("status", {}).get("phase") != "Bound":
        failures.append("pvc/redis-data is not Bound")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check live cloud resources against repository manifests.")
    parser.add_argument("--namespace", default="cost-living")
    parser.add_argument("--fission-namespace", default="default")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    failures: list[str] = []
    check_deployment(
        "cost-living-platform-api",
        args.namespace,
        REPO_ROOT / "deployment" / "kubernetes" / "api-deployment.yaml",
        failures,
    )
    check_deployment(
        "cost-living-platform-nlp-worker",
        args.namespace,
        REPO_ROOT / "deployment" / "kubernetes" / "nlp-worker-deployment.yaml",
        failures,
    )
    check_configmap(args.namespace, REPO_ROOT / "deployment" / "kubernetes" / "configmap.yaml", failures)
    check_fission_functions(
        args.fission_namespace,
        REPO_ROOT / "deployment" / "fission" / "platform-pipeline-functions.yaml",
        failures,
    )
    check_fission_timers(
        args.fission_namespace,
        REPO_ROOT / "deployment" / "fission" / "platform-pipeline-timers.yaml",
        failures,
    )
    check_no_fission_http_routes(args.fission_namespace, failures)
    check_redis_pvc(failures)

    result = {"ok": not failures, "failures": failures}
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    elif failures:
        print("Cloud drift detected:")
        for failure in failures:
            print(f"- {failure}")
    else:
        print("Cloud drift check passed.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
