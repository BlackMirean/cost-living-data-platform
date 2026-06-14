"""Apply Elasticsearch ILM, rollover template and processed-post aliases."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from backend.common.config import settings  # noqa: E402
from backend.common.es_client import get_es_client, load_posts_mapping  # noqa: E402

POLICY_PATH = REPO_ROOT / "database" / "ilm" / "processed_posts_policy.json"
POLICY_NAME = "cost_living_processed_posts_policy"
TEMPLATE_NAME = "cost_living_processed_posts_template"
BACKING_INDEX = "cost_living_processed_posts-000001"
SOURCE_INDEX = "cost_living_processed_posts"


def load_policy() -> dict[str, Any]:
    with POLICY_PATH.open("r", encoding="utf-8") as policy_file:
        return json.load(policy_file)


def processed_template(write_alias: str) -> dict[str, Any]:
    mapping = load_posts_mapping()
    settings_block = {
        **mapping.get("settings", {}),
        "index.lifecycle.name": POLICY_NAME,
        "index.lifecycle.rollover_alias": write_alias,
    }
    return {
        "index_patterns": ["cost_living_processed_posts-*"],
        "priority": 500,
        "template": {
            "settings": settings_block,
            "mappings": mapping.get("mappings", {}),
        },
        "_meta": {
            "description": "Processed cost-of-living posts backing indices with ILM rollover.",
            "write_alias": write_alias,
            "read_alias": settings.posts_current_alias,
        },
    }


def exists_alias(client: Any, alias: str) -> bool:
    try:
        return bool(client.indices.exists_alias(name=alias))
    except Exception:
        return False


def exists_index(client: Any, index: str) -> bool:
    try:
        return bool(client.indices.exists(index=index))
    except Exception:
        return False


def alias_indices(client: Any, alias: str) -> list[str]:
    try:
        return list(client.indices.get_alias(name=alias).keys())
    except Exception:
        return []


def create_backing_index(client: Any, *, write_alias: str, read_alias: str, dry_run: bool) -> bool:
    if exists_index(client, BACKING_INDEX):
        return False
    body = {
        "aliases": {
            write_alias: {"is_write_index": True},
            read_alias: {},
        }
    }
    if not dry_run:
        client.indices.create(index=BACKING_INDEX, body=body)
    return True


def ensure_aliases(client: Any, *, write_alias: str, read_alias: str, dry_run: bool) -> None:
    actions = []
    for alias in (write_alias, read_alias):
        for index in alias_indices(client, alias):
            actions.append({"remove": {"index": index, "alias": alias}})
    actions.extend([
        {"add": {"index": BACKING_INDEX, "alias": write_alias, "is_write_index": True}},
        {"add": {"index": BACKING_INDEX, "alias": read_alias}},
    ])
    if actions and not dry_run:
        client.indices.update_aliases(body={"actions": actions})


def migrate_current_index(client: Any, *, write_alias: str, read_alias: str, dry_run: bool) -> dict[str, Any]:
    result: dict[str, Any] = {
        "source_index": SOURCE_INDEX,
        "backing_index": BACKING_INDEX,
        "write_alias": write_alias,
        "read_alias": read_alias,
        "migrated_source_index": False,
        "created_backing_index": False,
    }
    source_exists = exists_index(client, SOURCE_INDEX)
    write_alias_exists = exists_alias(client, write_alias)
    if write_alias_exists:
        ensure_aliases(client, write_alias=write_alias, read_alias=read_alias, dry_run=dry_run)
        return result

    if source_exists:
        if not exists_index(client, BACKING_INDEX):
            if not dry_run:
                client.indices.create(index=BACKING_INDEX)
            result["created_backing_index"] = True
        if not dry_run:
            client.reindex(
                source={"index": SOURCE_INDEX},
                dest={"index": BACKING_INDEX},
                refresh=True,
                conflicts="proceed",
                wait_for_completion=True,
                request_timeout=600,
            )
            client.indices.delete(index=SOURCE_INDEX)
        result["migrated_source_index"] = True
        ensure_aliases(client, write_alias=write_alias, read_alias=read_alias, dry_run=dry_run)
        return result

    result["created_backing_index"] = create_backing_index(
        client,
        write_alias=write_alias,
        read_alias=read_alias,
        dry_run=dry_run,
    )
    ensure_aliases(client, write_alias=write_alias, read_alias=read_alias, dry_run=dry_run)
    return result


def apply_lifecycle(*, dry_run: bool = False) -> dict[str, Any]:
    client = get_es_client()
    write_alias = settings.processed_posts_write_index
    read_alias = settings.posts_current_alias
    policy = load_policy()
    template = processed_template(write_alias)
    if not dry_run:
        client.ilm.put_lifecycle(name=POLICY_NAME, policy=policy)
        client.indices.put_index_template(name=TEMPLATE_NAME, body=template)
    migration = migrate_current_index(
        client,
        write_alias=write_alias,
        read_alias=read_alias,
        dry_run=dry_run,
    )
    if not dry_run:
        try:
            client.ilm.retry(index=BACKING_INDEX)
        except Exception:
            pass
    return {
        "dry_run": dry_run,
        "policy": POLICY_NAME,
        "template": TEMPLATE_NAME,
        "write_alias": write_alias,
        "read_alias": read_alias,
        "migration": migration,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply Elasticsearch ILM and rollover aliases.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(json.dumps(apply_lifecycle(dry_run=args.dry_run), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
