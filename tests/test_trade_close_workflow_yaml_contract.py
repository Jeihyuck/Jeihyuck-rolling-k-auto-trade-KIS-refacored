from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "trade-close.yml"


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key: {key}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def test_trade_close_workflow_has_no_duplicate_yaml_keys():
    with WORKFLOW.open(encoding="utf-8") as handle:
        doc = yaml.load(handle, Loader=_UniqueKeyLoader)
    assert isinstance(doc, dict)
    assert "jobs" in doc
    assert "trade_close" in doc["jobs"]


def test_trade_close_exit_router_env_is_declared_once():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert text.count("PB1_EXIT_ROUTER_ENABLED:") == 1
