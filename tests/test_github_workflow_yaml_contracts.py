from __future__ import annotations

from pathlib import Path

import pytest
import yaml


WORKFLOW_ROOT = Path(__file__).resolve().parents[1] / ".github" / "workflows"


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False):
    mapping = {}
    seen: dict[object, tuple[int, int]] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            hash(key)
        except TypeError as exc:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found unhashable key {key!r}",
                key_node.start_mark,
            ) from exc
        if key in seen:
            first_line, first_col = seen[key]
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                (
                    f"duplicate key {key!r}; first declared at "
                    f"line {first_line}, column {first_col}"
                ),
                key_node.start_mark,
            )
        seen[key] = (key_node.start_mark.line + 1, key_node.start_mark.column + 1)
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _workflow_paths() -> list[Path]:
    return sorted(
        path
        for pattern in ("*.yml", "*.yaml")
        for path in WORKFLOW_ROOT.glob(pattern)
        if path.is_file()
    )


@pytest.mark.parametrize(
    "path",
    _workflow_paths(),
    ids=lambda path: path.name,
)
def test_github_workflow_yaml_has_no_duplicate_mapping_keys(path: Path) -> None:
    """Every GitHub Actions workflow must be parseable and duplicate-key free.

    GitHub rejects an invalid workflow before creating any jobs, which appears
    as a workflow run with jobs=0.  Reject duplicate keys in CI before merge.
    """
    with path.open(encoding="utf-8") as handle:
        try:
            yaml.load(handle, Loader=_UniqueKeyLoader)
        except yaml.YAMLError as exc:
            pytest.fail(f"{path.relative_to(WORKFLOW_ROOT.parent.parent)} invalid YAML: {exc}")


def test_trade_close_workflow_keeps_single_exit_router_env_key() -> None:
    """Regression for the pre-PR115 trade-close jobs=0 workflow failure."""
    path = WORKFLOW_ROOT / "trade-close.yml"
    content = path.read_text(encoding="utf-8")
    assert content.count('  PB1_EXIT_ROUTER_ENABLED: "1"') == 1
