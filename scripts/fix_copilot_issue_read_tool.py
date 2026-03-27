#!/usr/bin/env python3
"""Fix GitHub Copilot custom agent prompt files that reference deprecated tools.

This utility removes the unsupported `github/issue_read` tool token from
agent prompt files (Ask/Explore/Plan) in VS Code remote user storage.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

DEFAULT_AGENT_FILES = (
    "ask-agent/Ask.agent.md",
    "explore-agent/Explore.agent.md",
    "plan-agent/Plan.agent.md",
)

DEFAULT_BASE_DIRS = (
    Path("/home/codespace/.vscode-remote/data/User/globalStorage/github.copilot-chat"),
    Path.home() / ".vscode-remote/data/User/globalStorage/github.copilot-chat",
)


def fix_text(text: str) -> tuple[str, int]:
    original = text

    # YAML list style: `- github/issue_read`
    text = re.sub(r"(?m)^\s*-\s*github/issue_read\s*$\n?", "", text)

    # JSON/YAML inline array style with optional quotes and surrounding commas.
    text = re.sub(r"\s*,\s*['\"]github/issue_read['\"]", "", text)
    text = re.sub(r"['\"]github/issue_read['\"]\s*,\s*", "", text)

    # Plain token style (e.g., tools: github/a, github/issue_read, github/b)
    text = re.sub(r"\s*,\s*github/issue_read\b", "", text)
    text = re.sub(r"\bgithub/issue_read\s*,\s*", "", text)

    # Single remaining token.
    text = re.sub(r"\bgithub/issue_read\b", "", text)

    # Compact comma artifacts left after token removal.
    text = re.sub(r",\s*,", ", ", text)
    text = re.sub(r"\[\s*,", "[", text)
    text = re.sub(r",\s*\]", "]", text)

    changes = 0 if text == original else original.count("github/issue_read")
    return text, changes


def resolve_targets(extra_paths: list[str]) -> list[Path]:
    if extra_paths:
        return [Path(p).expanduser() for p in extra_paths]

    resolved: list[Path] = []
    for base in DEFAULT_BASE_DIRS:
        for rel in DEFAULT_AGENT_FILES:
            p = base / rel
            if p.exists() and p not in resolved:
                resolved.append(p)
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Remove deprecated github/issue_read tool references from Copilot agent prompt files."
    )
    parser.add_argument("paths", nargs="*", help="Optional explicit file paths to fix")
    parser.add_argument("--dry-run", action="store_true", help="Show files that would change")
    args = parser.parse_args()

    targets = resolve_targets(args.paths)
    if not targets:
        print("No target files found.")
        return 0

    touched = 0
    replacements = 0
    for target in targets:
        if not target.exists():
            print(f"SKIP (missing): {target}")
            continue

        src = target.read_text(encoding="utf-8")
        fixed, count = fix_text(src)
        if count == 0:
            print(f"OK (no change): {target}")
            continue

        touched += 1
        replacements += count
        if args.dry_run:
            print(f"DRY-RUN change: {target} (removed {count} token(s))")
            continue

        target.write_text(fixed, encoding="utf-8")
        print(f"UPDATED: {target} (removed {count} token(s))")

    print(f"Done. files_changed={touched}, removed_tokens={replacements}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
