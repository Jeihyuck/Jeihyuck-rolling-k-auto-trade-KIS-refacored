from pathlib import Path

WORKFLOWS = [
    Path(".github/workflows/us-trade-am.yml"),
    Path(".github/workflows/us-trade-afternoon.yml"),
]

FORBIDDEN = [
    "python - <<'PY'",
    "python3 - <<'PY'",
    "python - <<\"PY\"",
    "python3 - <<\"PY\"",
    "$(python - <<'PY'",
    "$(python3 - <<'PY'",
]


def test_us_workflows_do_not_use_embedded_python_heredoc():
    for path in WORKFLOWS:
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN:
            assert pattern not in text, f"{path} contains forbidden embedded heredoc pattern: {pattern}"
