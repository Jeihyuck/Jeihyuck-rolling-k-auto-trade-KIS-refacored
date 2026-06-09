from pathlib import Path

WORKFLOWS = [
    Path(".github/workflows/us-trade-am.yml"),
    Path(".github/workflows/us-trade-afternoon.yml"),
    Path(".github/workflows/us-harness.yml"),
]

FORBIDDEN_HEREDOC_PATTERNS = [
    "<<",
    "<<-",
]


def test_us_workflows_do_not_use_any_heredoc():
    for path in WORKFLOWS:
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_HEREDOC_PATTERNS:
            assert pattern not in text, f"{path} contains forbidden heredoc/operator pattern: {pattern}"
