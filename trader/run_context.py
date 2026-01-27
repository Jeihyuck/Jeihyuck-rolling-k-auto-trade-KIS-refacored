from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import uuid4


@dataclass
class RunContext:
    run_id: str
    env: str
    strategy: str
    started_at: datetime
    gh_run_number: Optional[int] = None
    git_sha: Optional[str] = None
    window: Optional[str] = None
    phase: Optional[str] = None
    dry_run: bool = False

    @classmethod
    def new(
        cls,
        env: str,
        strategy: str,
        gh_run_number: Optional[int] = None,
        git_sha: Optional[str] = None,
        window: Optional[str] = None,
        phase: Optional[str] = None,
        dry_run: bool = False,
    ) -> RunContext:
        return cls(
            run_id=str(uuid4()),
            env=env,
            strategy=strategy,
            started_at=datetime.now(),
            gh_run_number=gh_run_number,
            git_sha=git_sha,
            window=window,
            phase=phase,
            dry_run=dry_run,
        )