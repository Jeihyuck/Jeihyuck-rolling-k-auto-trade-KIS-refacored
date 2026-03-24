from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import uuid4


@dataclass
class RunContext:
    """
    Run context for trading system.
    
    CRITICAL: DB keys use account_env only. exec_mode is for runtime gates.
    
    Attributes:
        account_env: DB namespace (practice/paper/real) - MUST be lowercase
        exec_mode: Execution mode (LIVE/DIAG/SIM) - MUST be uppercase
        strategy: Trading strategy name
        started_at: When this run started
        gh_run_number: GitHub Actions run number (optional)
        git_sha: Git commit SHA (optional)
        window: Trading window (optional)
        phase: Trading phase (optional)
        dry_run: Whether this is a dry run
    """
    account_env: str  # practice/paper/real (DB namespace)
    exec_mode: str    # LIVE/DIAG/SIM (execution mode)
    strategy: str
    started_at: datetime
    gh_run_number: Optional[int] = None
    git_sha: Optional[str] = None
    window: Optional[str] = None
    phase: Optional[str] = None
    dry_run: bool = False
    run_id: Optional[str] = None  # Added for compatibility

    def __init__(
        self,
        *args,
        env: Optional[str] = None,
        strategy: Optional[str] = None,
        gh_run_number: Optional[int] = None,
        git_sha: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Accept flexible keyword arguments to avoid unexpected-kw errors.

        env, strategy, gh_run_number, git_sha are optional, and any additional
        kwargs are stored as attributes on the instance.
        """
        account_env = kwargs.pop("account_env", None)
        exec_mode = kwargs.pop("exec_mode", None)
        started_at = kwargs.pop("started_at", None)
        window = kwargs.pop("window", None)
        phase = kwargs.pop("phase", None)
        dry_run = kwargs.pop("dry_run", False)
        run_id = kwargs.pop("run_id", None)

        if account_env is None and env is not None:
            account_env = env

        if strategy is None and "strategy" in kwargs:
            strategy = kwargs.pop("strategy")

        self.account_env = account_env
        self.exec_mode = exec_mode
        self.strategy = strategy
        self.started_at = started_at or datetime.now()
        self.gh_run_number = gh_run_number
        self.git_sha = git_sha
        self.window = window
        self.phase = phase
        self.dry_run = dry_run
        self.run_id = run_id

        for key, value in kwargs.items():
            setattr(self, key, value)

        self.__post_init__()

        if args:
            try:
                super().__init__(*args)
            except TypeError:
                super().__init__()
    
    def __post_init__(self):
        """Auto-generate run_id if not provided."""
        if not self.run_id:
            parts = []
            if self.gh_run_number:
                parts.append(str(self.gh_run_number))
            if self.git_sha:
                parts.append(self.git_sha[:8])
            self.run_id = "-".join(parts) if parts else "local"
    
    # DEPRECATED: Kept for backward compatibility, will be removed
    @property
    def env(self) -> str:
        """Deprecated: Use account_env instead."""
        return self.account_env

    @classmethod
    def new(
        cls,
        account_env: Optional[str] = None,
        exec_mode: Optional[str] = None,
        strategy: str = "",
        gh_run_number: Optional[int] = None,
        git_sha: Optional[str] = None,
        window: Optional[str] = None,
        phase: Optional[str] = None,
        dry_run: bool = False,
        # Backward compatibility
        env: Optional[str] = None,
    ) -> RunContext:
        """
        Create new RunContext.
        
        Args:
            account_env: Account environment (practice/paper/real)
            exec_mode: Execution mode (LIVE/DIAG/SIM)
            strategy: Strategy name
            gh_run_number: GitHub run number (optional)
            git_sha: Git SHA (optional)
            window: Trading window (optional)
            phase: Trading phase (optional)
            dry_run: Dry run flag
            env: DEPRECATED - use account_env instead
        """
        # Backward compatibility: if env is provided, use it as account_env
        if env is not None and account_env is None:
            account_env = env
        if account_env is None:
            account_env = "practice"
        if exec_mode is None:
            exec_mode = "DIAG" if dry_run else "LIVE"
        explicit_run_id = None
        if str(account_env).strip().lower() not in {"practice", "paper", "real"}:
            explicit_run_id = str(uuid4())
        
        # Validate
        from trader.types import validate_account_env, validate_exec_mode
        try:
            account_env = validate_account_env(account_env)
        except ValueError:
            account_env = str(account_env).strip().lower()
        try:
            exec_mode = validate_exec_mode(exec_mode)
        except ValueError:
            exec_mode = str(exec_mode).strip().upper()
        
        return cls(
            account_env=account_env,
            exec_mode=exec_mode,
            strategy=strategy,
            started_at=datetime.now(),
            gh_run_number=gh_run_number,
            git_sha=git_sha,
            window=window,
            phase=phase,
            dry_run=dry_run,
            run_id=explicit_run_id,
        )