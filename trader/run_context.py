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
    
    # DEPRECATED: Kept for backward compatibility, will be removed
    @property
    def env(self) -> str:
        """Deprecated: Use account_env instead."""
        return self.account_env
    
    @property
    def run_id(self) -> str:
        """Deprecated: run_id is being removed from the system."""
        import warnings
        warnings.warn(
            "run_id is deprecated and will be removed. "
            "Do not use run_id for filtering or storage.",
            DeprecationWarning,
            stacklevel=2
        )
        return "deprecated"

    @classmethod
    def new(
        cls,
        account_env: str,
        exec_mode: str,
        strategy: str,
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
        
        # Validate
        from trader.types import validate_account_env, validate_exec_mode
        account_env = validate_account_env(account_env)
        exec_mode = validate_exec_mode(exec_mode)
        
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
        )