"""
Core type definitions for the trading system.

This module defines the canonical types used across the system for 
environment and execution mode identification.
"""

from __future__ import annotations
from typing import Literal

# 🔐 DB namespace: account environment only (practice/paper/real)
ACCOUNT_ENV = Literal["practice", "paper", "real"]

# Execution mode: determines behavior gates (LIVE/DIAG/SIM)
EXEC_MODE = Literal["LIVE", "DIAG", "SIM"]

# Valid account environments
VALID_ACCOUNT_ENVS = {"practice", "paper", "real"}

# Valid execution modes
VALID_EXEC_MODES = {"LIVE", "DIAG", "SIM"}


def validate_account_env(env: str) -> str:
    """
    Validate and normalize account environment.
    
    Args:
        env: Environment string to validate
        
    Returns:
        Normalized environment string (lowercase)
        
    Raises:
        ValueError: If environment is not valid
    """
    env_normalized = env.lower().strip()
    if env_normalized not in VALID_ACCOUNT_ENVS:
        raise ValueError(
            f"Invalid account_env: {env}. Must be one of {VALID_ACCOUNT_ENVS}"
        )
    return env_normalized


def validate_exec_mode(mode: str) -> str:
    """
    Validate and normalize execution mode.
    
    Args:
        mode: Mode string to validate
        
    Returns:
        Normalized mode string (uppercase)
        
    Raises:
        ValueError: If mode is not valid
    """
    mode_normalized = mode.upper().strip()
    if mode_normalized not in VALID_EXEC_MODES:
        raise ValueError(
            f"Invalid exec_mode: {mode}. Must be one of {VALID_EXEC_MODES}"
        )
    return mode_normalized
