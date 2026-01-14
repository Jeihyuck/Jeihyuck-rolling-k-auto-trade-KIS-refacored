#!/usr/bin/env bash
set -euo pipefail

MESSAGE="${1:-Update bot state (plain) [skip ci]}"

python -m trader.botstate_persist --message "${MESSAGE}"
