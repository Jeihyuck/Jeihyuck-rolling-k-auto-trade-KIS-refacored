#!/usr/bin/env bash
# Requested practice rollout for new US_STANDARD SWING contracts. Existing
# frozen contracts remain authoritative; this does not grant order permission.
if [[ "${KIS_ENV:-}" == "practice" && "${STRATEGY_ENV:-practice}" == "practice" ]]; then
  if [[ "${US_SWING_V3_ACTIVATION:-ON}" == "ON" ]]; then
    export US_SWING_EXIT_MODE=V3_ACTIVE_PRACTICE
    export US_SWING_V3_DEPLOY_APPROVED=1
  elif [[ "${US_SWING_V3_ACTIVATION}" == "OFF" ]]; then
    export US_SWING_EXIT_MODE=LEGACY_V2
    export US_SWING_V3_DEPLOY_APPROVED=0
  else
    echo "[US_SWING_V3][CONFIG_ERROR] activation_requires_ON_or_OFF" >&2
    return 43
  fi
fi
echo "[US_SWING_V3][PROFILE] env=${KIS_ENV:-missing} mode=${US_SWING_EXIT_MODE:-LEGACY_V2} approved=${US_SWING_V3_DEPLOY_APPROVED:-0} scope=new_standard_swing frozen_existing=preserved"
