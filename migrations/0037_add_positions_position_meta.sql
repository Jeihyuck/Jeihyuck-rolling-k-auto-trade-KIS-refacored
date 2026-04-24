-- Migration: 0037_add_positions_position_meta.sql
-- Add position_meta JSONB column to positions table
-- Stores trade_horizon, exit_policy_family, tp1_done/tp2_done, r_value etc.

ALTER TABLE positions
ADD COLUMN IF NOT EXISTS position_meta JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN positions.position_meta IS
  'Horizon-based exit policy state: trade_horizon, exit_policy_family, '
  'tp1_done, tp2_done, max_r_since_entry, max_pnl_pct_since_entry, '
  'initial_stop_price, runner_qty, current_stop_price, etc.';
