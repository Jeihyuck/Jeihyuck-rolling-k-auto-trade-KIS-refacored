-- Migration 0040: KR provenance columns
-- Adds producer_branch, producer_sha, producer_workflow, producer_run_id,
-- is_quarantined, quarantine_reason to pb1_watchlist (final30) for branch provenance tracking.
-- Also adds producer_branch index on ledger_events.payload_json for fast lookup.
-- Safe to run multiple times (IF NOT EXISTS).

-- [2026-05-27] Korean market stability fix: track which branch produced each final30 snapshot.

ALTER TABLE pb1_watchlist
  ADD COLUMN IF NOT EXISTS producer_branch    TEXT,
  ADD COLUMN IF NOT EXISTS producer_sha       TEXT,
  ADD COLUMN IF NOT EXISTS producer_workflow  TEXT,
  ADD COLUMN IF NOT EXISTS producer_run_id    TEXT,
  ADD COLUMN IF NOT EXISTS is_quarantined     BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS quarantine_reason  TEXT;

COMMENT ON COLUMN pb1_watchlist.producer_branch   IS 'GITHUB_REF_NAME of the branch that produced this row';
COMMENT ON COLUMN pb1_watchlist.producer_sha      IS 'GITHUB_SHA of the commit that produced this row';
COMMENT ON COLUMN pb1_watchlist.producer_workflow IS 'GITHUB_WORKFLOW of the workflow that produced this row';
COMMENT ON COLUMN pb1_watchlist.producer_run_id   IS 'GITHUB_RUN_ID of the workflow run that produced this row';
COMMENT ON COLUMN pb1_watchlist.is_quarantined    IS 'If true, this row was produced by a non-canonical branch and is quarantined';
COMMENT ON COLUMN pb1_watchlist.quarantine_reason IS 'Reason this row is quarantined (e.g. BRANCH_MISMATCH)';
