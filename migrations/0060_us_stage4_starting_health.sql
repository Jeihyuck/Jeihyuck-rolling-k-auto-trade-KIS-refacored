-- Extend Stage4 non-authorizing bootstrap evidence before first KIS GET.
-- PR #255 required STARTING; the original 0059 constraint permitted only
-- READY/FAILED/STOPPED. Without this migration, boot would fail at INSERT.
-- Must be applied BEFORE enabling Stage4 WORKER. No historical row mutation.
ALTER TABLE public.us_stage4_reconcile_health
 DROP CONSTRAINT IF EXISTS us_stage4_reconcile_health_worker_state_check;
ALTER TABLE public.us_stage4_reconcile_health
 ADD CONSTRAINT us_stage4_reconcile_health_worker_state_check
 CHECK (worker_state IN ('STARTING','READY','FAILED','STOPPED'));
