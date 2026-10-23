-- P0 Stage4 practice-only reconciliation writer readiness and generation.
-- A record is diagnostic evidence; broker order authorization must still be
-- enforced at the canonical router and account reservation transaction.
CREATE TABLE IF NOT EXISTS public.us_stage4_reconcile_health (
 env text NOT NULL CHECK(env='practice'),
 account_fingerprint text NOT NULL CHECK(length(account_fingerprint)=64),
 trading_epoch_id text NOT NULL CHECK(length(trading_epoch_id)>0),
 writer_generation uuid NOT NULL,
 worker_state text NOT NULL CHECK(worker_state IN ('READY','FAILED','STOPPED')),
 evidence_at timestamptz,
 reconcile_status text NOT NULL,
 replay_status text NOT NULL,
 pending_count integer NOT NULL CHECK (pending_count>=0),
 unresolved_count integer NOT NULL CHECK (unresolved_count>=0),
 filled_rows integer NOT NULL CHECK (filled_rows>=0),
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(env,account_fingerprint,trading_epoch_id)
);
ALTER TABLE public.us_stage4_reconcile_health ENABLE ROW LEVEL SECURITY;
