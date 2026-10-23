-- Stage 4 PB1 STOP policy revision: append-only, no automatic operational roll-forward.
-- Original v2 frozen buy contract and SHA remain unchanged.
-- Only separately approved US_STANDARD/ practice account-scoped rows are applied.
CREATE TABLE IF NOT EXISTS public.us_stop_policy_revisions (
 trading_epoch_id text NOT NULL,
 env text NOT NULL CHECK (env='practice'),
 account_id text NOT NULL CHECK (length(account_id)>0),
 strategy_owner text NOT NULL CHECK (strategy_owner='US_STANDARD'),
 symbol text NOT NULL CHECK (symbol<>'TQQQ' AND length(symbol)>0),
 lifecycle_id text NOT NULL CHECK (length(lifecycle_id)>0),
 revision_code text NOT NULL CHECK (revision_code='PB1_STOP_3_6_V1'),
 source_contract_sha256 text NOT NULL CHECK (length(source_contract_sha256)=64),
 soft_stop numeric(6,5) NOT NULL CHECK (soft_stop=0.03),
 hard_stop numeric(6,5) NOT NULL CHECK (hard_stop=0.06),
 approval_ref text NOT NULL CHECK (length(approval_ref)>0),
 active boolean NOT NULL DEFAULT false,
 effective_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (trading_epoch_id, env, account_id, strategy_owner, symbol, lifecycle_id, revision_code)
);
CREATE INDEX IF NOT EXISTS idx_us_stop_policy_revisions_active
 ON public.us_stop_policy_revisions(trading_epoch_id,env,lifecycle_id,account_id,effective_at DESC)
 WHERE active IS TRUE;
ALTER TABLE public.us_stop_policy_revisions ENABLE ROW LEVEL SECURITY;
-- No anon/authenticated policies: internal service role / PostgreSQL only.
