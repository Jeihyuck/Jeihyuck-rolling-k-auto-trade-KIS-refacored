-- 0064: US_STANDARD portfolio profit-giveback native-SELL control plane.
-- SCHEMA ONLY: does not authorize, arm, submit, cancel, or alter any order.
-- Must be installed by the existing migration runner in its own transaction.
-- Immutable policy approval is independent of original BUY/V2/V3 contracts.

CREATE TABLE IF NOT EXISTS public.us_portfolio_guard_policy (
  policy_id text NOT NULL,
  policy_version text NOT NULL,
  env text NOT NULL CHECK(env='practice'),
  account_hash text NOT NULL CHECK(account_hash ~ '^[0-9a-f]{64}$'),
  trading_epoch_id text NOT NULL,
  portfolio_epoch_id text NOT NULL,
  sleeve text NOT NULL CHECK(sleeve='US_STANDARD'),
  authorization_sha256 text NOT NULL CHECK(authorization_sha256 ~ '^[0-9a-f]{64}$'),
  authorization_json jsonb NOT NULL,
  signed_at timestamptz NOT NULL,
  effective_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(policy_id,policy_version,env,account_hash,trading_epoch_id,portfolio_epoch_id),
  CONSTRAINT ck_us_portfolio_guard_auth_time CHECK (expires_at > effective_at)
);

CREATE TABLE IF NOT EXISTS public.us_portfolio_guard_state (
  policy_id text NOT NULL,
  policy_version text NOT NULL,
  env text NOT NULL CHECK(env='practice'),
  account_hash text NOT NULL CHECK(account_hash ~ '^[0-9a-f]{64}$'),
  trading_epoch_id text NOT NULL,
  portfolio_epoch_id text NOT NULL,
  sleeve text NOT NULL CHECK(sleeve='US_STANDARD'),
  accounting_method text NOT NULL,
  baseline_provenance jsonb NOT NULL,
  high_water_pnl_usd numeric(20,6) NOT NULL,
  latest_pnl_usd numeric(20,6) NOT NULL,
  last_snapshot_id text NOT NULL,
  last_snapshot_at timestamptz NOT NULL,
  episode_id text,
  episode_anchor jsonb,
  stage_no integer NOT NULL DEFAULT 0 CHECK(stage_no>=0),
  state text NOT NULL DEFAULT 'WATCH'
    CHECK(state IN ('WATCH','TRIGGERED','PLANNED','SUBMIT_UNKNOWN','PARTIAL','COMPLETE','REARM_WAIT','BLOCKED')),
  revision bigint NOT NULL DEFAULT 1 CHECK(revision>0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(policy_id,policy_version,env,account_hash,trading_epoch_id,portfolio_epoch_id),
  FOREIGN KEY(policy_id,policy_version,env,account_hash,trading_epoch_id,portfolio_epoch_id)
    REFERENCES public.us_portfolio_guard_policy(policy_id,policy_version,env,account_hash,trading_epoch_id,portfolio_epoch_id)
);

CREATE TABLE IF NOT EXISTS public.us_portfolio_guard_actions (
  policy_id text NOT NULL,
  policy_version text NOT NULL,
  env text NOT NULL CHECK(env='practice'),
  account_hash text NOT NULL CHECK(account_hash ~ '^[0-9a-f]{64}$'),
  trading_epoch_id text NOT NULL,
  portfolio_epoch_id text NOT NULL,
  episode_id text NOT NULL,
  stage_no integer NOT NULL CHECK(stage_no>0),
  symbol text NOT NULL,
  position_cycle_id text NOT NULL,
  action_key text NOT NULL UNIQUE,
  target_qty integer NOT NULL CHECK(target_qty>0),
  cumulative_verified_fill_qty integer NOT NULL DEFAULT 0
    CHECK(cumulative_verified_fill_qty>=0),
  reserved_qty integer NOT NULL DEFAULT 0 CHECK(reserved_qty>=0),
  state text NOT NULL CHECK(state IN ('PLANNED','SUBMITTED','ACKED','PARTIAL','SUBMIT_UNKNOWN','COMPLETE','BLOCKED')),
  manifest_sha256 text NOT NULL,
  snapshot_id text NOT NULL,
  decision_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(policy_id,policy_version,env,account_hash,trading_epoch_id,portfolio_epoch_id,episode_id,stage_no,symbol,position_cycle_id),
  FOREIGN KEY(policy_id,policy_version,env,account_hash,trading_epoch_id,portfolio_epoch_id)
    REFERENCES public.us_portfolio_guard_policy(policy_id,policy_version,env,account_hash,trading_epoch_id,portfolio_epoch_id),
  CONSTRAINT ck_us_portfolio_guard_fill_not_over_target
    CHECK(cumulative_verified_fill_qty <= target_qty),
  CONSTRAINT ck_us_portfolio_guard_reservation_not_over_target
    CHECK(reserved_qty <= target_qty)
);

CREATE INDEX IF NOT EXISTS ix_us_portfolio_guard_actions_pending_0064
  ON public.us_portfolio_guard_actions(account_hash,trading_epoch_id,portfolio_epoch_id,state,updated_at);

-- Signed, per-episode BUY-cap release; the portfolio guard's original
-- HWM/trigger and SELL facts remain immutable for audit. Missing release
-- always holds risk budget rather than silently restoring it.
CREATE TABLE IF NOT EXISTS public.us_portfolio_guard_reentry_release (
  policy_id text NOT NULL,
  policy_version text NOT NULL,
  env text NOT NULL CHECK (env='practice'),
  account_hash text NOT NULL CHECK(account_hash ~ '^[0-9a-f]{64}$'),
  trading_epoch_id text NOT NULL,
  portfolio_epoch_id text NOT NULL,
  episode_id text NOT NULL,
  release_sha256 text NOT NULL CHECK(release_sha256 ~ '^[0-9a-f]{64}$'),
  release_json jsonb NOT NULL,
  effective_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (policy_id,policy_version,env,account_hash,trading_epoch_id,
               portfolio_epoch_id,episode_id),
  FOREIGN KEY(policy_id,policy_version,env,account_hash,trading_epoch_id,
              portfolio_epoch_id)
    REFERENCES public.us_portfolio_guard_policy(
      policy_id,policy_version,env,account_hash,trading_epoch_id,portfolio_epoch_id
    ),
  CONSTRAINT ck_us_portfolio_guard_release_window CHECK (expires_at > effective_at)
);

-- Durable BUY budget consumed *before* the KIS native HTTP boundary.
-- Keep reservations after unknown POST/ACK failure: they can be released
-- only by separately verified, audited broker cancellation.
CREATE TABLE IF NOT EXISTS public.us_portfolio_guard_reentry_buy_reservations (
  policy_id text NOT NULL,
  policy_version text NOT NULL,
  env text NOT NULL CHECK(env='practice'),
  account_hash text NOT NULL CHECK(account_hash ~ '^[0-9a-f]{64}
ALTER TABLE public.us_portfolio_guard_policy ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.us_portfolio_guard_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.us_portfolio_guard_actions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.us_portfolio_guard_reentry_release ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.us_portfolio_guard_reentry_buy_reservations ENABLE ROW LEVEL SECURITY;
-- Development PostgreSQL containers do not create Supabase API roles.
-- Revoke only roles which actually exist; RLS still defaults to deny.
DO $guard_auth_roles$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
   REVOKE ALL ON TABLE public.us_portfolio_guard_policy,
                       public.us_portfolio_guard_state,
                       public.us_portfolio_guard_actions,
                       public.us_portfolio_guard_reentry_release,
                       public.us_portfolio_guard_reentry_buy_reservations FROM anon;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
   REVOKE ALL ON TABLE public.us_portfolio_guard_policy,
                       public.us_portfolio_guard_state,
                       public.us_portfolio_guard_actions,
                       public.us_portfolio_guard_reentry_release,
                       public.us_portfolio_guard_reentry_buy_reservations FROM authenticated;
 END IF;
END $guard_auth_roles$;
),
  trading_epoch_id text NOT NULL,
  portfolio_epoch_id text NOT NULL,
  episode_id text NOT NULL,
  client_order_key text NOT NULL,
  notional_usd numeric(20,6) NOT NULL CHECK(notional_usd>0),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (policy_id,policy_version,env,account_hash,trading_epoch_id,
               portfolio_epoch_id,episode_id,client_order_key),
  UNIQUE (client_order_key),
  FOREIGN KEY (policy_id,policy_version,env,account_hash,trading_epoch_id,
               portfolio_epoch_id,episode_id)
    REFERENCES public.us_portfolio_guard_reentry_release (
      policy_id,policy_version,env,account_hash,trading_epoch_id,
      portfolio_epoch_id,episode_id
    )
);

-- Guard tables are internal broker-control ledgers, never public REST APIs.
ALTER TABLE public.us_portfolio_guard_policy ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.us_portfolio_guard_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.us_portfolio_guard_actions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.us_portfolio_guard_reentry_release ENABLE ROW LEVEL SECURITY;
-- Development PostgreSQL containers do not create Supabase API roles.
-- Revoke only roles which actually exist; RLS still defaults to deny.
DO $guard_auth_roles$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
   REVOKE ALL ON TABLE public.us_portfolio_guard_policy,
                       public.us_portfolio_guard_state,
                       public.us_portfolio_guard_actions,
                       public.us_portfolio_guard_reentry_release,
                       public.us_portfolio_guard_reentry_buy_reservations FROM anon;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
   REVOKE ALL ON TABLE public.us_portfolio_guard_policy,
                       public.us_portfolio_guard_state,
                       public.us_portfolio_guard_actions,
                       public.us_portfolio_guard_reentry_release,
                       public.us_portfolio_guard_reentry_buy_reservations FROM authenticated;
 END IF;
END $guard_auth_roles$;
