-- Durable PB1 protective exit debt. It never proves broker terminality.
CREATE TABLE IF NOT EXISTS public.us_protective_stop_obligations (
 trading_epoch_id text NOT NULL,
 env text NOT NULL CHECK (env='practice'),
 account_id text NOT NULL CHECK (length(account_id)>0),
 strategy_owner text NOT NULL CHECK (strategy_owner='US_STANDARD'),
 symbol text NOT NULL CHECK (length(symbol)>0 AND symbol<>'TQQQ'),
 lifecycle_id text NOT NULL CHECK(length(lifecycle_id)>0),
 stop_family text NOT NULL,
 severity integer NOT NULL CHECK (severity BETWEEN 1 AND 3),
 target_qty integer NOT NULL CHECK (target_qty>0),
 confirmed_filled_qty integer NOT NULL DEFAULT 0 CHECK (confirmed_filled_qty>=0),
 signal_price numeric NOT NULL CHECK (signal_price>0),
 signal_pnl_pct numeric NOT NULL,
 soft_stop_pct numeric NOT NULL,
 hard_stop_pct numeric NOT NULL,
 first_signal_at timestamptz NOT NULL,
 last_signal_at timestamptz NOT NULL,
 obligation_state text NOT NULL DEFAULT 'PENDING' CHECK
   (obligation_state IN ('PENDING','BLOCKED','RECONCILE_REQUIRED','SUBMITTED','PARTIAL','ESCALATED','SATISFIED')),
 blocking_reason text,
 original_order_no text,
 terminal_proof jsonb,
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (trading_epoch_id,env,account_id,strategy_owner,symbol,lifecycle_id),
 CHECK (confirmed_filled_qty<=target_qty),
 CHECK (obligation_state<>'SATISFIED' OR terminal_proof IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_us_stop_obligations_pending
 ON public.us_protective_stop_obligations(trading_epoch_id,env,severity DESC,first_signal_at)
 WHERE obligation_state<>'SATISFIED';
ALTER TABLE public.us_protective_stop_obligations ENABLE ROW LEVEL SECURITY;
