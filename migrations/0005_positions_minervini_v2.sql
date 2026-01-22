ALTER TABLE positions ADD COLUMN last_add_price REAL;
ALTER TABLE positions ADD COLUMN last_stop_update_ts TEXT;
ALTER TABLE positions ADD COLUMN partial_exit_level INTEGER DEFAULT 0;
ALTER TABLE positions ADD COLUMN base_id TEXT;
