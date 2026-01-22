ALTER TABLE positions ADD COLUMN entry_ts TEXT;
ALTER TABLE positions ADD COLUMN initial_stop REAL;
ALTER TABLE positions ADD COLUMN stop_price REAL;
ALTER TABLE positions ADD COLUMN max_price REAL;
ALTER TABLE positions ADD COLUMN pyramid_level INTEGER DEFAULT 0;
ALTER TABLE positions ADD COLUMN pivot REAL;
