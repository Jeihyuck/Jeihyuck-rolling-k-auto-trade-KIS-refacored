ALTER TABLE positions ADD COLUMN status TEXT;
ALTER TABLE positions ADD COLUMN closed_reason TEXT;
ALTER TABLE positions ADD COLUMN closed_ts TIMESTAMP;
