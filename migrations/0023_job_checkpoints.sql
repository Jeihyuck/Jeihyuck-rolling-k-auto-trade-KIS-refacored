-- Add job_checkpoints table for candidate pool prefetch resumability
-- This table stores checkpoint state for long-running jobs like OHLCV prefetch

CREATE TABLE IF NOT EXISTS job_checkpoints (
    job_key VARCHAR(512) PRIMARY KEY,
    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_job_checkpoints_updated ON job_checkpoints(updated_ts DESC);

COMMENT ON TABLE job_checkpoints IS 'Checkpoints for long-running jobs (e.g., OHLCV prefetch)';
COMMENT ON COLUMN job_checkpoints.job_key IS 'Unique identifier: ohlcv_prefetch:{env}:{strategy}:{as_of}:{days}:{chunk_size}';
COMMENT ON COLUMN job_checkpoints.payload IS 'JSON: {next_chunk_index, total_chunks, done_count, success, skip, fail, status, completed_ts, elapsed_total_sec, fail_codes}';
