-- Persist explicit universe snapshot metadata for as_of consistency and reproducibility.

ALTER TABLE IF EXISTS universe_runs
    ADD COLUMN IF NOT EXISTS requested_as_of DATE,
    ADD COLUMN IF NOT EXISTS actual_as_of DATE,
    ADD COLUMN IF NOT EXISTS build_reason TEXT,
    ADD COLUMN IF NOT EXISTS universe_name TEXT;

UPDATE universe_runs
SET requested_as_of = COALESCE(requested_as_of, as_of),
    actual_as_of = COALESCE(actual_as_of, as_of),
    universe_name = COALESCE(universe_name, strategy)
WHERE requested_as_of IS NULL
   OR actual_as_of IS NULL
   OR universe_name IS NULL;
