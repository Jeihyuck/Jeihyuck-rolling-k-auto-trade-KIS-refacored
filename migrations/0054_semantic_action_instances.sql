ALTER TABLE execution_claims
    ADD COLUMN IF NOT EXISTS action_instance TEXT;

ALTER TABLE us_execution_claims
    ADD COLUMN IF NOT EXISTS action_instance TEXT;
