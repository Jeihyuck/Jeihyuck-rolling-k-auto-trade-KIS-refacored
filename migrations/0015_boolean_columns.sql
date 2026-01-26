-- Change positions.tp1_done and tp2_done from INTEGER to BOOLEAN (safe cast)

-- positions.tp1_done
ALTER TABLE positions
  ALTER COLUMN tp1_done DROP DEFAULT;

ALTER TABLE positions
  ALTER COLUMN tp1_done TYPE BOOLEAN
  USING (tp1_done <> 0);

ALTER TABLE positions
  ALTER COLUMN tp1_done SET DEFAULT FALSE;

ALTER TABLE positions
  ALTER COLUMN tp1_done SET NOT NULL;

-- positions.tp2_done
ALTER TABLE positions
  ALTER COLUMN tp2_done DROP DEFAULT;

ALTER TABLE positions
  ALTER COLUMN tp2_done TYPE BOOLEAN
  USING (tp2_done <> 0);

ALTER TABLE positions
  ALTER COLUMN tp2_done SET DEFAULT FALSE;

ALTER TABLE positions
  ALTER COLUMN tp2_done SET NOT NULL;