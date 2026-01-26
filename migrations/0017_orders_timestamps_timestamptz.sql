-- migrations/0017_orders_timestamps_timestamptz.sql
-- 목적: orders의 시간 컬럼들을 TEXT -> TIMESTAMPTZ로 통일

BEGIN;

-- 1) created_at
ALTER TABLE orders
  ALTER COLUMN created_at TYPE TIMESTAMPTZ
  USING (
    CASE
      WHEN created_at IS NULL OR created_at = '' THEN NULL
      ELSE created_at::timestamptz
    END
  );

-- 2) updated_at
ALTER TABLE orders
  ALTER COLUMN updated_at TYPE TIMESTAMPTZ
  USING (
    CASE
      WHEN updated_at IS NULL OR updated_at = '' THEN NULL
      ELSE updated_at::timestamptz
    END
  );

-- 3) submitted_at (존재하면)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='orders' AND column_name='submitted_at'
  ) THEN
    EXECUTE $q$
      ALTER TABLE orders
        ALTER COLUMN submitted_at TYPE TIMESTAMPTZ
        USING (
          CASE
            WHEN submitted_at IS NULL OR submitted_at = '' THEN NULL
            ELSE submitted_at::timestamptz
          END
        )
    $q$;
  END IF;
END $$;

-- 4) acked_at (존재하면)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='orders' AND column_name='acked_at'
  ) THEN
    EXECUTE $q$
      ALTER TABLE orders
        ALTER COLUMN acked_at TYPE TIMESTAMPTZ
        USING (
          CASE
            WHEN acked_at IS NULL OR acked_at = '' THEN NULL
            ELSE acked_at::timestamptz
          END
        )
    $q$;
  END IF;
END $$;

-- 5) 기본값/NOT NULL 정책(현재 운영 정책에 맞춰 최소만)
ALTER TABLE orders
  ALTER COLUMN created_at SET DEFAULT now();

ALTER TABLE orders
  ALTER COLUMN updated_at SET DEFAULT now();

-- 6) 조회 성능 인덱스 (env + created_at 많이 탐)
CREATE INDEX IF NOT EXISTS idx_orders_env_created_at
  ON orders (env, created_at);

COMMIT;