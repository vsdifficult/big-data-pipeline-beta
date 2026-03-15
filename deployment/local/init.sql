CREATE TABLE IF NOT EXISTS comments (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  text TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  language VARCHAR(16),
  metadata JSONB
);

INSERT INTO comments (user_id, text, created_at, language, metadata)
SELECT
  (RANDOM() * 100000)::BIGINT,
  'Sample comment #' || gs,
  NOW() - (RANDOM() * INTERVAL '1 hour'),
  'en',
  '{"source":"seed"}'::jsonb
FROM generate_series(1, 10000) gs;
