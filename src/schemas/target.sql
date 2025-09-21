CREATE TABLE IF NOT EXISTS migrated_users (
  id           INT PRIMARY KEY,
  name         TEXT,
  email        TEXT,
  op           TEXT,
  event_time   TIMESTAMP DEFAULT NOW(),
  processed_at TIMESTAMP DEFAULT NOW()
);