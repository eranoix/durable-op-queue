/**
 * Every guarantee rests on the unique index on (kind, idempotency_key): the
 * database refuses duplicates, because an application-level check-then-insert
 * races under concurrent submits.
 */

import type { Database } from 'better-sqlite3';

export const SCHEMA = `
CREATE TABLE IF NOT EXISTS operations (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  idempotency_key   TEXT    NOT NULL,
  kind              TEXT    NOT NULL,
  payload           TEXT    NOT NULL DEFAULT 'null',
  status            TEXT    NOT NULL DEFAULT 'pending',
  attempts          INTEGER NOT NULL DEFAULT 0,
  max_attempts      INTEGER NOT NULL DEFAULT 5,
  next_attempt_at   INTEGER NOT NULL,
  expires_at        INTEGER,
  lease_expires_at  INTEGER,
  last_error        TEXT,
  created_at        INTEGER NOT NULL,
  updated_at        INTEGER NOT NULL
);

-- The guarantee itself. Submitting the same intent twice hits this constraint
-- and is absorbed, rather than becoming a second operation.
CREATE UNIQUE INDEX IF NOT EXISTS operations_identity
  ON operations (kind, idempotency_key);

-- Claiming work scans for due, unleased rows. Without this the scan is a table
-- scan that gets slower exactly as the backlog you need to drain gets bigger.
CREATE INDEX IF NOT EXISTS operations_claimable
  ON operations (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS attempts (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  operation_id   INTEGER NOT NULL REFERENCES operations(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL,
  outcome        TEXT    NOT NULL,
  error          TEXT,
  duration_ms    INTEGER NOT NULL DEFAULT 0,
  started_at     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS attempts_by_operation
  ON attempts (operation_id, attempt_number);
`;

export function migrate(db: Database): void {
  // WAL lets readers inspect the queue while workers write.
  db.pragma('journal_mode = WAL');
  // SQLite does not enforce ON DELETE CASCADE unless foreign keys are enabled.
  db.pragma('foreign_keys = ON');
  db.exec(SCHEMA);
}
