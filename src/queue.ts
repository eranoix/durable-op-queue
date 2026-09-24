/**
 * The queue.
 *
 * It exists for effects that must happen exactly once against a system you do
 * not control — charge a card, create a remote folder, send a statement — where
 * the process can die at any instant and the provider has no memory of your
 * intent, only of what you asked it to do.
 *
 * The hard part is never the happy path. It is the window between "the
 * provider applied the change" and "our row says so". A crash in that window
 * leaves a completed effect that our database believes is still owed. Retrying
 * blind charges twice; not retrying loses work silently. Neither is acceptable,
 * so the handler is required to distinguish "I made this change" from "it was
 * already there", and the queue records the difference.
 */

import Database from 'better-sqlite3';
import type { Database as Db } from 'better-sqlite3';
import { migrate } from './schema.js';
import {
  PermanentFailure,
  type Attempt,
  type AttemptOutcome,
  type Handler,
  type Operation,
  type SubmitOptions,
} from './types.js';

export interface QueueOptions {
  /** File path, or ':memory:'. */
  path?: string;
  /** Injected for tests, so time can be controlled rather than waited for. */
  now?: () => number;
  /** How long a worker may hold an operation before the lease is reclaimed. */
  leaseMs?: number;
  /** First backoff step; doubles per attempt up to backoffCapMs. */
  backoffBaseMs?: number;
  backoffCapMs?: number;
  /** Default attempt budget when submit does not set one. */
  maxAttempts?: number;
}

interface OperationRow {
  id: number;
  idempotency_key: string;
  kind: string;
  payload: string;
  status: Operation['status'];
  attempts: number;
  max_attempts: number;
  next_attempt_at: number;
  expires_at: number | null;
  lease_expires_at: number | null;
  last_error: string | null;
  created_at: number;
  updated_at: number;
}

interface AttemptRow {
  id: number;
  operation_id: number;
  attempt_number: number;
  outcome: AttemptOutcome;
  error: string | null;
  duration_ms: number;
  started_at: number;
}

export interface SubmitResult {
  operation: Operation;
  /** False when an operation with this identity already existed. */
  created: boolean;
}

export interface RunSummary {
  claimed: number;
  applied: number;
  noop: number;
  retried: number;
  failed: number;
  expired: number;
}

export class OperationQueue {
  private readonly db: Db;
  private readonly now: () => number;
  private readonly leaseMs: number;
  private readonly backoffBaseMs: number;
  private readonly backoffCapMs: number;
  private readonly defaultMaxAttempts: number;
  private readonly handlers = new Map<string, Handler>();

  constructor(opts: QueueOptions = {}) {
    this.db = new Database(opts.path ?? ':memory:');
    this.now = opts.now ?? (() => Date.now());
    this.leaseMs = opts.leaseMs ?? 30_000;
    this.backoffBaseMs = opts.backoffBaseMs ?? 1_000;
    this.backoffCapMs = opts.backoffCapMs ?? 5 * 60_000;
    this.defaultMaxAttempts = opts.maxAttempts ?? 5;
    migrate(this.db);
  }

  register(kind: string, handler: Handler): void {
    this.handlers.set(kind, handler);
  }

  close(): void {
    this.db.close();
  }

  /**
   * Record an intent.
   *
   * Safe to call repeatedly with the same key: the second call returns the
   * existing operation with `created: false` rather than queueing a duplicate.
   * That is what lets a caller retry a submit after a timeout without knowing
   * whether the first one landed — the common case that makes callers invent
   * their own fragile deduplication.
   *
   * The payload of an existing operation is NOT overwritten. Two callers
   * claiming the same identity with different payloads disagree about what the
   * operation is, and the first one to arrive is the one already scheduled;
   * silently adopting the second would change work that may be mid-flight.
   */
  submit(opts: SubmitOptions): SubmitResult {
    const now = this.now();
    const existing = this.findByIdentity(opts.kind, opts.idempotencyKey);
    if (existing) return { operation: existing, created: false };

    const info = this.db
      .prepare(
        `INSERT INTO operations
           (idempotency_key, kind, payload, status, attempts, max_attempts,
            next_attempt_at, expires_at, created_at, updated_at)
         VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?)
         ON CONFLICT (kind, idempotency_key) DO NOTHING`,
      )
      .run(
        opts.idempotencyKey,
        opts.kind,
        JSON.stringify(opts.payload ?? null),
        opts.maxAttempts ?? this.defaultMaxAttempts,
        now + (opts.delayMs ?? 0),
        opts.ttlMs != null ? now + opts.ttlMs : null,
        now,
        now,
      );

    // A concurrent submit can win between the lookup above and this insert.
    // The unique index turns that race into a no-op, and we read back whatever
    // is actually there rather than trusting our own view of a moment ago.
    if (info.changes === 0) {
      const raced = this.findByIdentity(opts.kind, opts.idempotencyKey);
      if (!raced) throw new Error('submit: insert absorbed but no row found');
      return { operation: raced, created: false };
    }

    const row = this.getRow(Number(info.lastInsertRowid));
    if (!row) throw new Error('submit: failed to read back inserted row');
    return { operation: toOperation(row), created: true };
  }

  /**
   * Run one pass over everything currently due.
   *
   * A pass is bounded: it claims what is eligible now and stops. Callers decide
   * the cadence, which keeps this a library rather than a daemon, and makes it
   * trivial to drive deterministically from a test.
   */
  async runOnce(limit = 50): Promise<RunSummary> {
    const summary: RunSummary = {
      claimed: 0, applied: 0, noop: 0, retried: 0, failed: 0, expired: 0,
    };

    summary.expired = this.expireOverdue();

    for (const op of this.claim(limit)) {
      summary.claimed += 1;
      const outcome = await this.execute(op);
      if (outcome === 'applied') summary.applied += 1;
      else if (outcome === 'noop') summary.noop += 1;
      else if (outcome === 'retryable') summary.retried += 1;
      else summary.failed += 1;
    }
    return summary;
  }

  /**
   * Drain until nothing is left to do, advancing a test clock over backoff.
   *
   * Only useful when `now` is injected: real time cannot be skipped, and a
   * production caller should run `runOnce` on a schedule instead.
   */
  async drain(advance: (ms: number) => void, maxPasses = 100): Promise<RunSummary> {
    const total: RunSummary = {
      claimed: 0, applied: 0, noop: 0, retried: 0, failed: 0, expired: 0,
    };
    for (let i = 0; i < maxPasses; i += 1) {
      const pass = await this.runOnce();
      for (const k of Object.keys(total) as (keyof RunSummary)[]) total[k] += pass[k];
      if (this.countPending() === 0) break;
      if (pass.claimed === 0) {
        // Nothing was due. Jump to whenever the earliest pending row becomes
        // eligible, rather than stepping a fixed amount and hoping.
        const next = this.earliestNextAttempt();
        if (next == null) break;
        advance(Math.max(1, next - this.now()));
      }
    }
    return total;
  }

  /**
   * Reconcile against the provider's own view of the world.
   *
   * Everything else here assumes our record of what happened is complete. This
   * is the escape hatch for when it is not: an operation stuck in `running`
   * because the worker died mid-flight, a `failed` one that the provider
   * actually completed on its last attempt before the connection dropped.
   *
   * The probe answers "does this already exist on your side?" and the queue
   * settles the row accordingly. Without a pass like this, the only way back
   * from a torn write is someone reading logs by hand.
   */
  async converge(
    kind: string,
    probe: (op: Operation) => Promise<boolean>,
  ): Promise<{ checked: number; settled: number }> {
    const rows = this.db
      .prepare<[string], OperationRow>(
        `SELECT * FROM operations
          WHERE kind = ? AND status IN ('running', 'failed', 'pending')`,
      )
      .all(kind);

    let settled = 0;
    for (const row of rows) {
      const op = toOperation(row);
      if (!(await probe(op))) continue;
      const now = this.now();
      this.db
        .prepare(
          `UPDATE operations
              SET status = 'succeeded', lease_expires_at = NULL, updated_at = ?
            WHERE id = ?`,
        )
        .run(now, op.id);
      this.recordAttempt(op.id, op.attempts + 1, 'noop', 'settled by converge', 0, now);
      settled += 1;
    }
    return { checked: rows.length, settled };
  }

  // ── reads ────────────────────────────────────────────────────────────────

  get(kind: string, idempotencyKey: string): Operation | null {
    return this.findByIdentity(kind, idempotencyKey);
  }

  attemptsOf(operationId: number): Attempt[] {
    return this.db
      .prepare<[number], AttemptRow>(
        'SELECT * FROM attempts WHERE operation_id = ? ORDER BY attempt_number',
      )
      .all(operationId)
      .map((r) => ({
        id: r.id,
        operationId: r.operation_id,
        attemptNumber: r.attempt_number,
        outcome: r.outcome,
        error: r.error,
        durationMs: r.duration_ms,
        startedAt: r.started_at,
      }));
  }

  countPending(): number {
    const row = this.db
      .prepare<[], { n: number }>(
        "SELECT COUNT(*) AS n FROM operations WHERE status IN ('pending','running')",
      )
      .get();
    return row?.n ?? 0;
  }

  // ── internals ────────────────────────────────────────────────────────────

  private findByIdentity(kind: string, key: string): Operation | null {
    const row = this.db
      .prepare<[string, string], OperationRow>(
        'SELECT * FROM operations WHERE kind = ? AND idempotency_key = ?',
      )
      .get(kind, key);
    return row ? toOperation(row) : null;
  }

  private getRow(id: number): OperationRow | undefined {
    return this.db
      .prepare<[number], OperationRow>('SELECT * FROM operations WHERE id = ?')
      .get(id);
  }

  private earliestNextAttempt(): number | null {
    const row = this.db
      .prepare<[], { t: number | null }>(
        "SELECT MIN(next_attempt_at) AS t FROM operations WHERE status = 'pending'",
      )
      .get();
    return row?.t ?? null;
  }

  /**
   * Abandon operations whose deadline passed.
   *
   * Checked before claiming, so an expired operation is never handed to a
   * worker: doing the work and then discovering it was too late is the worst
   * of both outcomes — the effect happened, and the record says it did not.
   */
  private expireOverdue(): number {
    const now = this.now();
    return this.db
      .prepare(
        `UPDATE operations
            SET status = 'expired', lease_expires_at = NULL, updated_at = ?,
                last_error = COALESCE(last_error, 'deadline passed before completion')
          WHERE status IN ('pending', 'running')
            AND expires_at IS NOT NULL AND expires_at <= ?`,
      )
      .run(now, now).changes;
  }

  /**
   * Take ownership of due operations.
   *
   * The claim is a single UPDATE that flips status and stamps a lease, so two
   * workers cannot both take the same row. The lease is what makes a crashed
   * worker recoverable: its rows become claimable again once the lease lapses,
   * with no operator involvement and no separate reaper process.
   */
  private claim(limit: number): Operation[] {
    const now = this.now();
    const claimed = this.db.transaction((n: number): OperationRow[] => {
      const due = this.db
        .prepare<[number, number, number], { id: number }>(
          `SELECT id FROM operations
            WHERE (status = 'pending' AND next_attempt_at <= ?)
               OR (status = 'running' AND lease_expires_at IS NOT NULL
                   AND lease_expires_at <= ?)
            ORDER BY next_attempt_at
            LIMIT ?`,
        )
        .all(now, now, n);

      const out: OperationRow[] = [];
      const take = this.db.prepare(
        `UPDATE operations
            SET status = 'running', lease_expires_at = ?, updated_at = ?
          WHERE id = ? AND status IN ('pending', 'running')`,
      );
      for (const { id } of due) {
        if (take.run(now + this.leaseMs, now, id).changes === 1) {
          const row = this.getRow(id);
          if (row) out.push(row);
        }
      }
      return out;
    })(limit);

    return claimed.map(toOperation);
  }

  private async execute(op: Operation): Promise<AttemptOutcome> {
    const handler = this.handlers.get(op.kind);
    const attemptNumber = op.attempts + 1;
    const startedAt = this.now();

    if (!handler) {
      // No handler is a deployment mistake, not a transient fault. Retrying
      // cannot fix it, and burning the attempt budget hides it.
      this.settleFailed(op, attemptNumber, `no handler registered for kind "${op.kind}"`, startedAt);
      return 'permanent';
    }

    try {
      const result = await handler(op.payload, {
        operationId: op.id,
        idempotencyKey: op.idempotencyKey,
        attemptNumber,
      });

      // Validate what came back. An adversarial probe found that a handler
      // returning a typo'd outcome was recorded as a success, with the
      // nonsense value written into the attempts table -- so the audit trail
      // this queue exists to keep became quietly untrustworthy, and nothing
      // reported it. Types do not help here: the handler may be JavaScript,
      // or the value may come from a network reply.
      // Widened deliberately: the declared type says this cannot happen, and
      // at runtime it can -- the handler may be JavaScript, or the value may
      // have come off a network reply.
      const outcome = (result as { outcome?: unknown } | null | undefined)?.outcome;
      if (outcome !== 'applied' && outcome !== 'noop') {
        throw new PermanentFailure(
          `handler for "${op.kind}" returned an invalid outcome: ` +
          `${JSON.stringify(outcome)} (expected "applied" or "noop")`,
        );
      }

      const now = this.now();
      this.db
        .prepare(
          `UPDATE operations
              SET status = 'succeeded', attempts = ?, lease_expires_at = NULL,
                  last_error = NULL, updated_at = ?
            WHERE id = ?`,
        )
        .run(attemptNumber, now, op.id);
      this.recordAttempt(
        op.id, attemptNumber, result.outcome, result.detail ?? null,
        now - startedAt, startedAt,
      );
      return result.outcome;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);

      if (err instanceof PermanentFailure) {
        this.settleFailed(op, attemptNumber, message, startedAt);
        return 'permanent';
      }

      if (attemptNumber >= op.maxAttempts) {
        this.settleFailed(op, attemptNumber, message, startedAt, 'retryable');
        return 'permanent';
      }

      const now = this.now();
      this.db
        .prepare(
          `UPDATE operations
              SET status = 'pending', attempts = ?, lease_expires_at = NULL,
                  next_attempt_at = ?, last_error = ?, updated_at = ?
            WHERE id = ?`,
        )
        .run(attemptNumber, now + this.backoffFor(attemptNumber), message, now, op.id);
      this.recordAttempt(op.id, attemptNumber, 'retryable', message, now - startedAt, startedAt);
      return 'retryable';
    }
  }

  private settleFailed(
    op: Operation,
    attemptNumber: number,
    message: string,
    startedAt: number,
    outcome: AttemptOutcome = 'permanent',
  ): void {
    const now = this.now();
    this.db
      .prepare(
        `UPDATE operations
            SET status = 'failed', attempts = ?, lease_expires_at = NULL,
                last_error = ?, updated_at = ?
          WHERE id = ?`,
      )
      .run(attemptNumber, message, now, op.id);
    this.recordAttempt(op.id, attemptNumber, outcome, message, now - startedAt, startedAt);
  }

  private recordAttempt(
    operationId: number,
    attemptNumber: number,
    outcome: AttemptOutcome,
    error: string | null,
    durationMs: number,
    startedAt: number,
  ): void {
    this.db
      .prepare(
        `INSERT INTO attempts
           (operation_id, attempt_number, outcome, error, duration_ms, started_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(operationId, attemptNumber, outcome, error, durationMs, startedAt);
  }

  /**
   * Exponential backoff, capped.
   *
   * Uncapped doubling reaches days by the tenth attempt, which in practice
   * means the operation never runs again and nobody notices. The cap keeps a
   * long-lived retry loop polling at a rate a recovering provider can absorb.
   */
  private backoffFor(attemptNumber: number): number {
    return Math.min(this.backoffBaseMs * 2 ** (attemptNumber - 1), this.backoffCapMs);
  }
}

function toOperation(row: OperationRow): Operation {
  return {
    id: row.id,
    idempotencyKey: row.idempotency_key,
    kind: row.kind,
    payload: JSON.parse(row.payload) as unknown,
    status: row.status,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    nextAttemptAt: row.next_attempt_at,
    expiresAt: row.expires_at,
    leaseExpiresAt: row.lease_expires_at,
    lastError: row.last_error,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}
