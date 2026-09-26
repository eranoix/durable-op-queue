/**
 * An OPERATION is an intent, identified by a caller-chosen key so submitting
 * it twice yields one operation. An ATTEMPT is one try at it; attempts are
 * never overwritten, so a flaky provider stays visible in the history.
 */

/** Where an operation is in its life. */
export type OperationStatus =
  | 'pending' // waiting for its turn, or for its backoff to elapse
  | 'running' // leased by a worker right now
  | 'succeeded'
  | 'failed' // retries exhausted, or refused permanently
  | 'expired'; // deadline passed before it could succeed

/**
 * What one attempt actually did.
 *
 * `noop` means the retry found the effect already applied (e.g. a worker died
 * after the provider call but before the row was updated). Recording it as
 * `applied` would claim a second effect; as `failed`, it would retry forever.
 */
export type AttemptOutcome = 'applied' | 'noop' | 'retryable' | 'permanent';

export interface Operation {
  id: number;
  /** Caller-chosen identity. Unique per kind; this is what makes submit safe to repeat. */
  idempotencyKey: string;
  kind: string;
  payload: unknown;
  status: OperationStatus;
  attempts: number;
  maxAttempts: number;
  /** Not eligible to run before this instant. Carries the backoff. */
  nextAttemptAt: number;
  /** After this instant the operation is abandoned rather than retried. */
  expiresAt: number | null;
  /** Non-null while leased, so a crashed worker's lease can be reclaimed. */
  leaseExpiresAt: number | null;
  lastError: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface Attempt {
  id: number;
  operationId: number;
  attemptNumber: number;
  outcome: AttemptOutcome;
  error: string | null;
  durationMs: number;
  startedAt: number;
}

/**
 * What a handler returns. Only the handler can tell "I made this change"
 * (`applied`) from "it was already there" (`noop`).
 */
export type HandlerResult =
  | { outcome: 'applied'; detail?: string }
  | { outcome: 'noop'; detail?: string };

/**
 * Thrown by a handler when the failure will not resolve on its own (validation
 * rejected, account closed). The operation stops immediately instead of
 * spending its retry budget; any other error is treated as retryable.
 */
export class PermanentFailure extends Error {
  override readonly name = 'PermanentFailure';
}

export interface SubmitOptions {
  idempotencyKey: string;
  kind: string;
  payload?: unknown;
  maxAttempts?: number;
  /** Milliseconds from now, after which the operation is abandoned. */
  ttlMs?: number;
  /** Delay before the first attempt. */
  delayMs?: number;
}

export type Handler = (payload: unknown, ctx: HandlerContext) => Promise<HandlerResult>;

export interface HandlerContext {
  operationId: number;
  idempotencyKey: string;
  /**
   * Which try this is, starting at 1. From attempt 2 on, a handler should check
   * whether the provider already has the work before doing it again.
   */
  attemptNumber: number;
}
