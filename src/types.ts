/**
 * The vocabulary this queue is built on.
 *
 * Two ideas carry the whole design, and both are about telling apart things
 * that look identical from the outside:
 *
 *   An OPERATION is the intent: "charge this invoice", "create this folder".
 *   It is identified by a key the CALLER chooses, so submitting the same
 *   intent twice produces one operation, not two.
 *
 *   An ATTEMPT is one try at carrying it out. Attempts are recorded
 *   separately and never overwrite each other, because "this failed four
 *   times and then worked" and "this worked" are different facts, and only
 *   the first one tells you the provider is unwell.
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
 * `noop` is the one people leave out, and it is the reason this queue can be
 * trusted after a crash. When a worker dies between "the provider applied the
 * change" and "the row was marked done", the retry finds the work already
 * present. That is not a failure and it is not a fresh success: it is proof
 * the effect happened exactly once. Recording it as `applied` would claim a
 * second charge was made; recording it as `failed` would retry forever.
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
 * What a handler returns.
 *
 * The handler decides `applied` versus `noop`, because only the code talking
 * to the provider can tell "I made this change" from "it was already there".
 * The queue cannot infer it, and guessing would defeat the point.
 */
export type HandlerResult =
  | { outcome: 'applied'; detail?: string }
  | { outcome: 'noop'; detail?: string };

/**
 * Thrown by a handler to say the failure will not resolve on its own.
 *
 * The distinction matters more than it looks. A retryable failure (timeout,
 * 503, connection reset) should back off and try again. A permanent one
 * (validation rejected, account closed, 404 on a resource that will never
 * exist) should stop immediately: retrying it wastes the attempt budget and
 * delays every operation queued behind it, and the outcome never changes.
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
   * Which try this is, starting at 1.
   *
   * Given to the handler because a retry is not the same situation as a first
   * run: on attempt two and beyond it is worth asking the provider whether the
   * work is already there before doing it again.
   */
  attemptNumber: number;
}
