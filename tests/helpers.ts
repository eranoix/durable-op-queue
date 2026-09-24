import { OperationQueue, type QueueOptions } from '../src/index.js';

/**
 * A queue on a controllable clock.
 *
 * Backoff is the behaviour most worth testing and the least worth waiting for.
 * Injecting time means a test can prove a five-minute backoff in a millisecond,
 * and prove it exactly rather than approximately.
 */
export function makeQueue(opts: Partial<QueueOptions> = {}) {
  let clock = 1_700_000_000_000;
  const queue = new OperationQueue({
    now: () => clock,
    backoffBaseMs: 1_000,
    ...opts,
  });
  return {
    queue,
    now: () => clock,
    advance: (ms: number) => {
      clock += ms;
    },
  };
}
