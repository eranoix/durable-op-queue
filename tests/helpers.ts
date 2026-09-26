import { OperationQueue, type QueueOptions } from '../src/index.js';

/** A queue on a controllable clock, so backoff is tested exactly without waiting. */
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
