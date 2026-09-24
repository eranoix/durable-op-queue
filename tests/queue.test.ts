import { afterEach, describe, expect, it } from 'vitest';
import { PermanentFailure } from '../src/index.js';
import { makeQueue } from './helpers.js';

let close: (() => void) | null = null;
afterEach(() => {
  close?.();
  close = null;
});

function setup(opts = {}) {
  const h = makeQueue(opts);
  close = () => h.queue.close();
  return h;
}

describe('identity', () => {
  it('absorbs a repeated submit instead of queueing it twice', () => {
    const { queue } = setup();
    const a = queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', payload: { cents: 500 } });
    const b = queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', payload: { cents: 500 } });

    expect(a.created).toBe(true);
    expect(b.created).toBe(false);
    expect(b.operation.id).toBe(a.operation.id);
  });

  it('keeps the payload of the operation that already exists', () => {
    // Two callers claiming the same identity disagree about what the operation
    // is. The one already scheduled may be mid-flight, so adopting the second
    // payload would change work in progress.
    const { queue } = setup();
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', payload: { cents: 500 } });
    const second = queue.submit({
      idempotencyKey: 'inv-1', kind: 'charge', payload: { cents: 999_999 },
    });

    expect(second.created).toBe(false);
    expect(second.operation.payload).toEqual({ cents: 500 });
  });

  it('treats the same key under a different kind as a different operation', () => {
    const { queue } = setup();
    const a = queue.submit({ idempotencyKey: 'x', kind: 'charge' });
    const b = queue.submit({ idempotencyKey: 'x', kind: 'refund' });

    expect(b.created).toBe(true);
    expect(b.operation.id).not.toBe(a.operation.id);
  });
});

describe('execution', () => {
  it('runs a handler once and records the applied attempt', async () => {
    const { queue } = setup();
    let calls = 0;
    queue.register('charge', async () => {
      calls += 1;
      return { outcome: 'applied' };
    });
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge' });

    const first = await queue.runOnce();
    const second = await queue.runOnce();

    expect(calls).toBe(1);
    expect(first.applied).toBe(1);
    // A succeeded operation is not claimable again, so a second pass is a
    // no-op. Without that, every scheduler tick would re-run finished work.
    expect(second.claimed).toBe(0);
    expect(queue.get('charge', 'inv-1')?.status).toBe('succeeded');
  });

  it('distinguishes noop from applied and still settles as done', async () => {
    // The crash-window case: the effect landed, our row did not hear about it.
    // The retry finds the work present. That is success, and it must not be
    // recorded as a second application.
    const { queue } = setup();
    queue.register('folder', async () => ({ outcome: 'noop', detail: 'already existed' }));
    const { operation } = queue.submit({ idempotencyKey: 'f-1', kind: 'folder' });

    const summary = await queue.runOnce();

    expect(summary.noop).toBe(1);
    expect(summary.applied).toBe(0);
    expect(queue.get('folder', 'f-1')?.status).toBe('succeeded');
    expect(queue.attemptsOf(operation.id).at(-1)?.outcome).toBe('noop');
  });
});

describe('retry and backoff', () => {
  it('backs off exponentially rather than hammering a failing provider', async () => {
    const { queue, now } = setup({ backoffBaseMs: 1_000 });
    queue.register('charge', async () => {
      throw new Error('503 from provider');
    });
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', maxAttempts: 4 });

    await queue.runOnce();
    const after1 = queue.get('charge', 'inv-1')!;
    expect(after1.status).toBe('pending');
    expect(after1.nextAttemptAt - now()).toBe(1_000);
  });

  it('does not run an operation before its backoff has elapsed', async () => {
    const { queue, advance } = setup({ backoffBaseMs: 1_000 });
    let calls = 0;
    queue.register('charge', async () => {
      calls += 1;
      throw new Error('timeout');
    });
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', maxAttempts: 5 });

    await queue.runOnce();
    expect(calls).toBe(1);

    await queue.runOnce(); // still inside the backoff window
    expect(calls).toBe(1);

    advance(1_000);
    await queue.runOnce();
    expect(calls).toBe(2);
  });

  it('caps the backoff so a long retry loop keeps polling', async () => {
    const { queue, now, advance } = setup({ backoffBaseMs: 1_000, backoffCapMs: 4_000 });
    queue.register('charge', async () => {
      throw new Error('still down');
    });
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', maxAttempts: 10 });

    const gaps: number[] = [];
    for (let i = 0; i < 5; i += 1) {
      await queue.runOnce();
      const op = queue.get('charge', 'inv-1')!;
      gaps.push(op.nextAttemptAt - now());
      advance(op.nextAttemptAt - now());
    }

    expect(gaps).toEqual([1_000, 2_000, 4_000, 4_000, 4_000]);
  });

  it('gives up once the attempt budget is spent', async () => {
    const { queue, advance } = setup();
    queue.register('charge', async () => {
      throw new Error('nope');
    });
    const { operation } = queue.submit({
      idempotencyKey: 'inv-1', kind: 'charge', maxAttempts: 3,
    });

    await queue.drain(advance);

    const op = queue.get('charge', 'inv-1')!;
    expect(op.status).toBe('failed');
    expect(op.attempts).toBe(3);
    expect(queue.attemptsOf(operation.id)).toHaveLength(3);
  });

  it('stops immediately on a permanent failure without spending the budget', async () => {
    // Retrying a validation rejection wastes attempts and delays everything
    // queued behind it, and the answer never changes.
    const { queue, advance } = setup();
    let calls = 0;
    queue.register('charge', async () => {
      calls += 1;
      throw new PermanentFailure('card closed');
    });
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', maxAttempts: 8 });

    await queue.drain(advance);

    expect(calls).toBe(1);
    const op = queue.get('charge', 'inv-1')!;
    expect(op.status).toBe('failed');
    expect(op.lastError).toBe('card closed');
  });

  it('refuses to retry a kind with no handler', async () => {
    const { queue, advance } = setup();
    queue.submit({ idempotencyKey: 'x', kind: 'unregistered', maxAttempts: 5 });

    await queue.drain(advance);

    const op = queue.get('unregistered', 'x')!;
    expect(op.status).toBe('failed');
    expect(op.attempts).toBe(1);
    expect(op.lastError).toMatch(/no handler registered/);
  });
});

describe('attempt history', () => {
  it('keeps every attempt rather than overwriting the last one', async () => {
    // "Failed four times then worked" and "worked" are different facts, and
    // only the first says the provider is unwell.
    const { queue, advance } = setup();
    let calls = 0;
    queue.register('charge', async () => {
      calls += 1;
      if (calls < 3) throw new Error(`attempt ${calls} failed`);
      return { outcome: 'applied' };
    });
    const { operation } = queue.submit({ idempotencyKey: 'inv-1', kind: 'charge' });

    await queue.drain(advance);

    const attempts = queue.attemptsOf(operation.id);
    expect(attempts.map((a) => a.outcome)).toEqual(['retryable', 'retryable', 'applied']);
    expect(attempts.map((a) => a.attemptNumber)).toEqual([1, 2, 3]);
    expect(queue.get('charge', 'inv-1')?.status).toBe('succeeded');
  });
});

describe('expiry', () => {
  it('abandons an operation whose deadline passed', async () => {
    const { queue, advance } = setup();
    queue.register('charge', async () => ({ outcome: 'applied' }));
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', ttlMs: 5_000, delayMs: 10_000 });

    advance(6_000);
    const summary = await queue.runOnce();

    expect(summary.expired).toBe(1);
    expect(queue.get('charge', 'inv-1')?.status).toBe('expired');
  });

  it('never hands an expired operation to a handler', async () => {
    // Doing the work and only then noticing it was too late is the worst
    // outcome available: the effect happened and the record denies it.
    const { queue, advance } = setup();
    let calls = 0;
    queue.register('charge', async () => {
      calls += 1;
      return { outcome: 'applied' };
    });
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', ttlMs: 1_000 });

    advance(2_000);
    await queue.runOnce();

    expect(calls).toBe(0);
    expect(queue.get('charge', 'inv-1')?.status).toBe('expired');
  });
});

describe('leases', () => {
  it('reclaims work from a worker that died mid-flight', async () => {
    // Simulates a crash by claiming and never settling: the row stays
    // `running` with a lease. Once the lease lapses it becomes claimable
    // again, with no reaper process and no operator involvement.
    const { queue, advance } = setup({ leaseMs: 10_000 });
    let calls = 0;
    let hang = true;
    queue.register('charge', async () => {
      calls += 1;
      if (hang) throw new Error('worker died');
      return { outcome: 'applied' };
    });
    queue.submit({ idempotencyKey: 'inv-1', kind: 'charge', maxAttempts: 5 });

    await queue.runOnce();
    expect(calls).toBe(1);

    hang = false;
    advance(20_000);
    await queue.runOnce();

    expect(calls).toBe(2);
    expect(queue.get('charge', 'inv-1')?.status).toBe('succeeded');
  });
});

describe('converge', () => {
  it('settles an operation the provider already completed', async () => {
    // The torn-write case. Our record says failed; the provider says done.
    // The provider is the authority on its own state.
    const { queue, advance } = setup();
    queue.register('folder', async () => {
      throw new PermanentFailure('connection reset after the write landed');
    });
    queue.submit({ idempotencyKey: 'f-1', kind: 'folder' });
    await queue.drain(advance);
    expect(queue.get('folder', 'f-1')?.status).toBe('failed');

    const result = await queue.converge('folder', async () => true);

    expect(result.settled).toBe(1);
    expect(queue.get('folder', 'f-1')?.status).toBe('succeeded');
  });

  it('leaves alone what the provider does not have', async () => {
    const { queue, advance } = setup();
    queue.register('folder', async () => {
      throw new PermanentFailure('rejected');
    });
    queue.submit({ idempotencyKey: 'f-1', kind: 'folder' });
    await queue.drain(advance);

    const result = await queue.converge('folder', async () => false);

    expect(result.settled).toBe(0);
    expect(queue.get('folder', 'f-1')?.status).toBe('failed');
  });

  it('records the settlement as a noop attempt, not an application', async () => {
    const { queue, advance } = setup();
    queue.register('folder', async () => {
      throw new PermanentFailure('lost the response');
    });
    const { operation } = queue.submit({ idempotencyKey: 'f-1', kind: 'folder' });
    await queue.drain(advance);

    await queue.converge('folder', async () => true);

    const last = queue.attemptsOf(operation.id).at(-1);
    expect(last?.outcome).toBe('noop');
    expect(last?.error).toBe('settled by converge');
  });
});

describe('batching', () => {
  it('honours the per-pass limit so a backlog drains in bounded chunks', async () => {
    const { queue } = setup();
    queue.register('charge', async () => ({ outcome: 'applied' }));
    for (let i = 0; i < 10; i += 1) {
      queue.submit({ idempotencyKey: `inv-${i}`, kind: 'charge' });
    }

    const first = await queue.runOnce(4);
    expect(first.claimed).toBe(4);
    expect(queue.countPending()).toBe(6);
  });
});

describe('regressions', () => {
  it('refuses a handler that returns an invalid outcome', async () => {
    // Found by an adversarial probe: a typo'd outcome was recorded as success
    // and written verbatim into the attempts table, so the audit trail this
    // queue exists to keep became untrustworthy with nothing reporting it.
    const { queue, advance } = setup();
    queue.register('charge', async () => ({ outcome: 'nonsense' } as never));
    const { operation } = queue.submit({ idempotencyKey: 'x', kind: 'charge' });

    await queue.drain(advance);

    const op = queue.get('charge', 'x')!;
    expect(op.status).toBe('failed');
    expect(op.lastError).toMatch(/invalid outcome/);
    // And it is permanent: a handler with a typo will not fix itself on retry.
    expect(queue.attemptsOf(operation.id)).toHaveLength(1);
  });
});
