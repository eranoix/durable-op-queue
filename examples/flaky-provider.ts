/**
 * A provider that behaves the way real ones do, so the queue's answer to each
 * behaviour is visible rather than described.
 *
 *   npm run demo
 *
 * Four invoices go in. The provider times out, succeeds, rejects permanently,
 * and — the interesting one — applies a change and then drops the connection
 * before answering, leaving the caller unable to tell whether it worked.
 */

import { OperationQueue, PermanentFailure } from '../src/index.js';

const charged = new Set<string>();
let clock = Date.now();

const queue = new OperationQueue({
  now: () => clock,
  backoffBaseMs: 500,
  backoffCapMs: 4_000,
});

let timeouts = 0;

queue.register('charge', async (payload, ctx) => {
  const { invoice } = payload as { invoice: string };

  // On a retry, ask first. This is the discipline the whole design depends on:
  // a handler that checks before acting can report `noop` instead of charging
  // a second time.
  if (ctx.attemptNumber > 1 && charged.has(invoice)) {
    return { outcome: 'noop', detail: 'provider already had it' };
  }

  if (invoice === 'INV-002') {
    timeouts += 1;
    if (timeouts <= 2) throw new Error('gateway timeout');
  }

  if (invoice === 'INV-003') {
    throw new PermanentFailure('card declined: account closed');
  }

  charged.add(invoice);

  if (invoice === 'INV-004' && ctx.attemptNumber === 1) {
    // Applied on their side, lost on ours. The next attempt will see it.
    throw new Error('connection reset after the charge landed');
  }

  return { outcome: 'applied', detail: `charged ${invoice}` };
});

for (const invoice of ['INV-001', 'INV-002', 'INV-003', 'INV-004']) {
  queue.submit({ idempotencyKey: invoice, kind: 'charge', payload: { invoice }, maxAttempts: 5 });
}

// Submitting the same intent again, the way a caller retrying a timed-out API
// call would. It must not become a second charge.
const again = queue.submit({
  idempotencyKey: 'INV-001', kind: 'charge', payload: { invoice: 'INV-001' },
});
console.log(`resubmit of INV-001 created a new operation: ${again.created}\n`);

await queue.drain((ms) => {
  clock += ms;
});

console.log('invoice    status      attempts  outcomes');
console.log('─────────────────────────────────────────────────────────────');
for (const invoice of ['INV-001', 'INV-002', 'INV-003', 'INV-004']) {
  const op = queue.get('charge', invoice);
  if (!op) continue;
  const outcomes = queue.attemptsOf(op.id).map((a) => a.outcome).join(' → ');
  console.log(
    `${invoice}   ${op.status.padEnd(11)} ${String(op.attempts).padEnd(9)} ${outcomes}`,
  );
}

console.log(`\ncharges actually applied at the provider: ${charged.size}`);
console.log('INV-003 never reached it; the other three were charged once each.');
queue.close();
