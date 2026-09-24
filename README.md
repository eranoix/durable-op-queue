# durable-op-queue

An idempotent, durable operation queue for effects that must happen **exactly
once** against a system you do not control — charge a card, create a remote
folder, send a statement.

```bash
npm install
npm run demo
```

```
invoice    status      attempts  outcomes
─────────────────────────────────────────────────────────────
INV-001   succeeded   1         applied
INV-002   succeeded   3         retryable → retryable → applied
INV-003   failed      1         permanent
INV-004   succeeded   2         retryable → noop

charges actually applied at the provider: 3
```

---

## The problem this exists for

The happy path is never the hard part. The hard part is the window between
*the provider applied the change* and *our database says so*.

A process that dies in that window leaves a completed effect our records
believe is still owed. Retry blindly and you charge twice. Skip the retry and
you lose work silently, and nobody finds out until a reconciliation months
later. Neither is acceptable, and no amount of care in the calling code fixes
it, because the calling code is the thing that died.

So the handler is required to answer a question the queue cannot answer for it:

> Did **I** make this change, or was it already there?

`applied` and `noop` are both success. Recording a `noop` as `applied` claims a
second charge was made. Recording it as a failure retries forever. Keeping them
apart is what makes "exactly once" a fact rather than a hope.

## How the guarantees are held

**Identity lives in the database, not in application code.**
A unique index on `(kind, idempotency_key)` refuses the duplicate. The
alternative — check whether it exists, then insert — is a race wearing a
comfortable disguise: two workers both find nothing, both insert, and the
effect happens twice, reliably, under exactly the load where it hurts most.

**Attempts are rows, not a counter.**
Every try is recorded with its outcome, error and duration, and never
overwrites the one before. "Failed four times and then worked" and "worked"
are different facts, and only the first one tells you the provider is unwell.

**Failure has two kinds and they are not treated alike.**
A timeout backs off and tries again. A `PermanentFailure` — validation
rejected, account closed — stops on the spot. Retrying it burns the attempt
budget, delays everything queued behind it, and the answer never changes.

**Backoff is exponential and capped.**
Uncapped doubling reaches days by the tenth attempt, which in practice means
the operation never runs again and nobody notices.

**A lease, not a reaper.**
Claiming stamps an expiry. A worker that dies holding an operation has its rows
become claimable again once the lease lapses — no separate process to run, and
nothing for an operator to do at 3am.

**Expiry is checked before claiming, never after.**
Doing the work and only then discovering the deadline had passed is the worst
outcome available: the effect happened and the record denies it.

**`converge` is the way back from a torn write.**
Everything above assumes our record of what happened is complete. When it is
not, `converge` asks the provider *does this already exist on your side?* and
settles the row against the answer — recorded as `noop`, because the provider
did the work, not this attempt.

## Using it

```ts
import { OperationQueue, PermanentFailure } from 'durable-op-queue';

const queue = new OperationQueue({ path: './queue.db' });

queue.register('charge', async ({ invoice, cents }, ctx) => {
  // On a retry, ask before acting. This is what lets the handler report
  // `noop` instead of charging twice.
  if (ctx.attemptNumber > 1 && (await provider.hasCharge(invoice))) {
    return { outcome: 'noop', detail: 'already present' };
  }
  const res = await provider.charge(invoice, cents);
  if (res.status === 'declined') throw new PermanentFailure(res.reason);
  return { outcome: 'applied' };
});

// Safe to call again after a timeout: the second call returns the existing
// operation rather than queueing a duplicate.
queue.submit({
  idempotencyKey: invoice,
  kind: 'charge',
  payload: { invoice, cents: 4_200 },
  ttlMs: 24 * 60 * 60 * 1000,
});

setInterval(() => void queue.runOnce(), 1_000);
```

## Tests

```bash
npm test        # 20 tests
npm run typecheck
```

The clock is injected, so backoff is proven exactly rather than waited for: a
five-minute delay is asserted in a millisecond. Each test opens its own
in-memory database, so they cannot interfere and there is nothing to clean up.

## Scope

SQLite via `better-sqlite3` — no service to provision, and the whole thing runs
in one process. The shape (identity index, attempt rows, lease, backoff,
converge pass) ports to Postgres unchanged; `SELECT … FOR UPDATE SKIP LOCKED`
replaces the claim transaction when more than one process is draining.

Node 22+, TypeScript strict, no runtime dependency beyond the driver.

## Languages

TypeScript, 40,911 bytes — 100% of GitHub's language bar.

The SQL is hand-written and real, but it is not a `.sql` file. The DDL lives in
`src/schema.ts` as a template literal: the `operations` and `attempts` tables,
the unique index on `(kind, idempotency_key)` that every guarantee above rests
on, the indexes the claim scan and attempt lookups use, and the
`journal_mode = WAL` and `foreign_keys = ON` pragmas. GitHub attributes it to
the file it sits in.

It is embedded because the build is `tsc` and nothing else. A `.sql` file would
need a copy step into `dist`, and a schema that can go missing because a
packaging step was skipped is a schema that eventually does.
