// Child-process fixture for npmDist ESM abort integration tests.
// Run by src/__tests__/npmDist-abort-integration-test.ts after `npm run build:npm`.
import assert from 'node:assert';
import { pathToFileURL } from 'node:url';
import dc from 'node:diagnostics_channel';

const npmDistPath = process.env.GRAPHQL_NPMDIST_PATH;
if (!npmDistPath) {
  console.error('GRAPHQL_NPMDIST_PATH not set');
  process.exit(1);
}

const graphql = await import(pathToFileURL(npmDistPath).href);

const { experimentalExecuteIncrementally, subscribe, buildSchema, parse } = graphql;

const schema = buildSchema(`
  type Query {
    immediate: String
    slow: String
  }

  type Subscription {
    tick: String
  }
`);

function makeCounter(name) {
  const channel = dc.tracingChannel(name);
  const counts = { start: 0, end: 0, asyncStart: 0, asyncEnd: 0, error: 0 };
  const handlers = {};
  for (const sub of ['start', 'end', 'asyncStart', 'asyncEnd', 'error']) {
    handlers[sub] = () => {
      counts[sub]++;
    };
  }
  channel.subscribe(handlers);
  return {
    counts,
    unsubscribe: () => channel.unsubscribe(handlers),
  };
}

function assertSingleTermination(counts, label) {
  assert.strictEqual(counts.start, 1, `${label}: expected 1 start, got ${counts.start}`);
  assert.ok(counts.end <= 1, `${label}: expected at most 1 end, got ${counts.end}`);
  assert.ok(counts.asyncEnd <= 1, `${label}: expected at most 1 asyncEnd, got ${counts.asyncEnd}`);
  assert.ok(counts.error <= 1, `${label}: expected at most 1 error, got ${counts.error}`);
}

const results = {};

// --- Scenario 1: normal incremental completion ---
{
  const tracker = makeCounter('graphql:execute');
  const result = await experimentalExecuteIncrementally({
    schema,
    document: parse('{ immediate slow }'),
    rootValue: {
      immediate: 'now',
      slow: () => Promise.resolve('later'),
    },
  });

  if ('subsequentResults' in result) {
    for await (const _patch of result.subsequentResults) {
      // drain
    }
  }

  assertSingleTermination(tracker.counts, 'normal-incremental');
  results.normal = { ...tracker.counts };
  tracker.unsubscribe();
}

// --- Scenario 2: user cancellation ---
{
  const tracker = makeCounter('graphql:execute');
  const controller = new AbortController();
  let resolveSlow;
  const slowPromise = new Promise((resolve) => {
    resolveSlow = resolve;
  });

  const resultPromise = experimentalExecuteIncrementally({
    schema,
    document: parse('{ slow }'),
    rootValue: { slow: () => slowPromise },
    abortSignal: controller.signal,
  });

  await Promise.resolve();
  await Promise.resolve();

  controller.abort(new Error('client disconnected'));

  let caught;
  try {
    await resultPromise;
  } catch (e) {
    caught = e;
  }
  assert(caught, 'expected abort to reject');
  assert.strictEqual(caught.name, 'AbortedGraphQLExecutionError');

  // Resolve late to verify no unhandled rejection
  resolveSlow('late');
  await Promise.resolve();
  await Promise.resolve();

  assertSingleTermination(tracker.counts, 'user-cancel');
  results.cancel = { ...tracker.counts };
  tracker.unsubscribe();
}

// --- Scenario 3: resolver failure (field error collected, not abrupt) ---
{
  const tracker = makeCounter('graphql:execute');
  const result = await experimentalExecuteIncrementally({
    schema,
    document: parse('{ slow }'),
    rootValue: {
      slow: () => Promise.reject(new Error('resolver failed')),
    },
  });

  if ('initialResult' in result) {
    assert(result.initialResult.errors, 'expected errors in result');
    assert.strictEqual(result.initialResult.errors[0].message, 'resolver failed');
  } else {
    assert(result.errors, 'expected errors in result');
    assert.strictEqual(result.errors[0].message, 'resolver failed');
  }

  // execute channel should still terminate cleanly with no error event
  assertSingleTermination(tracker.counts, 'resolver-failure');
  assert.strictEqual(tracker.counts.error, 0, 'field errors should not fire error channel');
  results.failure = { ...tracker.counts };
  tracker.unsubscribe();
}

// --- Scenario 4: subscription with cancellation ---
{
  const tracker = makeCounter('graphql:subscribe');

  let resolveNext;
  const nextPromise = new Promise((resolve) => {
    resolveNext = resolve;
  });

  let returnCount = 0;
  const iterator = {
    [Symbol.asyncIterator]() { return this; },
    next: () => nextPromise,
    return: () => {
      returnCount++;
      return Promise.resolve({ value: undefined, done: true });
    },
  };

  const controller = new AbortController();
  const result = subscribe({
    schema,
    document: parse('subscription { tick }'),
    rootValue: { tick: () => iterator },
    abortSignal: controller.signal,
  });

  assert(typeof result.next === 'function', 'expected async generator');

  const pending = result.next();
  await Promise.resolve();
  await Promise.resolve();

  controller.abort(new Error('client disconnected'));

  let caught;
  try {
    await pending;
  } catch (e) {
    caught = e;
  }
  assert(caught, 'expected subscription abort to reject pending next');

  // Late event
  resolveNext({ value: { tick: 'late' }, done: false });
  await Promise.resolve();
  await Promise.resolve();

  assert.strictEqual(returnCount, 1, 'source return() should be called exactly once');
  assertSingleTermination(tracker.counts, 'subscription-cancel');
  results.subscription = { ...tracker.counts };
  tracker.unsubscribe();
}

console.log(JSON.stringify({ ok: true, results }));
