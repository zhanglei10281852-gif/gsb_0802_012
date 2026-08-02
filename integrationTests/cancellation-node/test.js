// TracingChannel is marked experimental in Node's docs but is shipped on
// every runtime graphql-js supports. This test exercises it directly.
/* eslint-disable n/no-unsupported-features/node-builtins */

import assert from 'node:assert/strict';
import dc from 'node:diagnostics_channel';

import {
  AbortedGraphQLExecutionError,
  buildSchema,
  execute,
  experimentalExecuteIncrementally,
  parse,
  subscribe,
} from 'graphql';
import { AbortedGraphQLExecutionError as ExecutionEntryError } from 'graphql/execution';

// The package root and the `graphql/execution` ESM entry points must expose
// the same cancellation primitives.
assert.strictEqual(AbortedGraphQLExecutionError, ExecutionEntryError);

const schema = buildSchema(`
  type Todo {
    id: ID
    author: User
  }

  type User {
    id: ID
  }

  type Query {
    todo: Todo
    scalarList: [String]
    fail: String
  }

  type Subscription {
    counter: Int
  }
`);

/** Yields to the event loop a bounded number of turns, never a real sleep. */
function drainEventLoop() {
  let drained = Promise.resolve();
  for (let i = 0; i < 5; i++) {
    drained = drained.then(
      () =>
        new Promise((resolve) => {
          setImmediate(resolve);
        }),
    );
  }
  return drained;
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Collects the lifecycle event names published on a tracing channel. */
function collectChannelEvents(name) {
  const events = [];
  const channel = dc.tracingChannel(name);
  const handler = {};
  for (const sub of ['start', 'end', 'asyncStart', 'asyncEnd', 'error']) {
    handler[sub] = () => {
      events.push(sub);
    };
  }
  channel.subscribe(handler);
  return {
    events,
    stop: () => channel.unsubscribe(handler),
  };
}

/** An async iterator whose pulls are each settled explicitly by the test. */
function makeControllableStream() {
  const pulls = [];
  const pullWaiters = [];
  let awaitedPulls = 0;
  let returnCount = 0;

  const source = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      const pull = deferred();
      pulls.push(pull);
      const waiter = pullWaiters.shift();
      if (waiter !== undefined) {
        waiter(pull);
      }
      return pull.promise;
    },
    return() {
      returnCount++;
      return Promise.resolve({ value: undefined, done: true });
    },
  };

  return {
    source,
    returnCount: () => returnCount,
    nextPull() {
      const pull = pulls[awaitedPulls++];
      if (pull !== undefined) {
        return Promise.resolve(pull);
      }
      return new Promise((resolve) => {
        pullWaiters.push(resolve);
      });
    },
  };
}

async function rejectsWith(predicate) {
  try {
    await predicate();
  } catch (error) {
    return error;
  }
  assert.fail('Expected the promise to reject.');
}

async function cancelBeforeInitialPayload() {
  const abortController = new AbortController();
  const reason = new Error('client disconnected');
  const todo = deferred();
  const executeEvents = collectChannelEvents('graphql:execute');
  try {
    const resultPromise = experimentalExecuteIncrementally({
      schema,
      document: parse('query Q { todo { id ... @defer { author { id } } } }'),
      rootValue: { todo: () => todo.promise },
      abortSignal: abortController.signal,
      enableEarlyExecution: true,
    });

    abortController.abort(reason);

    const error = await rejectsWith(() => resultPromise);
    assert.ok(error instanceof AbortedGraphQLExecutionError);
    assert.strictEqual(error.cause, reason);

    // Late settlement of in-flight work publishes nothing further.
    todo.resolve({ id: '1' });
    await drainEventLoop();
  } finally {
    executeEvents.stop();
  }
  assert.deepStrictEqual(executeEvents.events, [
    'start',
    'end',
    'error',
    'asyncStart',
    'asyncEnd',
  ]);
  console.log('ok - cancel before the initial payload');
}

async function cancelAfterInitialPayload() {
  const abortController = new AbortController();
  const reason = new Error('client disconnected');
  const stream = makeControllableStream();
  const executeEvents = collectChannelEvents('graphql:execute');
  try {
    const result = await experimentalExecuteIncrementally({
      schema,
      document: parse('{ scalarList @stream(initialCount: 0) }'),
      rootValue: { scalarList: () => stream.source },
      abortSignal: abortController.signal,
    });
    assert.ok('initialResult' in result);
    assert.deepStrictEqual(JSON.parse(JSON.stringify(result.initialResult)), {
      data: { scalarList: [] },
      pending: [{ id: '0', path: ['scalarList'] }],
      hasNext: true,
    });

    const iterator = result.subsequentResults[Symbol.asyncIterator]();
    const nextPromise = iterator.next();
    const pull = await stream.nextPull();
    abortController.abort(reason);

    assert.strictEqual(await rejectsWith(() => nextPromise), reason);

    // Late items are ignored; the stream reports done afterwards.
    pull.resolve({ value: 'late', done: false });
    await drainEventLoop();
    assert.deepStrictEqual(await iterator.next(), {
      value: undefined,
      done: true,
    });
    assert.strictEqual(stream.returnCount(), 1);
  } finally {
    executeEvents.stop();
  }
  // Cancellation after the initial payload adds no further execute events.
  assert.deepStrictEqual(executeEvents.events, [
    'start',
    'end',
    'asyncStart',
    'asyncEnd',
  ]);
  console.log('ok - cancel after the initial payload');
}

async function completeIncrementally() {
  const stream = makeControllableStream();
  const executeEvents = collectChannelEvents('graphql:execute');
  try {
    const result = await experimentalExecuteIncrementally({
      schema,
      document: parse('{ scalarList @stream(initialCount: 0) }'),
      rootValue: { scalarList: () => stream.source },
    });
    assert.ok('initialResult' in result);

    const delivered = [];
    const consume = (async () => {
      for await (const patch of result.subsequentResults) {
        delivered.push(JSON.parse(JSON.stringify(patch)));
      }
    })();

    (await stream.nextPull()).resolve({ value: 'a', done: false });
    // Let the item patch flush before completing the source.
    await drainEventLoop();
    (await stream.nextPull()).resolve({ value: undefined, done: true });
    await consume;
    await drainEventLoop();

    assert.deepStrictEqual(delivered, [
      { hasNext: true, incremental: [{ id: '0', items: ['a'] }] },
      { hasNext: false, completed: [{ id: '0' }] },
    ]);
    // Natural completion needs no early close of the source iterator.
    assert.strictEqual(stream.returnCount(), 0);
  } finally {
    executeEvents.stop();
  }
  assert.deepStrictEqual(executeEvents.events, [
    'start',
    'end',
    'asyncStart',
    'asyncEnd',
  ]);
  console.log('ok - incremental query completes fully');
}

async function resolverFailure() {
  const failure = new Error('async-boom');
  const executeEvents = collectChannelEvents('graphql:execute');
  const resolveEvents = collectChannelEvents('graphql:resolve');
  try {
    const result = await execute({
      schema,
      document: parse('{ fail }'),
      rootValue: { fail: () => Promise.reject(failure) },
    });
    assert.deepStrictEqual(JSON.parse(JSON.stringify(result)), {
      data: { fail: null },
      errors: [
        {
          message: 'async-boom',
          locations: [{ line: 1, column: 3 }],
          path: ['fail'],
        },
      ],
    });
    await drainEventLoop();
  } finally {
    executeEvents.stop();
    resolveEvents.stop();
  }
  // A resolver failure is formatted into the result: the execute lifecycle
  // completes normally once, while resolve publishes one error lifecycle.
  assert.deepStrictEqual(executeEvents.events, [
    'start',
    'end',
    'asyncStart',
    'asyncEnd',
  ]);
  assert.deepStrictEqual(resolveEvents.events, [
    'start',
    'end',
    'error',
    'asyncStart',
    'asyncEnd',
  ]);
  console.log('ok - resolver failure completes lifecycles exactly once');
}

async function cancelSubscription() {
  const abortController = new AbortController();
  const reason = new Error('client disconnected');
  const pending = deferred();
  let nextCount = 0;
  let returnCount = 0;
  const source = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      nextCount++;
      if (nextCount === 1) {
        return Promise.resolve({ value: { counter: 1 }, done: false });
      }
      return pending.promise;
    },
    return() {
      returnCount++;
      // Pubsub-style cleanup releases the pending pull.
      pending.resolve({ value: undefined, done: true });
      return Promise.resolve({ value: undefined, done: true });
    },
  };
  const subscribeEvents = collectChannelEvents('graphql:subscribe');
  try {
    const stream = await subscribe({
      schema,
      document: parse('subscription { counter }'),
      rootValue: { counter: () => Promise.resolve(source) },
      abortSignal: abortController.signal,
    });
    assert.ok('next' in stream);

    const first = await stream.next();
    assert.deepStrictEqual(JSON.parse(JSON.stringify(first)), {
      value: { data: { counter: 1 } },
      done: false,
    });

    const nextPromise = stream.next();
    abortController.abort(reason);
    assert.strictEqual(await rejectsWith(() => nextPromise), reason);

    // Closing the response stream closes the source stream exactly once.
    await stream.return();
    await stream.return();
    assert.strictEqual(returnCount, 1);

    // Cancellation is sticky: later next() calls reject instead of hanging.
    assert.strictEqual(await rejectsWith(() => stream.next()), reason);

    await drainEventLoop();
  } finally {
    subscribeEvents.stop();
  }
  assert.deepStrictEqual(subscribeEvents.events, [
    'start',
    'end',
    'asyncStart',
    'asyncEnd',
  ]);
  console.log('ok - cancel during subscription consumption');
}

async function preAbortedExecute() {
  const abortController = new AbortController();
  const reason = new Error('client disconnected');
  abortController.abort(reason);
  const executeEvents = collectChannelEvents('graphql:execute');
  try {
    let resolverCalled = false;
    let thrown;
    try {
      execute({
        schema,
        document: parse('{ todo { id } }'),
        rootValue: {
          todo: () => {
            resolverCalled = true;
            return { id: '1' };
          },
        },
        abortSignal: abortController.signal,
      });
    } catch (error) {
      thrown = error;
    }
    assert.strictEqual(thrown, reason);
    assert.strictEqual(resolverCalled, false);
    await drainEventLoop();
  } finally {
    executeEvents.stop();
  }
  assert.deepStrictEqual(executeEvents.events, ['start', 'error', 'end']);
  console.log('ok - execute with a pre-aborted signal');
}

async function main() {
  await cancelBeforeInitialPayload();
  await cancelAfterInitialPayload();
  await completeIncrementally();
  await resolverFailure();
  await cancelSubscription();
  await preAbortedExecute();
}

main();
