import assert from 'node:assert/strict';
import diagnosticsChannel from 'node:diagnostics_channel';

import {
  AbortedGraphQLExecutionError,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLSchema,
  GraphQLString,
  experimentalExecuteIncrementally,
  parse,
  subscribe,
} from 'graphql';

function createControllableAsyncIterator() {
  let pendingNext;
  let nextWaiters = [];
  let nextIsPending = false;
  let returnCallCount = 0;
  let returnStarted = false;
  let resolveReturn;
  const returnPromise = new Promise((resolve) => {
    resolveReturn = resolve;
  });

  const waitForNext = () => {
    if (nextIsPending) return Promise.resolve();
    return new Promise((resolve) => nextWaiters.push(resolve));
  };

  const iterator = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      nextIsPending = true;
      const waiters = nextWaiters;
      nextWaiters = [];
      waiters.forEach((waiter) => waiter());
      const result = promiseWithResolvers();
      pendingNext = { resolve: result.resolve, reject: result.reject };
      return result.promise;
    },
    return: async () => {
      if (returnStarted) return { value: undefined, done: true };
      returnStarted = true;
      returnCallCount += 1;
      pendingNext?.resolve({ value: undefined, done: true });
      pendingNext = undefined;
      nextIsPending = false;
      await returnPromise;
      return { value: undefined, done: true };
    },
    async throw(error) {
      throw error;
    },
  };

  return {
    iterator,
    waitForNext,
    resolveNext(value) {
      pendingNext?.resolve(value);
      pendingNext = undefined;
      nextIsPending = false;
    },
    resolveReturn() {
      resolveReturn();
    },
    get returnCallCount() {
      return returnCallCount;
    },
  };
}

function promiseWithResolvers() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function collectLifecycle() {
  const events = [];
  const makeHandler = (kind) => ({
    start(context) {
      events.push({ kind, channel: 'start', hasError: context.error !== undefined });
    },
    end(context) {
      events.push({ kind, channel: 'end', hasError: context.error !== undefined });
    },
    asyncStart(context) {
      events.push({ kind, channel: 'asyncStart', hasError: context.error !== undefined });
    },
    asyncEnd(context) {
      events.push({ kind, channel: 'asyncEnd', hasError: context.error !== undefined });
    },
    error(context) {
      events.push({ kind, channel: 'error', hasError: context.error !== undefined });
    },
  });
  const executeHandler = makeHandler('execute');
  const subscribeHandler = makeHandler('subscribe');
  const executeTracing = diagnosticsChannel.tracingChannel('graphql:execute');
  const subscribeTracing = diagnosticsChannel.tracingChannel('graphql:subscribe');
  executeTracing.subscribe(executeHandler);
  subscribeTracing.subscribe(subscribeHandler);
  return {
    events,
    unsubscribe() {
      executeTracing.unsubscribe(executeHandler);
      subscribeTracing.unsubscribe(subscribeHandler);
    },
  };
}

const itemType = new GraphQLObjectType({
  name: 'NpmCancellationItem',
  fields: () => ({ id: { type: GraphQLString } }),
});
const nestedType = new GraphQLObjectType({
  name: 'NpmCancellationNested',
  fields: () => ({
    nonNullValue: { type: new GraphQLNonNull(GraphQLString) },
    items: { type: new GraphQLList(itemType) },
  }),
});
const deferredType = new GraphQLObjectType({
  name: 'NpmCancellationDeferred',
  fields: () => ({
    value: { type: GraphQLString },
    items: { type: new GraphQLList(itemType) },
    nested: { type: nestedType },
  }),
});
const queryType = new GraphQLObjectType({
  name: 'NpmCancellationQuery',
  fields: () => ({ placeholder: { type: GraphQLString } }),
});
const mutationType = new GraphQLObjectType({
  name: 'NpmCancellationMutation',
  fields: () => ({
    sync: { type: GraphQLString },
    deferred: { type: deferredType },
  }),
});
const subscriptionType = new GraphQLObjectType({
  name: 'NpmCancellationSubscription',
  fields: () => ({ event: { type: GraphQLString } }),
});
const schema = new GraphQLSchema({
  query: queryType,
  mutation: mutationType,
  subscription: subscriptionType,
});

async function rejectError(promise) {
  try {
    const value = await promise;
    throw new Error(`Expected rejection but got ${JSON.stringify(value)}`);
  } catch (error) {
    return error;
  }
}

async function normalIncrementalCompletion() {
  const fixture = createControllableAsyncIterator();
  const lifecycle = collectLifecycle();
  try {
    const deferred = promiseWithResolvers();
    const syncField = promiseWithResolvers();
    const resultPromise = experimentalExecuteIncrementally({
      schema,
      document: parse(`
        mutation {
          sync
          ... @defer {
            deferred {
              value
              items @stream(initialCount: 0) { id }
            }
          }
        }
      `),
      rootValue: {
        sync: () => syncField.promise,
        deferred: () => ({
          value: () => deferred.promise,
          items: () => fixture.iterator,
        }),
      },
      enableEarlyExecution: true,
    });

    const firstNext = fixture.waitForNext();
    deferred.resolve('value');
    syncField.resolve('sync-done');
    const result = await resultPromise;
    assert.equal('initialResult' in result, true);

    await firstNext;
    fixture.resolveNext({ value: { id: '0' }, done: false });
    const patchPromise = result.subsequentResults.next();
    const secondNext = fixture.waitForNext();
    await patchPromise;
    await secondNext;
    fixture.resolveNext({ value: undefined, done: true });
    fixture.resolveReturn();
    const finalPatch = await result.subsequentResults.next();
    assert.equal(finalPatch.done, false);
    assert.equal(finalPatch.value.hasNext, false);

    const executeEvents = lifecycle.events
      .filter((e) => e.kind === 'execute')
      .map((e) => e.channel);
    assert.deepEqual(executeEvents, ['start', 'end', 'asyncStart', 'asyncEnd']);
    assert.equal(fixture.returnCallCount, 0);
  } finally {
    lifecycle.unsubscribe();
  }
}

async function userCancellation() {
  const fixture = createControllableAsyncIterator();
  const lifecycle = collectLifecycle();
  try {
    const reason = new Error('Client disconnected');
    const abortController = new AbortController();
    const syncField = promiseWithResolvers();
    const deferred = promiseWithResolvers();
    const resultPromise = experimentalExecuteIncrementally({
      schema,
      document: parse(`
        mutation {
          sync
          ... @defer {
            deferred {
              value
              items @stream(initialCount: 0) { id }
            }
          }
        }
      `),
      rootValue: {
        sync: () => syncField.promise,
        deferred: () => ({
          value: () => deferred.promise,
          items: () => fixture.iterator,
        }),
      },
      abortSignal: abortController.signal,
      enableEarlyExecution: true,
    });

    const firstNext = fixture.waitForNext();
    deferred.resolve('value');
    await firstNext;
    abortController.abort(reason);

    const error = await rejectError(resultPromise);
    assert.equal(error instanceof AbortedGraphQLExecutionError, true);
    assert.equal(error.cause, reason);

    const executeEvents = lifecycle.events
      .filter((e) => e.kind === 'execute')
      .map((e) => e.channel);
    assert.deepEqual(executeEvents, [
      'start',
      'end',
      'error',
      'asyncStart',
      'asyncEnd',
    ]);
    const errorEvent = lifecycle.events.find(
      (e) => e.kind === 'execute' && e.channel === 'error',
    );
    assert.equal(errorEvent.hasError, true);
    assert.equal(fixture.returnCallCount, 1);
    fixture.resolveReturn();

    syncField.resolve('late');
    fixture.resolveNext({ value: { id: 'late' }, done: false });
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(
      lifecycle.events.filter((e) => e.kind === 'execute').length,
      5,
    );
  } finally {
    lifecycle.unsubscribe();
  }
}

async function resolverFailure() {
  const fixture = createControllableAsyncIterator();
  const lifecycle = collectLifecycle();
  try {
    const deferred = promiseWithResolvers();
    const syncField = promiseWithResolvers();
    const resultPromise = experimentalExecuteIncrementally({
      schema,
      document: parse(`
        mutation {
          sync
          ... @defer {
            deferred {
              nested {
                nonNullValue
                items @stream(initialCount: 0) { id }
              }
            }
          }
        }
      `),
      rootValue: {
        sync: () => syncField.promise,
        deferred: () => ({
          nested: () => ({
            nonNullValue: () => deferred.promise,
            items: () => fixture.iterator,
          }),
        }),
      },
      enableEarlyExecution: true,
    });

    const firstNext = fixture.waitForNext();
    syncField.resolve('sync-done');
    const result = await resultPromise;
    assert.equal('initialResult' in result, true);

    await firstNext;
    const nextPromise = result.subsequentResults.next();
    deferred.reject(new Error('Resolver failed'));
    const patch = await nextPromise;
    assert.equal(patch.done, false);
    const failedEntry = patch.value.incremental?.find(
      (entry) => entry.errors !== undefined,
    );
    assert.notEqual(failedEntry, undefined);
    assert.deepEqual(
      { ...failedEntry.data, deferred: { ...failedEntry.data.deferred } },
      { deferred: { nested: null } },
    );
    assert.equal(fixture.returnCallCount, 1);
    fixture.resolveReturn();
    await Promise.resolve();

    const executeEvents = lifecycle.events
      .filter((e) => e.kind === 'execute')
      .map((e) => e.channel);
    assert.deepEqual(executeEvents, ['start', 'end', 'asyncStart', 'asyncEnd']);
  } finally {
    lifecycle.unsubscribe();
  }
}

async function subscriptionNormalReturn() {
  const fixture = createControllableAsyncIterator();
  const lifecycle = collectLifecycle();
  try {
    const sourcePromise = Promise.resolve().then(() => fixture.iterator);
    const result = await subscribe({
      schema,
      document: parse('subscription { event }'),
      rootValue: { event: () => sourcePromise },
    });
    assert.equal(typeof result.next, 'function');

    const nextPromise = result.next();
    await fixture.waitForNext();
    fixture.resolveNext({ value: { event: 'one' }, done: false });
    const delivered = await nextPromise;
    assert.equal(delivered.done, false);

    const returned = result.return();
    assert.equal(fixture.returnCallCount, 1);
    fixture.resolveReturn();
    await returned;

    const subscribeEvents = lifecycle.events
      .filter((e) => e.kind === 'subscribe')
      .map((e) => e.channel);
    assert.deepEqual(subscribeEvents, [
      'start',
      'end',
      'asyncStart',
      'asyncEnd',
    ]);
  } finally {
    lifecycle.unsubscribe();
  }
}

async function subscriptionCancellation() {
  const fixture = createControllableAsyncIterator();
  const lifecycle = collectLifecycle();
  try {
    const reason = new Error('Client disconnected');
    const abortController = new AbortController();
    const sourcePromise = Promise.resolve().then(() => fixture.iterator);
    const result = await subscribe({
      schema,
      document: parse('subscription { event }'),
      rootValue: { event: () => sourcePromise },
      abortSignal: abortController.signal,
    });
    assert.equal(typeof result.next, 'function');

    const nextPromise = result.next();
    await fixture.waitForNext();
    abortController.abort(reason);

    const error = await rejectError(nextPromise);
    assert.equal(error instanceof AbortedGraphQLExecutionError, true);
    assert.equal(error.cause, reason);

    const returned = result.return();
    assert.equal(fixture.returnCallCount, 1);
    fixture.resolveReturn();
    await returned;

    const subscribeEvents = lifecycle.events
      .filter((e) => e.kind === 'subscribe')
      .map((e) => e.channel);
    assert.deepEqual(subscribeEvents, [
      'start',
      'end',
      'asyncStart',
      'asyncEnd',
    ]);
  } finally {
    lifecycle.unsubscribe();
  }
}

const unhandledRejections = [];
process.on('unhandledRejection', (reason) => {
  unhandledRejections.push(reason);
});

await normalIncrementalCompletion();
await userCancellation();
await resolverFailure();
await subscriptionNormalReturn();
await subscriptionCancellation();

if (unhandledRejections.length > 0) {
  throw new Error(
    `Unhandled rejection(s): ${unhandledRejections
      .map((r) => (r instanceof Error ? r.message : String(r)))
      .join(', ')}`,
  );
}

console.log('cancellation-npm integration: all scenarios passed');
