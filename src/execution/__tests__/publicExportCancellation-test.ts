import { afterEach, describe, it } from 'node:test';

import { expect } from 'chai';

import { getTracingChannel } from '../../__testUtils__/getTracingChannel.ts';
import { resolveOnNextTick } from '../../__testUtils__/resolveOnNextTick.ts';

import { promiseWithResolvers } from '../../jsutils/promiseWithResolvers.ts';

import { parse } from '../../language/parser.ts';

import {
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
} from '../../type/definition.ts';
import { GraphQLString } from '../../type/scalars.ts';
import { GraphQLSchema } from '../../type/schema.ts';

// Drive the public root package entry point, not internal modules, to confirm
// cancellation works through the published surface.
import {
  AbortedGraphQLExecutionError,
  experimentalExecuteIncrementally,
  subscribe,
} from '../../index.ts';
import type { ExecutionResult } from '../Executor.ts';

interface LifecycleEvent {
  channel: 'start' | 'end' | 'asyncStart' | 'asyncEnd' | 'error';
  kind: 'execute' | 'subscribe';
  hasError: boolean;
  hasResult: boolean;
}

interface ControllableAsyncIterator<T> {
  iterator: AsyncGenerator<T, void, void>;
  waitForNext: () => Promise<void>;
  resolveNext: (value: IteratorResult<T>) => void;
  rejectNext: (reason: unknown) => void;
  returnCallCount: number;
  resolveReturn: () => void;
  cleanup: () => void;
}

function createControllableAsyncIterator<T>(): ControllableAsyncIterator<T> {
  let pendingNext:
    | {
        resolve: (value: IteratorResult<T>) => void;
        reject: (reason: unknown) => void;
      }
    | undefined;
  let nextWaiters: Array<() => void> = [];
  let nextIsPending = false;
  let returnCallCount = 0;
  let resolveReturn: () => void = () => {};
  let returnStarted = false;
  const returnPromise = new Promise<void>((resolve) => {
    resolveReturn = resolve;
  });

  function waitForNext(): Promise<void> {
    if (nextIsPending) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      nextWaiters.push(resolve);
    });
  }

  async function returnImpl(): Promise<IteratorResult<T, void>> {
    // Real async generators are idempotent under repeated return() calls;
    // mirror that so both explicit close and for-await cleanup resolve once.
    if (returnStarted) {
      return { value: undefined, done: true };
    }
    returnStarted = true;
    returnCallCount += 1;
    const pending = pendingNext;
    pendingNext = undefined;
    nextIsPending = false;
    pending?.resolve({ value: undefined, done: true });
    await returnPromise;
    return { value: undefined, done: true };
  }

  const iterator: AsyncGenerator<T, void, void> = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      nextIsPending = true;
      const waiters = nextWaiters;
      nextWaiters = [];
      for (const waiter of waiters) {
        waiter();
      }
      const { promise, resolve, reject } =
        promiseWithResolvers<IteratorResult<T>>();
      pendingNext = { resolve, reject };
      return promise;
    },
    return: returnImpl,
    async throw(error?: unknown) {
      throw error;
    },
    async [Symbol.asyncDispose]() {
      await returnImpl();
    },
  };

  return {
    iterator,
    waitForNext,
    resolveNext(value: IteratorResult<T>) {
      const pending = pendingNext;
      pendingNext = undefined;
      nextIsPending = false;
      pending?.resolve(value);
    },
    rejectNext(reason: unknown) {
      const pending = pendingNext;
      pendingNext = undefined;
      nextIsPending = false;
      pending?.reject(reason);
    },
    get returnCallCount() {
      return returnCallCount;
    },
    resolveReturn() {
      resolveReturn();
    },
    cleanup() {
      pendingNext?.resolve({ value: undefined, done: true });
      pendingNext = undefined;
      nextIsPending = false;
      returnStarted = true;
      resolveReturn();
    },
  };
}

const itemType = new GraphQLObjectType({
  name: 'PublicCancellationItem',
  fields: () => ({ id: { type: GraphQLString } }),
});

const nestedType = new GraphQLObjectType({
  name: 'PublicCancellationNested',
  fields: () => ({
    nonNullValue: { type: new GraphQLNonNull(GraphQLString) },
    items: { type: new GraphQLList(itemType) },
  }),
});

const deferredType = new GraphQLObjectType({
  name: 'PublicCancellationDeferred',
  fields: () => ({
    value: { type: GraphQLString },
    items: { type: new GraphQLList(itemType) },
    nested: { type: nestedType },
  }),
});

const queryType = new GraphQLObjectType({
  name: 'PublicCancellationQuery',
  fields: () => ({ placeholder: { type: GraphQLString } }),
});

const mutationType = new GraphQLObjectType({
  name: 'PublicCancellationMutation',
  fields: () => ({
    sync: { type: GraphQLString },
    deferred: { type: deferredType },
  }),
});

const subscriptionType = new GraphQLObjectType({
  name: 'PublicCancellationSubscription',
  fields: () => ({ event: { type: GraphQLString } }),
});

const schema = new GraphQLSchema({
  query: queryType,
  mutation: mutationType,
  subscription: subscriptionType,
});

const executeChannel = getTracingChannel('graphql:execute');
const subscribeChannel = getTracingChannel('graphql:subscribe');

const unhandledRejections: Array<unknown> = [];
function onUnhandledRejection(reason: unknown): void {
  unhandledRejections.push(reason);
}

afterEach(() => {
  if (unhandledRejections.length > 0) {
    const reasons = unhandledRejections.splice(0);
    expect.fail(
      `Unhandled promise rejection(s): ${reasons
        .map((r) => (r instanceof Error ? r.message : String(r)))
        .join(', ')}`,
    );
  }
});

function collectLifecycle(): {
  events: Array<LifecycleEvent>;
  unsubscribe: () => void;
} {
  const events: Array<LifecycleEvent> = [];

  const executeHandler = {
    start: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'start',
        kind: 'execute',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
    end: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'end',
        kind: 'execute',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
    asyncStart: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'asyncStart',
        kind: 'execute',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
    asyncEnd: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'asyncEnd',
        kind: 'execute',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
    error: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'error',
        kind: 'execute',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
  };

  const subscribeHandler = {
    start: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'start',
        kind: 'subscribe',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
    end: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'end',
        kind: 'subscribe',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
    asyncStart: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'asyncStart',
        kind: 'subscribe',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
    asyncEnd: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'asyncEnd',
        kind: 'subscribe',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
    error: (context: { error?: unknown; result?: unknown }) => {
      events.push({
        channel: 'error',
        kind: 'subscribe',
        hasError: context.error !== undefined,
        hasResult: context.result !== undefined,
      });
    },
  };

  executeChannel.subscribe(executeHandler);
  subscribeChannel.subscribe(subscribeHandler);

  return {
    events,
    unsubscribe() {
      executeChannel.unsubscribe(executeHandler);
      subscribeChannel.unsubscribe(subscribeHandler);
    },
  };
}

async function expectRejection<T>(promise: Promise<T>): Promise<unknown> {
  try {
    const value = await promise;
    expect.fail(
      `Expected promise to reject but it resolved with: ${JSON.stringify(value)}`,
    );
  } catch (error) {
    return error;
  }
}

describe('public export cancellation with diagnostics lifecycle', () => {
  describe('incremental execute via the root package export', () => {
    it('emits a single start/end/asyncStart/asyncEnd sequence on normal completion and leaves no active iterator', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createControllableAsyncIterator<{ id: string }>();
      const lifecycle = collectLifecycle();
      try {
        const deferred = promiseWithResolvers<string>();
        const syncField = promiseWithResolvers<string>();

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
        // Resolve the synchronous initial field last so execute traverses its
        // async lifecycle (start/end/asyncStart/asyncEnd) rather than the
        // purely synchronous start/end pair.
        syncField.resolve('sync-done');
        const result = await resultPromise;
        expect(result).to.have.property('initialResult');
        const incremental = result as {
          initialResult: unknown;
          subsequentResults: AsyncGenerator<unknown, void, void>;
        };

        // Drain the stream to a normal completion.
        await firstNext;
        fixture.resolveNext({ value: { id: '0' }, done: false });
        const patchPromise = incremental.subsequentResults.next();
        const secondNext = fixture.waitForNext();
        await patchPromise;
        await secondNext;
        fixture.resolveNext({ value: undefined, done: true });
        fixture.resolveReturn();
        const finalPatch = await incremental.subsequentResults.next();
        expect(finalPatch.done).to.equal(false);
        expect(
          (finalPatch as IteratorResult<{ hasNext: boolean }>).value.hasNext,
        ).to.equal(false);

        // No terminal lifecycle event is duplicated and the error channel never fires.
        const executeEvents = lifecycle.events.filter(
          (e) => e.kind === 'execute',
        );
        const channelNames = executeEvents.map((e) => e.channel);
        expect(channelNames).to.deep.equal([
          'start',
          'end',
          'asyncStart',
          'asyncEnd',
        ]);
        expect(executeEvents.every((e) => !e.hasError)).to.equal(true);
        expect(fixture.returnCallCount).to.equal(0);
      } finally {
        lifecycle.unsubscribe();
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('emits exactly one error/asyncEnd pair on user cancellation and closes the source iterator once', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createControllableAsyncIterator<{ id: string }>();
      const lifecycle = collectLifecycle();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const syncField = promiseWithResolvers<string>();
        const deferred = promiseWithResolvers<string>();

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

        // With early execution the deferred stream source has already had its
        // first next() called (firstNext resolved) even though the initial
        // payload is still blocked by the pending sync field. Abort while both
        // are in flight so the execute promise rejects and the source iterator
        // is torn down.
        const firstNext = fixture.waitForNext();
        deferred.resolve('value');
        await firstNext;

        abortController.abort(reason);

        const error = await expectRejection(resultPromise as Promise<unknown>);
        expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
        expect((error as AbortedGraphQLExecutionError<unknown>).cause).to.equal(reason);

        // The execute call was aborted before its initial result, so its
        // promise rejected: one start/end pair then exactly one error/asyncEnd
        // pair, with no duplicated terminal events.
        const executeEvents = lifecycle.events.filter(
          (e) => e.kind === 'execute',
        );
        const channelNames = executeEvents.map((e) => e.channel);
        expect(channelNames).to.deep.equal([
          'start',
          'end',
          'error',
          'asyncStart',
          'asyncEnd',
        ]);
        const errorEvent = executeEvents.find((e) => e.channel === 'error');
        expect(errorEvent?.hasError).to.equal(true);
        expect(errorEvent?.hasResult).to.equal(false);

        // Cancellation must have torn down the source iterator exactly once.
        expect(fixture.returnCallCount).to.equal(1);
        fixture.resolveReturn();
        await resolveOnNextTick();

        // Late resolutions do not re-fire lifecycle.
        syncField.resolve('late-sync');
        fixture.resolveNext({ value: { id: 'late' }, done: false });
        await resolveOnNextTick();
        await resolveOnNextTick();
        const eventCountAfter = lifecycle.events.filter(
          (e) => e.kind === 'execute',
        ).length;
        expect(eventCountAfter).to.equal(5);
      } finally {
        lifecycle.unsubscribe();
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('reports a resolver failure via asyncEnd and does not emit duplicate terminal events', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createControllableAsyncIterator<{ id: string }>();
      const lifecycle = collectLifecycle();
      try {
        const deferred = promiseWithResolvers<string>();
        const syncField = promiseWithResolvers<string>();

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
        expect(result).to.have.property('initialResult');
        const incremental = result as {
          subsequentResults: AsyncGenerator<unknown, void, void>;
        };

        // Let the deferred fragment and stream source start, then fail the
        // non-null resolver while a source next() is pending.
        await firstNext;
        const nextPromise = incremental.subsequentResults.next();
        const resolverError = new Error('Resolver failed');
        deferred.reject(resolverError);
        const patch = await nextPromise;

        // The failure is delivered as an incremental patch containing errors
        // (not an abrupt execute-channel error); the lifecycle still ends once.
        expect(patch.done).to.equal(false);
        const value = (patch as IteratorResult<{
          incremental?: ReadonlyArray<{
            data?: unknown;
            errors?: ReadonlyArray<unknown>;
          }>;
        }>).value;
        const failedEntry = value.incremental?.find(
          (entry: unknown): entry is {
            data: unknown;
            errors: ReadonlyArray<unknown>;
          } =>
            typeof entry === 'object' &&
            entry !== null &&
            'errors' in entry &&
            (entry as { errors?: ReadonlyArray<unknown> }).errors !== undefined,
        );
        expect(failedEntry).to.not.equal(undefined);
        // Nulling `nested` also reports its (not-yet-emitted) streamed items
        // as an empty array; the nested stream at path `deferred.nested.items`
        // is torn down because it is under the nulled position.
        expect(failedEntry?.data).to.deep.equal({
          deferred: { nested: null },
        });

        // Stream cancelled by the bubbling null must close the source iterator once.
        expect(fixture.returnCallCount).to.equal(1);
        fixture.resolveReturn();
        await resolveOnNextTick();

        const executeEvents = lifecycle.events.filter(
          (e) => e.kind === 'execute',
        );
        const channelNames = executeEvents.map((e) => e.channel);
        expect(channelNames).to.deep.equal([
          'start',
          'end',
          'asyncStart',
          'asyncEnd',
        ]);
        expect(executeEvents.every((e) => !e.hasError)).to.equal(true);
      } finally {
        lifecycle.unsubscribe();
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });
  });

  describe('subscribe via the root package export', () => {
    it('emits one subscribe lifecycle for setup and leaves no active iterator on normal return', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createControllableAsyncIterator<{ event: string }>();
      const lifecycle = collectLifecycle();
      try {
        // Resolve the source stream after a microtask so subscribe traverses
        // its asynchronous start/end lifecycle (rather than the sync path).
        const sourcePromise = Promise.resolve().then(() => fixture.iterator);
        const result = await subscribe({
          schema,
          document: parse('subscription { event }'),
          rootValue: { event: () => sourcePromise },
        });
        expect(result).to.have.property('next');
        const stream = result as AsyncGenerator<ExecutionResult, void, void>;

        const nextPromise = stream.next();
        await fixture.waitForNext();
        fixture.resolveNext({ value: { event: 'one' }, done: false });
        const delivered = await nextPromise;
        expect(delivered.done).to.equal(false);

        const returned = stream.return();
        expect(fixture.returnCallCount).to.equal(1);
        fixture.resolveReturn();
        await returned;

        // Exactly one start/end/asyncStart/asyncEnd sequence, no error and no
        // duplicated terminal events.
        const subscribeEvents = lifecycle.events.filter(
          (e) => e.kind === 'subscribe',
        );
        expect(subscribeEvents.map((e) => e.channel)).to.deep.equal([
          'start',
          'end',
          'asyncStart',
          'asyncEnd',
        ]);
        expect(subscribeEvents.every((e) => !e.hasError)).to.equal(true);
      } finally {
        lifecycle.unsubscribe();
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('cancels an in-flight subscription next() without duplicate terminal events or an active iterator', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createControllableAsyncIterator<{ event: string }>();
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
        expect(result).to.have.property('next');
        const stream = result as AsyncGenerator<ExecutionResult, void, void>;

        const nextPromise = stream.next();
        await fixture.waitForNext();
        abortController.abort(reason);

        const error = await expectRejection(nextPromise);
        expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
        expect((error as AbortedGraphQLExecutionError<unknown>).cause).to.equal(reason);

        const returned = stream.return();
        expect(fixture.returnCallCount).to.equal(1);
        fixture.resolveReturn();
        await returned;

        // The subscribe setup itself completed successfully; cancellation of an
        // in-flight event must not synthesize an extra subscribe error/end.
        const subscribeEvents = lifecycle.events.filter(
          (e) => e.kind === 'subscribe',
        );
        expect(subscribeEvents.map((e) => e.channel)).to.deep.equal([
          'start',
          'end',
          'asyncStart',
          'asyncEnd',
        ]);
        expect(subscribeEvents.every((e) => !e.hasError)).to.equal(true);

        fixture.resolveNext({ value: { event: 'late' }, done: false });
        await resolveOnNextTick();
        expect(
          lifecycle.events.filter((e) => e.kind === 'subscribe').length,
        ).to.equal(4);
      } finally {
        lifecycle.unsubscribe();
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });
  });
});
