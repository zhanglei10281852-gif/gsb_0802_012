import { afterEach, describe, it } from 'node:test';

import { expect } from 'chai';

import { resolveOnNextTick } from '../../__testUtils__/resolveOnNextTick.ts';

import { isPromise } from '../../jsutils/isPromise.ts';
import { promiseWithResolvers } from '../../jsutils/promiseWithResolvers.ts';

import { parse } from '../../language/parser.ts';
import type { DocumentNode } from '../../language/ast.ts';

import type { GraphQLResolveInfo } from '../../type/index.ts';
import {
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
} from '../../type/definition.ts';
import { GraphQLString } from '../../type/scalars.ts';
import { GraphQLSchema } from '../../type/schema.ts';

import {
  AbortedGraphQLExecutionError,
  execute,
  experimentalExecuteIncrementally,
  subscribe,
} from '../index.ts';
import type { ExecutionResult } from '../Executor.ts';

function createControllablePromise<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
} {
  const { promise, resolve, reject } = promiseWithResolvers<T>();
  return { promise, resolve, reject };
}

interface ControllableAsyncIterator<T> {
  iterator: AsyncGenerator<T, void, void>;
  resolveNext: (value: IteratorResult<T>) => void;
  rejectNext: (reason: unknown) => void;
  waitForNext: () => Promise<void>;
  returnCallCount: number;
  resolveReturn: () => void;
  rejectReturn: (reason: unknown) => void;
  cleanup: () => void;
}

function createControllableAsyncIterator<T>(): ControllableAsyncIterator<T> {
  let pendingNext:
    | {
        resolve: (value: IteratorResult<T>) => void;
        reject: (reason: unknown) => void;
      }
    | undefined;
  let pendingNextStarted: Array<() => void> = [];
  let nextIsPending = false;
  let returnCallCount = 0;
  let resolveReturn: () => void = () => {};
  let rejectReturn: (reason: unknown) => void = () => {};
  const returnPromise = new Promise<void>((resolve, reject) => {
    resolveReturn = resolve;
    rejectReturn = reject;
  });
  returnPromise.catch(() => undefined);

  function signalNextStarted(): void {
    const waiters = pendingNextStarted;
    pendingNextStarted = [];
    for (const waiter of waiters) {
      waiter();
    }
  }

  function waitForNext(): Promise<void> {
    if (nextIsPending) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      pendingNextStarted.push(resolve);
    });
  }

  async function returnImpl(): Promise<IteratorResult<T, void>> {
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
      signalNextStarted();
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
    waitForNext,
    get returnCallCount() {
      return returnCallCount;
    },
    resolveReturn() {
      resolveReturn();
    },
    rejectReturn(reason: unknown) {
      rejectReturn(reason);
    },
    cleanup() {
      pendingNext?.resolve({ value: undefined, done: true });
      pendingNext = undefined;
      nextIsPending = false;
      resolveReturn();
    },
  };
}

function assertAbortedError(
  error: unknown,
  reason: unknown,
): asserts error is AbortedGraphQLExecutionError<unknown> {
  expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
  const abortedError = error as AbortedGraphQLExecutionError<unknown>;
  if (reason instanceof Error) {
    expect(abortedError.cause).to.equal(reason);
    expect(abortedError.message).to.equal(reason.message);
  } else {
    expect(abortedError.cause).to.equal(reason);
  }
}

async function expectRejection<T>(
  promise: Promise<T>,
): Promise<unknown> {
  try {
    const value = await promise;
    expect.fail(
      `Expected promise to reject but it resolved with: ${JSON.stringify(value)}`,
    );
  } catch (error) {
    return error;
  }
}

const queryType: GraphQLObjectType = new GraphQLObjectType({
  name: 'Query',
  fields: () => ({
    syncField: { type: GraphQLString },
    promiseField: { type: GraphQLString },
    nonNullPromiseField: { type: new GraphQLNonNull(GraphQLString) },
    listField: { type: new GraphQLList(GraphQLString) },
    listOfPromiseField: { type: new GraphQLList(GraphQLString) },
  }),
});

const subscriptionType = new GraphQLObjectType({
  name: 'Subscription',
  fields: () => ({
    event: { type: GraphQLString },
    promiseEvent: { type: GraphQLString },
  }),
});

const schema = new GraphQLSchema({
  query: queryType,
  subscription: subscriptionType,
});

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

describe('AbortSignal: public execution cancellation', () => {
  describe('execute', () => {
    it('rejects when abort signal is already aborted before execution starts', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        abortController.abort(reason);

        const result = execute({
          schema,
          document: parse('{ syncField }'),
          rootValue: { syncField: 'value' },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        const error = await expectRejection(result as Promise<ExecutionResult>);
        assertAbortedError(error, reason);
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('uses the default abort reason when aborted without a reason', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const abortController = new AbortController();
        const controllable = createControllablePromise<string>();

        const result = execute({
          schema,
          document: parse('{ promiseField }'),
          rootValue: {
            promiseField: () => controllable.promise,
          },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        const resultPromise = result as Promise<ExecutionResult>;
        abortController.abort();

        const error = await expectRejection(resultPromise);
        expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
        const abortedError = error as AbortedGraphQLExecutionError<unknown>;
        expect(abortedError.cause).to.equal(abortController.signal.reason);

        controllable.resolve('late');
        await resolveOnNextTick();
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('aborts while a resolver promise is pending and surfaces the abort reason', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const controllable = createControllablePromise<string>();
        let observedAbortSignal: AbortSignal | undefined;

        const result = execute({
          schema,
          document: parse('{ promiseField }'),
          rootValue: {
            promiseField: (
              _args: unknown,
              _ctx: unknown,
              info: GraphQLResolveInfo,
            ) => {
              observedAbortSignal = info.getAbortSignal();
              return controllable.promise;
            },
          },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        abortController.abort(reason);

        const error = await expectRejection(result as Promise<ExecutionResult>);
        assertAbortedError(error, reason);

        expect(observedAbortSignal?.aborted).to.equal(true);

        controllable.resolve('late-value');
        await resolveOnNextTick();
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('does not turn a late resolver rejection into an unhandled rejection after abort', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const controllable = createControllablePromise<string>();

        const result = execute({
          schema,
          document: parse('{ promiseField }'),
          rootValue: {
            promiseField: () => controllable.promise,
          },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        abortController.abort(reason);

        const error = await expectRejection(result as Promise<ExecutionResult>);
        assertAbortedError(error, reason);

        const lateError = new Error('Late resolver failure');
        controllable.reject(lateError);
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('bubbles abort through a non-null field when the inner resolver is pending', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const controllable = createControllablePromise<string>();

        const result = execute({
          schema,
          document: parse('{ nonNullPromiseField }'),
          rootValue: {
            nonNullPromiseField: () => controllable.promise,
          },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        abortController.abort(reason);

        const error = await expectRejection(result as Promise<ExecutionResult>);
        assertAbortedError(error, reason);

        controllable.resolve('late');
        await resolveOnNextTick();
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('aborts while waiting for an async iterable list item and calls iterator return exactly once', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const controllableIterator =
        createControllableAsyncIterator<string>();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();

        const result = execute({
          schema,
          document: parse('{ listField }'),
          rootValue: {
            listField: () => controllableIterator.iterator,
          },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        await resolveOnNextTick();
        abortController.abort(reason);
        controllableIterator.resolveNext({ value: 'item', done: false });

        const error = await expectRejection(result as Promise<ExecutionResult>);
        assertAbortedError(error, reason);

        expect(controllableIterator.returnCallCount).to.equal(1);

        controllableIterator.resolveReturn();
        await resolveOnNextTick();
        await resolveOnNextTick();
        expect(controllableIterator.returnCallCount).to.equal(1);
      } finally {
        controllableIterator.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('handles a late rejection from the list iterator after abort without unhandled rejection', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const controllableIterator =
        createControllableAsyncIterator<string>();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();

        const result = execute({
          schema,
          document: parse('{ listField }'),
          rootValue: {
            listField: () => controllableIterator.iterator,
          },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        await resolveOnNextTick();
        abortController.abort(reason);
        const iteratorError = new Error('Iterator exploded');
        controllableIterator.rejectNext(iteratorError);

        const error = await expectRejection(result as Promise<ExecutionResult>);
        assertAbortedError(error, reason);

        controllableIterator.resolveReturn();
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        controllableIterator.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('aborts while a list item promise is pending', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const controllable = createControllablePromise<string>();

        const result = execute({
          schema,
          document: parse('{ listOfPromiseField }'),
          rootValue: {
            listOfPromiseField: () => [controllable.promise],
          },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        abortController.abort(reason);

        const error = await expectRejection(result as Promise<ExecutionResult>);
        assertAbortedError(error, reason);

        controllable.resolve('late-item');
        await resolveOnNextTick();
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });
  });

  describe('subscribe', () => {
    it('aborts while the subscription resolver promise is pending during establishment', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const controllableIterator =
        createControllableAsyncIterator<string>();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const controllable = createControllablePromise<AsyncIterable<string>>();

        const result = subscribe({
          schema,
          document: parse('subscription { event }'),
          rootValue: {
            event: () => controllable.promise,
          },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        abortController.abort(reason);

        const resolved = await (result as Promise<unknown>);
        expect(resolved).to.have.property('errors');
        const executionResult = resolved as ExecutionResult;
        expect(executionResult.errors).to.have.lengthOf(1);

        controllable.resolve(controllableIterator.iterator);
        controllableIterator.resolveReturn();
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        controllableIterator.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('aborts while waiting for the next subscription event and rejects the in-flight next()', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const controllableIterator =
        createControllableAsyncIterator<{ event: string }>();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();

        const result = await subscribe({
          schema,
          document: parse('subscription { event }'),
          rootValue: {
            event: () => controllableIterator.iterator,
          },
          abortSignal: abortController.signal,
        });

        expect(result).to.have.property('next');
        const responseStream = result as AsyncGenerator<
          ExecutionResult,
          void,
          void
        >;

        const nextPromise = responseStream.next();
        await resolveOnNextTick();
        abortController.abort(reason);

        const nextError = await expectRejection(nextPromise);
        assertAbortedError(nextError, reason);

        const returnPromise = responseStream.return();
        expect(controllableIterator.returnCallCount).to.equal(1);
        controllableIterator.resolveReturn();
        await returnPromise;
        expect(controllableIterator.returnCallCount).to.equal(1);

        controllableIterator.resolveNext({
          value: { event: 'late' },
          done: false,
        });
        await resolveOnNextTick();
        await resolveOnNextTick();
        expect(controllableIterator.returnCallCount).to.equal(1);
      } finally {
        controllableIterator.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('aborts while a per-event field resolver is pending during consumption', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const controllableIterator =
        createControllableAsyncIterator<{
          promiseEvent: () => Promise<string>;
        }>();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const eventControllable = createControllablePromise<string>();

        const result = await subscribe({
          schema,
          document: parse('subscription { promiseEvent }'),
          rootValue: {
            promiseEvent: () => controllableIterator.iterator,
          },
          abortSignal: abortController.signal,
        });

        expect(result).to.have.property('next');
        const responseStream = result as AsyncGenerator<
          ExecutionResult,
          void,
          void
        >;

        const nextPromise = responseStream.next();
        await resolveOnNextTick();
        controllableIterator.resolveNext({
          value: { promiseEvent: () => eventControllable.promise },
          done: false,
        });

        await resolveOnNextTick();
        abortController.abort(reason);

        const nextError = await expectRejection(nextPromise);
        assertAbortedError(nextError, reason);

        eventControllable.resolve('late');
        const returnPromise = responseStream.return();
        controllableIterator.resolveReturn();
        await returnPromise;
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        controllableIterator.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });
  });

  describe('experimentalExecuteIncrementally', () => {
    it('rejects when abort signal is already aborted before execution starts', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        abortController.abort(reason);

        const result = experimentalExecuteIncrementally({
          schema,
          document: parse('{ syncField }'),
          rootValue: { syncField: 'value' },
          abortSignal: abortController.signal,
        });

        expect(isPromise(result)).to.equal(true);
        const error = await expectRejection(
          result as Promise<ExecutionResult>,
        );
        assertAbortedError(error, reason);
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('delivers an initial result and aborts subsequent deferred results', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const deferred = createControllablePromise<string>();

        const result = await experimentalExecuteIncrementally({
          schema,
          document: parse(`
            {
              syncField
              ... @defer {
                promiseField
              }
            }
          `),
          rootValue: {
            syncField: 'initial',
            promiseField: () => deferred.promise,
          },
          abortSignal: abortController.signal,
        });

        expect(result).to.have.property('initialResult');
        const incrementalResult = result as {
          initialResult: { data: unknown; hasNext: boolean };
          subsequentResults: AsyncGenerator<unknown, void, void>;
        };
        expect(incrementalResult.initialResult.data).to.deep.equal({
          syncField: 'initial',
        });
        expect(incrementalResult.initialResult.hasNext).to.equal(true);

        const nextPromise = incrementalResult.subsequentResults.next();
        abortController.abort(reason);

        const nextError = await expectRejection(nextPromise);
        assertAbortedError(nextError, reason);

        deferred.resolve('late');
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('aborts subsequent streamed results and closes the source iterator once', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const controllableIterator =
        createControllableAsyncIterator<string>();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();

        const result = await experimentalExecuteIncrementally({
          schema,
          document: parse(`
            {
              listField @stream(initialCount: 0)
            }
          `),
          rootValue: {
            listField: () => controllableIterator.iterator,
          },
          abortSignal: abortController.signal,
        });

        expect(result).to.have.property('initialResult');
        const incrementalResult = result as {
          initialResult: { data: unknown; hasNext: boolean };
          subsequentResults: AsyncGenerator<unknown, void, void>;
        };
        expect(incrementalResult.initialResult.data).to.deep.equal({
          listField: [],
        });

        const nextPromise = incrementalResult.subsequentResults.next();
        await resolveOnNextTick();
        abortController.abort(reason);

        const nextError = await expectRejection(nextPromise);
        assertAbortedError(nextError, reason);

        expect(controllableIterator.returnCallCount).to.equal(1);
        controllableIterator.resolveReturn();
        controllableIterator.resolveNext({ value: 'late', done: false });
        await resolveOnNextTick();
        await resolveOnNextTick();
        expect(controllableIterator.returnCallCount).to.equal(1);
      } finally {
        controllableIterator.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('aborts while a deferred non-null field resolver is pending and surfaces the abort reason', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();
        const deferred = createControllablePromise<string>();

        const result = await experimentalExecuteIncrementally({
          schema,
          document: parse(`
            {
              syncField
              ... @defer {
                nonNullPromiseField
              }
            }
          `),
          rootValue: {
            syncField: 'initial',
            nonNullPromiseField: () => deferred.promise,
          },
          abortSignal: abortController.signal,
        });

        expect(result).to.have.property('initialResult');
        const incrementalResult = result as {
          initialResult: { data: unknown; hasNext: boolean };
          subsequentResults: AsyncGenerator<unknown, void, void>;
        };

        const nextPromise = incrementalResult.subsequentResults.next();
        await resolveOnNextTick();
        abortController.abort(reason);

        const nextError = await expectRejection(nextPromise);
        assertAbortedError(nextError, reason);

        deferred.resolve('late');
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });
  });

  describe('combined mutation with serial fields, parallel fields, @defer and @stream', () => {
    interface CombinedFixture {
      schema: GraphQLSchema;
      document: DocumentNode;
      firstMutation: ReturnType<typeof createControllablePromise<string>>;
      deferredField: ReturnType<typeof createControllablePromise<string>>;
      nonNullDeferredField: ReturnType<typeof createControllablePromise<string>>;
      iterator: ControllableAsyncIterator<{ id: string; name: string }>;
      resolverCalls: Array<string>;
      cleanup: () => void;
    }

    function createCombinedFixture(opts?: {
      nonNullDeferred?: boolean;
    }): CombinedFixture {
      const resolverCalls: Array<string> = [];
      const firstMutation = createControllablePromise<string>();
      const deferredField = createControllablePromise<string>();
      const nonNullDeferredField =
        createControllablePromise<string>();
      const iterator =
        createControllableAsyncIterator<{ id: string; name: string }>();

      const ItemType = new GraphQLObjectType({
        name: 'CombinedItem',
        fields: () => ({
          id: { type: GraphQLString },
          name: { type: GraphQLString },
        }),
      });

      const DeferredType = new GraphQLObjectType({
        name: 'CombinedDeferred',
        fields: () => ({
          deferredField: { type: GraphQLString },
          nonNullDeferredField: {
            type: new GraphQLNonNull(GraphQLString),
          },
          items: { type: new GraphQLList(ItemType) },
        }),
      });

      const MutationType = new GraphQLObjectType({
        name: 'CombinedMutation',
        fields: () => ({
          firstMutation: {
            type: GraphQLString,
            resolve: () => {
              resolverCalls.push('firstMutation:start');
              return firstMutation.promise.then((value) => {
                resolverCalls.push('firstMutation:end');
                return value;
              });
            },
          },
          secondMutation: {
            type: GraphQLString,
            resolve: () => {
              resolverCalls.push('secondMutation');
              return 'second-done';
            },
          },
          parallelField: { type: GraphQLString },
          deferred: { type: DeferredType },
        }),
      });

      const schema = new GraphQLSchema({
        query: new GraphQLObjectType({
          name: 'CombinedQuery',
          fields: { placeholder: { type: GraphQLString } },
        }),
        mutation: MutationType,
      });

      const nonNullSelection = opts?.nonNullDeferred
        ? 'nonNullDeferredField'
        : 'deferredField';

      const document = parse(`
        mutation Combined {
          firstMutation
          secondMutation
          parallelField
          ... @defer {
            deferred {
              ${nonNullSelection}
              items @stream(initialCount: 0) {
                id
                name
              }
            }
          }
        }
      `);

      return {
        schema,
        document,
        firstMutation,
        deferredField,
        nonNullDeferredField,
        iterator,
        resolverCalls,
        cleanup() {
          iterator.cleanup();
        },
      };
    }

    function rootValueFor(fixture: CombinedFixture) {
      return {
        parallelField: () => {
          fixture.resolverCalls.push('parallelField');
          return 'parallel-done';
        },
        deferred: () => ({
          deferredField: () => {
            fixture.resolverCalls.push('deferredField:start');
            return fixture.deferredField.promise.then((value) => {
              fixture.resolverCalls.push('deferredField:end');
              return value;
            });
          },
          nonNullDeferredField: () => {
            fixture.resolverCalls.push('nonNullDeferredField:start');
            return fixture.nonNullDeferredField.promise.then((value) => {
              fixture.resolverCalls.push('nonNullDeferredField:end');
              return value;
            });
          },
          items: () => fixture.iterator.iterator,
        }),
      };
    }

    it('cancels before the initial payload is produced while serial mutations are pending', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createCombinedFixture();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();

        const resultPromise = experimentalExecuteIncrementally({
          schema: fixture.schema,
          document: fixture.document,
          rootValue: rootValueFor(fixture),
          abortSignal: abortController.signal,
          enableEarlyExecution: true,
        });

        // The first serial mutation is pending; abort before anything is delivered.
        await resolveOnNextTick();
        abortController.abort(reason);

        const error = await expectRejection(resultPromise as Promise<unknown>);
        assertAbortedError(error, reason);

        // The second serial mutation must never have started.
        expect(fixture.resolverCalls).to.include('firstMutation:start');
        expect(fixture.resolverCalls).to.not.include('secondMutation');

        // Resolving the pending mutation and iterator must not cause unhandled rejections.
        fixture.firstMutation.resolve('late-first');
        fixture.deferredField.resolve('late-defer');
        fixture.iterator.resolveReturn();
        fixture.iterator.resolveNext({ value: undefined, done: true });
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('cancels after the initial payload and stops the deferred patch and stream', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createCombinedFixture();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();

        const resultPromise = experimentalExecuteIncrementally({
          schema: fixture.schema,
          document: fixture.document,
          rootValue: rootValueFor(fixture),
          abortSignal: abortController.signal,
          enableEarlyExecution: true,
        });

        // Resolve the serial mutation so the initial payload can be delivered;
        // the deferred field and stream remain pending.
        fixture.firstMutation.resolve('first-done');
        const result = await resultPromise;

        expect(result).to.have.property('initialResult');
        const incrementalResult = result as {
          initialResult: {
            data: unknown;
            pending: ReadonlyArray<{ id: string; path: ReadonlyArray<string | number> }>;
            hasNext: boolean;
          };
          subsequentResults: AsyncGenerator<unknown, void, void>;
        };

        // The serial mutations and parallel field all resolve in the initial payload,
        // with the deferred fragment (and its streamed list) still pending.
        expect(incrementalResult.initialResult.data).to.deep.equal({
          firstMutation: 'first-done',
          secondMutation: 'second-done',
          parallelField: 'parallel-done',
        });
        expect(incrementalResult.initialResult.hasNext).to.equal(true);
        expect(incrementalResult.initialResult.pending).to.have.lengthOf(1);

        // The deferred resolver starts early; abort after the initial payload.
        await resolveOnNextTick();
        abortController.abort(reason);

        const nextPromise = incrementalResult.subsequentResults.next();
        const nextError = await expectRejection(nextPromise);
        assertAbortedError(nextError, reason);

        // No patch should have been delivered; the deferred resolver resolves late
        // and must not surface or cause an unhandled rejection.
        fixture.deferredField.resolve('late-defer');
        fixture.iterator.resolveReturn();
        fixture.iterator.resolveNext({ value: undefined, done: true });
        await resolveOnNextTick();
        await resolveOnNextTick();

        // The first mutation completed before abort; the second ran serially after it.
        expect(fixture.resolverCalls).to.include('firstMutation:end');
        expect(fixture.resolverCalls).to.include('secondMutation');
      } finally {
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('cancels while a deferred patch is queued and drops the unsent patch', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createCombinedFixture();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();

        const resultPromise = experimentalExecuteIncrementally({
          schema: fixture.schema,
          document: fixture.document,
          rootValue: rootValueFor(fixture),
          abortSignal: abortController.signal,
          enableEarlyExecution: true,
        });

        // Resolve the serial mutation to deliver the initial payload.
        fixture.firstMutation.resolve('first-done');
        const result = await resultPromise;

        expect(result).to.have.property('initialResult');
        const incrementalResult = result as {
          initialResult: { data: unknown; hasNext: boolean };
          subsequentResults: AsyncGenerator<unknown, void, void>;
        };
        expect(incrementalResult.initialResult.hasNext).to.equal(true);

        // Resolve the deferred field so a patch is queued but not yet consumed.
        fixture.deferredField.resolve('deferred-value');
        await resolveOnNextTick();
        await resolveOnNextTick();

        abortController.abort(reason);

        const nextPromise = incrementalResult.subsequentResults.next();
        const nextError = await expectRejection(nextPromise);
        assertAbortedError(nextError, reason);

        // Resolve the stream iterator and any late work; must not unhandled-reject.
        fixture.iterator.resolveReturn();
        fixture.iterator.resolveNext({ value: undefined, done: true });
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('cancels after the stream iterator produced partial items and closes it once', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createCombinedFixture();
      try {
        const reason = new Error('Client disconnected');
        const abortController = new AbortController();

        const resultPromise = experimentalExecuteIncrementally({
          schema: fixture.schema,
          document: fixture.document,
          rootValue: rootValueFor(fixture),
          abortSignal: abortController.signal,
          enableEarlyExecution: true,
        });

        // Resolve the serial mutation to deliver the initial payload.
        fixture.firstMutation.resolve('first-done');
        const result = await resultPromise;

        expect(result).to.have.property('initialResult');
        const incrementalResult = result as {
          initialResult: { data: unknown; hasNext: boolean };
          subsequentResults: AsyncGenerator<unknown, void, void>;
        };

        // With early execution the nested stream starts pulling as soon as the
        // deferred group executes. Wait for the iterator's first next() call.
        const firstItemStarted = fixture.iterator.waitForNext();

        // The first subsequent patch completes the deferred group and registers
        // the nested stream. Resolve the deferred field to let the group finish.
        const deferredPatchPromise = incrementalResult.subsequentResults.next();
        fixture.deferredField.resolve('deferred-value');
        const deferredPatch = await deferredPatchPromise;
        expect(deferredPatch.done).to.equal(false);

        // Deliver the first stream item once next() has been called.
        await firstItemStarted;
        const streamPatchPromise = incrementalResult.subsequentResults.next();
        fixture.iterator.resolveNext({
          value: { id: '0', name: 'item-0' },
          done: false,
        });
        const streamPatch = await streamPatchPromise;
        expect(streamPatch.done).to.equal(false);
        const streamValue = (streamPatch as IteratorResult<{
          incremental?: ReadonlyArray<{ items?: ReadonlyArray<unknown> }>;
        }>).value;
        const streamEntry = streamValue.incremental?.find(
          (entry: unknown): entry is { items: ReadonlyArray<unknown> } =>
            entry != null &&
            typeof entry === 'object' &&
            'items' in entry,
        );
        expect(streamEntry?.items).to.have.lengthOf(1);

        // Cancel while the next stream item is pending; wait for the next next() call.
        const secondItemStarted = fixture.iterator.waitForNext();
        const nextPatchPromise = incrementalResult.subsequentResults.next();
        await secondItemStarted;
        abortController.abort(reason);

        const nextError = await expectRejection(nextPatchPromise);
        assertAbortedError(nextError, reason);

        // The source iterator is returned exactly once by cancellation cleanup.
        expect(fixture.iterator.returnCallCount).to.equal(1);
        fixture.iterator.resolveReturn();

        // A late item must not re-deliver or cause unhandled rejections.
        fixture.iterator.resolveNext({
          value: { id: '1', name: 'item-1' },
          done: false,
        });
        await resolveOnNextTick();
        await resolveOnNextTick();
        expect(fixture.iterator.returnCallCount).to.equal(1);
      } finally {
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });

    it('bubbles a non-null deferred field failure to the client and tears down pending stream work', async () => {
      process.on('unhandledRejection', onUnhandledRejection);
      const fixture = createCombinedFixture({ nonNullDeferred: true });
      try {
        const resultPromise = experimentalExecuteIncrementally({
          schema: fixture.schema,
          document: fixture.document,
          rootValue: rootValueFor(fixture),
          enableEarlyExecution: true,
        });

        // Resolve the serial mutation to deliver the initial payload.
        fixture.firstMutation.resolve('first-done');
        const result = await resultPromise;

        expect(result).to.have.property('initialResult');
        const incrementalResult = result as {
          initialResult: { data: unknown; hasNext: boolean };
          subsequentResults: AsyncGenerator<
            {
              hasNext: boolean;
              incremental?: ReadonlyArray<{
                data?: unknown;
                items?: ReadonlyArray<unknown>;
                errors?: ReadonlyArray<{
                  message: string;
                  path?: ReadonlyArray<string | number>;
                }>;
              }>;
            },
            void,
            void
          >;
        };
        expect(incrementalResult.initialResult.hasNext).to.equal(true);

        await resolveOnNextTick();
        await resolveOnNextTick();

        // Reject the non-null deferred field; it must bubble as null in the patch.
        const nonNullError = new Error('Deferred non-null failure');
        fixture.nonNullDeferredField.reject(nonNullError);

        const patch = await incrementalResult.subsequentResults.next();
        expect(patch.done).to.equal(false);
        const value = (
          patch as IteratorResult<{
            hasNext: boolean;
            incremental?: ReadonlyArray<{
              data?: unknown;
              errors?: ReadonlyArray<{ message: string }>;
            }>;
          }>
        ).value;

        // The non-null field failure bubbles up and nulls the containing `deferred`
        // object; the nested stream at `deferred.items` is cancelled by the failure.
        const deferEntry = value.incremental?.find(
          (entry: unknown): entry is {
            data: { deferred: unknown };
            errors?: ReadonlyArray<{ message: string; path?: ReadonlyArray<string | number> }>;
          } =>
            typeof entry === 'object' &&
            entry !== null &&
            'data' in entry,
        );
        expect(deferEntry?.data).to.deep.equal({ deferred: null });
        expect(deferEntry?.errors?.[0]?.message).to.equal(
          'Deferred non-null failure',
        );
        expect(deferEntry?.errors?.[0]?.path).to.deep.equal([
          'deferred',
          'nonNullDeferredField',
        ]);

        // The bubbling null at `deferred` cancels the nested stream at
        // `deferred.items`, so the source iterator is returned exactly once.
        expect(fixture.iterator.returnCallCount).to.equal(1);
        fixture.iterator.resolveReturn();
        await resolveOnNextTick();
        await resolveOnNextTick();
      } finally {
        fixture.cleanup();
        process.removeListener('unhandledRejection', onUnhandledRejection);
      }
    });
  });
});
