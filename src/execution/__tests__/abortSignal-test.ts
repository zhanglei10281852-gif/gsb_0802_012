import { afterEach, describe, it } from 'node:test';

import { expect } from 'chai';

import { resolveOnNextTick } from '../../__testUtils__/resolveOnNextTick.ts';

import { isPromise } from '../../jsutils/isPromise.ts';
import { promiseWithResolvers } from '../../jsutils/promiseWithResolvers.ts';

import { parse } from '../../language/parser.ts';

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
  let returnCallCount = 0;
  let resolveReturn: () => void = () => {};
  let rejectReturn: (reason: unknown) => void = () => {};
  const returnPromise = new Promise<void>((resolve, reject) => {
    resolveReturn = resolve;
    rejectReturn = reject;
  });
  returnPromise.catch(() => undefined);

  async function returnImpl(): Promise<IteratorResult<T, void>> {
    returnCallCount += 1;
    const pending = pendingNext;
    pendingNext = undefined;
    pending?.resolve({ value: undefined, done: true });
    await returnPromise;
    return { value: undefined, done: true };
  }

  const iterator: AsyncGenerator<T, void, void> = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
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
      pending?.resolve(value);
    },
    rejectNext(reason: unknown) {
      const pending = pendingNext;
      pendingNext = undefined;
      pending?.reject(reason);
    },
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
});
