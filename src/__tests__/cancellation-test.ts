import { describe, it } from 'node:test';

import { assert, expect } from 'chai';

import { expectEvents } from '../__testUtils__/expectEvents.ts';
import { expectJSON } from '../__testUtils__/expectJSON.ts';
import { expectPromise } from '../__testUtils__/expectPromise.ts';
import { expectToThrow } from '../__testUtils__/expectToThrow.ts';
import { getTracingChannel } from '../__testUtils__/getTracingChannel.ts';
import type { MethodSpy } from '../__testUtils__/spyOn.ts';
import { spyOnMethod } from '../__testUtils__/spyOn.ts';

import type { PromiseOrValue } from '../jsutils/PromiseOrValue.ts';
import { promiseWithResolvers } from '../jsutils/promiseWithResolvers.ts';

// This suite intentionally imports only from the package root so that the
// cancellation behavior is exercised through the same public surface that
// consumers and the published build artifacts expose.
import {
  AbortedGraphQLExecutionError,
  buildSchema,
  execute,
  experimentalExecuteIncrementally,
  OperationTypeNode,
  parse,
  subscribe,
} from '../index.ts';

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

const executeChannel = getTracingChannel('graphql:execute');
const resolveChannel = getTracingChannel('graphql:resolve');
const subscribeChannel = getTracingChannel('graphql:subscribe');

/**
 * Yields to the event loop a bounded number of turns so that work scheduled
 * by the implementation can settle, without sleeping for a real duration.
 */
function drainEventLoop(): Promise<void> {
  let drained: Promise<void> = Promise.resolve();
  for (let i = 0; i < 5; i++) {
    drained = drained.then(
      () =>
        new Promise<void>((resolve) => {
          // eslint-disable-next-line no-undef
          setImmediate(resolve);
        }),
    );
  }
  return drained;
}

interface Controlled<T> {
  promise: Promise<T>;
  resolve: (value: PromiseOrValue<T>) => void;
  reject: (reason?: unknown) => void;
}

interface ControllableStream {
  source: AsyncIterable<unknown> & {
    next: () => Promise<IteratorResult<unknown>>;
    return: () => Promise<IteratorResult<unknown>>;
  };
  returnSpy: MethodSpy;
  /** Waits for the next not-yet-observed pull on the source iterator. */
  nextPull: () => Promise<Controlled<IteratorResult<unknown>>>;
}

/**
 * Builds an async iterator whose pulls are each settled explicitly by the
 * test, so consumption timing is driven rather than raced.
 */
function makeControllableStream(): ControllableStream {
  const pulls: Array<Controlled<IteratorResult<unknown>>> = [];
  const pullWaiters: Array<
    (pull: Controlled<IteratorResult<unknown>>) => void
  > = [];
  let awaitedPulls = 0;

  const source = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next(): Promise<IteratorResult<unknown>> {
      const pull = promiseWithResolvers<IteratorResult<unknown>>();
      pulls.push(pull);
      const waiter = pullWaiters.shift();
      if (waiter !== undefined) {
        waiter(pull);
      }
      return pull.promise;
    },
    return(): Promise<IteratorResult<unknown>> {
      return Promise.resolve({ value: undefined, done: true });
    },
  };
  const returnSpy = spyOnMethod(source, 'return');

  function nextPull(): Promise<Controlled<IteratorResult<unknown>>> {
    const pull = pulls[awaitedPulls++];
    if (pull !== undefined) {
      return Promise.resolve(pull);
    }
    return new Promise((resolve) => {
      pullWaiters.push(resolve);
    });
  }

  return { source, returnSpy, nextPull };
}

describe('public surface: cancellable incremental execution', () => {
  it('aborts before the initial payload, emitting the execute error lifecycle once', async () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    const todo = promiseWithResolvers<{ id: string }>();
    const document = parse(
      'query Q { todo { id ... @defer { author { id } } } }',
    );

    let abortedError: unknown;
    await expectEvents(
      executeChannel,
      async () => {
        const resultPromise = experimentalExecuteIncrementally({
          schema,
          document,
          rootValue: { todo: () => todo.promise },
          abortSignal: abortController.signal,
          enableEarlyExecution: true,
        });

        abortController.abort(reason);

        abortedError = await expectPromise(resultPromise).toReject();
        expect(abortedError).to.be.instanceOf(AbortedGraphQLExecutionError);
        expect((abortedError as { cause?: unknown }).cause).to.equal(reason);

        // Late settlement of in-flight work emits nothing further.
        todo.resolve({ id: '1' });
        await drainEventLoop();
      },
      () => [
        {
          channel: 'start',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: 'Q',
            operationType: OperationTypeNode.QUERY,
          },
        },
        {
          channel: 'end',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: 'Q',
            operationType: OperationTypeNode.QUERY,
          },
        },
        {
          channel: 'error',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: 'Q',
            operationType: OperationTypeNode.QUERY,
            error: abortedError,
          },
        },
        {
          channel: 'asyncStart',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: 'Q',
            operationType: OperationTypeNode.QUERY,
            error: abortedError,
          },
        },
        {
          channel: 'asyncEnd',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: 'Q',
            operationType: OperationTypeNode.QUERY,
            error: abortedError,
          },
        },
      ],
    );
  });

  it('aborts after the initial payload without further execute events and closes the source once', async () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    const stream = makeControllableStream();
    const document = parse('{ scalarList @stream(initialCount: 0) }');

    await expectEvents(
      executeChannel,
      async () => {
        const result = await experimentalExecuteIncrementally({
          schema,
          document,
          rootValue: { scalarList: () => stream.source },
          abortSignal: abortController.signal,
        });
        assert('initialResult' in result);
        expectJSON(result.initialResult).toDeepEqual({
          data: { scalarList: [] },
          pending: [{ id: '0', path: ['scalarList'] }],
          hasNext: true,
        });

        const iterator = result.subsequentResults[Symbol.asyncIterator]();
        const nextPromise = iterator.next();
        const pull = await stream.nextPull();
        abortController.abort(reason);

        const error = await expectPromise(nextPromise).toReject();
        expect(error).to.equal(reason);

        // Late items are ignored; the stream reports done afterwards.
        pull.resolve({ value: 'late', done: false });
        await drainEventLoop();
        expect(await iterator.next()).to.deep.equal({
          value: undefined,
          done: true,
        });
        expect(stream.returnSpy.callCount).to.equal(1);
        return result;
      },
      (result) => [
        {
          channel: 'start',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
          },
        },
        {
          channel: 'end',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
          },
        },
        {
          channel: 'asyncStart',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
            result,
          },
        },
        {
          channel: 'asyncEnd',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
            result,
          },
        },
      ],
    );
  });

  it('completes fully with natural iterator exhaustion and a single execute lifecycle', async () => {
    const stream = makeControllableStream();
    const document = parse('{ scalarList @stream(initialCount: 0) }');

    await expectEvents(
      executeChannel,
      async () => {
        const result = await experimentalExecuteIncrementally({
          schema,
          document,
          rootValue: { scalarList: () => stream.source },
        });
        assert('initialResult' in result);

        const delivered: Array<unknown> = [];
        const consume = (async () => {
          for await (const patch of result.subsequentResults) {
            delivered.push(patch);
          }
        })();

        (await stream.nextPull()).resolve({ value: 'a', done: false });
        // Let the item patch flush before completing the source.
        await drainEventLoop();
        (await stream.nextPull()).resolve({ value: undefined, done: true });
        await consume;
        await drainEventLoop();

        expectJSON(delivered).toDeepEqual([
          { hasNext: true, incremental: [{ id: '0', items: ['a'] }] },
          { hasNext: false, completed: [{ id: '0' }] },
        ]);
        // Natural completion needs no early close of the source iterator.
        expect(stream.returnSpy.callCount).to.equal(0);
        return result;
      },
      (result) => [
        {
          channel: 'start',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
          },
        },
        {
          channel: 'end',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
          },
        },
        {
          channel: 'asyncStart',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
            result,
          },
        },
        {
          channel: 'asyncEnd',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
            result,
          },
        },
      ],
    );
  });
});

describe('public surface: resolver failure lifecycle', () => {
  it('formats the failure into the result while resolve publishes one error lifecycle', async () => {
    const failure = new Error('async-boom');
    const document = parse('{ fail }');

    await expectEvents(
      resolveChannel,
      () =>
        expectEvents(
          executeChannel,
          () =>
            execute({
              schema,
              document,
              rootValue: { fail: () => Promise.reject(failure) },
            }),
          (result) => {
            expectJSON(result).toDeepEqual({
              data: { fail: null },
              errors: [
                {
                  message: 'async-boom',
                  locations: [{ line: 1, column: 3 }],
                  path: ['fail'],
                },
              ],
            });
            return [
              {
                channel: 'start',
                context: {
                  schema,
                  document,
                  rawVariableValues: undefined,
                  operationName: undefined,
                  operationType: OperationTypeNode.QUERY,
                },
              },
              {
                channel: 'end',
                context: {
                  schema,
                  document,
                  rawVariableValues: undefined,
                  operationName: undefined,
                  operationType: OperationTypeNode.QUERY,
                },
              },
              {
                channel: 'asyncStart',
                context: {
                  schema,
                  document,
                  rawVariableValues: undefined,
                  operationName: undefined,
                  operationType: OperationTypeNode.QUERY,
                  result,
                },
              },
              {
                channel: 'asyncEnd',
                context: {
                  schema,
                  document,
                  rawVariableValues: undefined,
                  operationName: undefined,
                  operationType: OperationTypeNode.QUERY,
                  result,
                },
              },
            ];
          },
        ),
      () => [
        {
          channel: 'start',
          context: {
            fieldName: 'fail',
            alias: 'fail',
            parentType: 'Query',
            fieldType: 'String',
            args: {},
            isDefaultResolver: true,
            fieldPath: 'fail',
          },
        },
        {
          channel: 'end',
          context: {
            fieldName: 'fail',
            alias: 'fail',
            parentType: 'Query',
            fieldType: 'String',
            args: {},
            isDefaultResolver: true,
            fieldPath: 'fail',
          },
        },
        {
          channel: 'error',
          context: {
            fieldName: 'fail',
            alias: 'fail',
            parentType: 'Query',
            fieldType: 'String',
            args: {},
            isDefaultResolver: true,
            fieldPath: 'fail',
            error: failure,
          },
        },
        {
          channel: 'asyncStart',
          context: {
            fieldName: 'fail',
            alias: 'fail',
            parentType: 'Query',
            fieldType: 'String',
            args: {},
            isDefaultResolver: true,
            fieldPath: 'fail',
            error: failure,
          },
        },
        {
          channel: 'asyncEnd',
          context: {
            fieldName: 'fail',
            alias: 'fail',
            parentType: 'Query',
            fieldType: 'String',
            args: {},
            isDefaultResolver: true,
            fieldPath: 'fail',
            error: failure,
          },
        },
      ],
    );
  });
});

describe('public surface: cancellable subscription', () => {
  it('aborts consumption without further subscribe events and closes the source once', async () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    const pending = promiseWithResolvers<IteratorResult<unknown>>();

    let nextCount = 0;
    let returnCount = 0;
    const source = {
      [Symbol.asyncIterator]() {
        return this;
      },
      next(): Promise<IteratorResult<unknown>> {
        nextCount++;
        if (nextCount === 1) {
          return Promise.resolve({ value: { counter: 1 }, done: false });
        }
        return pending.promise;
      },
      return(): Promise<IteratorResult<unknown>> {
        returnCount++;
        // Pubsub-style cleanup releases the pending pull.
        pending.resolve({ value: undefined, done: true });
        return Promise.resolve({ value: undefined, done: true });
      },
    };
    const document = parse('subscription { counter }');

    await expectEvents(
      subscribeChannel,
      async () => {
        const stream = await subscribe({
          schema,
          document,
          rootValue: { counter: () => Promise.resolve(source) },
          abortSignal: abortController.signal,
        });
        assert('next' in stream);

        expectJSON(await stream.next()).toDeepEqual({
          value: { data: { counter: 1 } },
          done: false,
        });

        const nextPromise = stream.next();
        abortController.abort(reason);

        const error = await expectPromise(nextPromise).toReject();
        expect(error).to.equal(reason);

        // Closing the response stream closes the source stream exactly once.
        await stream.return();
        await stream.return();
        expect(returnCount).to.equal(1);

        // Cancellation is sticky: later next() calls reject instead of hanging.
        const lateNextError = await expectPromise(stream.next()).toReject();
        expect(lateNextError).to.equal(reason);

        await drainEventLoop();
        return stream;
      },
      (stream) => [
        {
          channel: 'start',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.SUBSCRIPTION,
          },
        },
        {
          channel: 'end',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.SUBSCRIPTION,
          },
        },
        {
          channel: 'asyncStart',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.SUBSCRIPTION,
            result: stream,
          },
        },
        {
          channel: 'asyncEnd',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.SUBSCRIPTION,
            result: stream,
          },
        },
      ],
    );
  });
});

describe('public surface: execute with a pre-aborted signal', () => {
  it('throws the abort reason synchronously, emitting start, error, and end once', async () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    abortController.abort(reason);
    const document = parse('{ todo { id } }');

    let resolverCalled = false;
    let thrown: unknown;
    await expectEvents(
      executeChannel,
      () => {
        thrown = expectToThrow(() =>
          execute({
            schema,
            document,
            rootValue: {
              todo: () => {
                resolverCalled = true;
                return { id: '1' };
              },
            },
            abortSignal: abortController.signal,
          }),
        );
      },
      () => [
        {
          channel: 'start',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
          },
        },
        {
          channel: 'error',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
            error: thrown,
          },
        },
        {
          channel: 'end',
          context: {
            schema,
            document,
            rawVariableValues: undefined,
            operationName: undefined,
            operationType: OperationTypeNode.QUERY,
            error: thrown,
          },
        },
      ],
    );

    expect(thrown).to.equal(reason);
    expect(resolverCalled).to.equal(false);
  });
});
