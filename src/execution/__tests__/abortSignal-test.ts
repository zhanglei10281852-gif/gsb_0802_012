import { describe, it } from 'node:test';

import { assert, expect } from 'chai';

import { expectJSON } from '../../__testUtils__/expectJSON.ts';
import { expectPromise } from '../../__testUtils__/expectPromise.ts';
import { resolveOnNextTick } from '../../__testUtils__/resolveOnNextTick.ts';
import { spyOnMethod } from '../../__testUtils__/spyOn.ts';

import { promiseWithResolvers } from '../../jsutils/promiseWithResolvers.ts';

import { parse } from '../../language/parser.ts';

import { buildSchema } from '../../utilities/buildASTSchema.ts';

import { AbortedGraphQLExecutionError } from '../AbortedGraphQLExecutionError.ts';
import {
  execute,
  experimentalExecuteIncrementally,
  subscribe,
} from '../execute.ts';
import type {
  ExperimentalIncrementalExecutionResults,
  InitialIncrementalExecutionResult,
  SubsequentIncrementalExecutionResult,
} from '../incremental/IncrementalExecutor.ts';

/**
 * Public-behavior regression suite for "stop executing after the client
 * disconnects".
 *
 * These tests exercise only the exported runtime surface (`execute`,
 * `subscribe`, and `experimentalExecuteIncrementally`) together with a caller
 * supplied `AbortSignal`. Timing is driven entirely by hand-controlled
 * Promises and AsyncIterators, so there is no reliance on the network, real
 * timers, `setTimeout`-based sleeps, or the executor's internal classes.
 */

const schema = buildSchema(`
  type Todo {
    id: ID
    author: User
    items: [String]
    nonNullName: String!
  }

  type User {
    id: ID
    name: String
  }

  type Query {
    todo: Todo
    nonNullableTodo: Todo!
    blocker: String
    scalarList: [String]
    slowScalarList: [String]
  }

  type Subscription {
    events: String
  }
`);

/** A promise that never settles; used to hold a resolver open until aborted. */
function neverResolves<T = unknown>(): Promise<T> {
  return new Promise<T>(() => {
    /* never resolves */
  });
}

/** Drain an incremental payload stream into an array of results. */
async function collectIncremental(
  result: ExperimentalIncrementalExecutionResults,
): Promise<
  Array<
    InitialIncrementalExecutionResult | SubsequentIncrementalExecutionResult
  >
> {
  const results: Array<
    InitialIncrementalExecutionResult | SubsequentIncrementalExecutionResult
  > = [result.initialResult];
  for await (const patch of result.subsequentResults) {
    results.push(patch);
  }
  return results;
}

/** Yield to a macrotask turn without relying on a real timer delay. */
function flushMacrotask(): Promise<void> {
  return new Promise((resolve) => {
    // eslint-disable-next-line no-undef
    setImmediate(resolve);
  });
}

/**
 * Run `fn` while capturing any `unhandledRejection` that escapes to the
 * process. Returns the captured reason (or `null` when none occurred) after
 * giving late rejections a couple of microtask/macrotask turns to surface.
 */
async function captureUnhandledRejection(
  fn: () => Promise<void>,
): Promise<unknown> {
  let unhandledRejection: unknown = null;
  const listener = (reason: unknown) => {
    unhandledRejection = reason;
  };
  // eslint-disable-next-line no-undef
  process.on('unhandledRejection', listener);
  try {
    await fn();
    // Give any late rejection a couple of macrotask turns to be reported as
    // unhandled. `setImmediate` avoids depending on a real timer delay.
    await resolveOnNextTick();
    await flushMacrotask();
    await flushMacrotask();
  } finally {
    // eslint-disable-next-line no-undef
    process.removeListener('unhandledRejection', listener);
  }
  return unhandledRejection;
}

describe('Execute: client disconnect cancellation', () => {
  describe('execute() with a non-incremental operation', () => {
    it('throws synchronously when the signal is already aborted before start', () => {
      const abortController = new AbortController();
      abortController.abort();

      expect(() =>
        execute({
          schema,
          document: parse('{ blocker }'),
          rootValue: {
            /* node:coverage ignore next */
            blocker: () => expect.fail('resolver should not run'),
          },
          abortSignal: abortController.signal,
        }),
      ).to.throw('This operation was aborted');
    });

    it('propagates a caller-supplied abort reason before start', () => {
      const abortController = new AbortController();
      const reason = new Error('client navigated away');
      abortController.abort(reason);

      expect(() =>
        execute({
          schema,
          document: parse('{ blocker }'),
          abortSignal: abortController.signal,
        }),
      ).to.throw(reason);
    });

    it('rejects with the default reason when aborted while a resolver is pending', async () => {
      const abortController = new AbortController();
      const resultPromise = execute({
        schema,
        document: parse('{ blocker }'),
        rootValue: {
          blocker: () => neverResolves<string>(),
        },
        abortSignal: abortController.signal,
      });

      abortController.abort();

      const error = await expectPromise(resultPromise).toReject();
      expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
      expect(error).to.include({ message: 'This operation was aborted' });
    });

    it('rejects with an AbortedGraphQLExecutionError carrying the reason and partial result', async () => {
      const abortController = new AbortController();
      const reason = new Error('client gone');
      const { promise: blocked, resolve: unblock } =
        promiseWithResolvers<string>();

      const resultPromise = execute({
        schema,
        document: parse('{ a: blocker b: blocker }'),
        rootValue: {
          blocker: () => blocked,
        },
        abortSignal: abortController.signal,
      });

      abortController.abort(reason);

      const error = await expectPromise(resultPromise).toReject();
      assert(error instanceof AbortedGraphQLExecutionError);
      expect(error.message).to.equal('client gone');
      expect(error.cause).to.equal(reason);

      // Releasing the resolver lets the partial result settle.
      unblock('done');
      expectJSON(await error.abortedResult).toDeepEqual({
        errors: [
          { message: 'Aborted!', locations: [{ line: 1, column: 3 }], path: ['a'] },
          {
            message: 'Aborted!',
            locations: [{ line: 1, column: 14 }],
            path: ['b'],
          },
        ],
        data: { a: null, b: null },
      });
    });

    it('stops waiting on a pending list item when aborted', async () => {
      const abortController = new AbortController();
      const resultPromise = execute({
        schema,
        document: parse('{ scalarList }'),
        rootValue: {
          scalarList: () => [neverResolves<string>()],
        },
        abortSignal: abortController.signal,
      });

      abortController.abort();

      const error = await expectPromise(resultPromise).toReject();
      expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
      expect(error).to.include({ message: 'This operation was aborted' });
    });

    it('bubbles a null through a non-null field into the aborted partial result', async () => {
      const abortController = new AbortController();
      const { promise: blocked, resolve: unblock } =
        promiseWithResolvers<string>();

      const resultPromise = execute({
        schema,
        document: parse('{ todo { id nonNullName } }'),
        rootValue: {
          todo: { id: '1', nonNullName: () => blocked },
        },
        abortSignal: abortController.signal,
      });

      abortController.abort();

      const error = await expectPromise(resultPromise).toReject();
      assert(error instanceof AbortedGraphQLExecutionError);

      unblock('too late');
      // The non-null field error bubbles to its nearest nullable parent
      // (`todo`) rather than nulling the whole response.
      expectJSON(await error.abortedResult).toDeepEqual({
        errors: [
          {
            message: 'Aborted!',
            locations: [{ line: 1, column: 13 }],
            path: ['todo', 'nonNullName'],
          },
        ],
        data: { todo: null },
      });
    });

    it('does not surface a late resolver rejection as an unhandled rejection', async () => {
      const abortController = new AbortController();
      const { promise: blocked, reject: failLater } =
        promiseWithResolvers<string>();
      // Pre-attach a catch so the deferred itself never counts as unhandled;
      // the executor is responsible for the promise it awaited internally.
      blocked.catch(() => undefined);

      const unhandled = await captureUnhandledRejection(async () => {
        const resultPromise = execute({
          schema,
          document: parse('{ blocker }'),
          rootValue: {
            blocker: () => blocked,
          },
          abortSignal: abortController.signal,
        });

        abortController.abort();
        await expectPromise(resultPromise).toRejectWith(
          'This operation was aborted',
        );

        // The resolver rejects only after cancellation already settled.
        failLater(new Error('late resolver failure'));
      });

      expect(unhandled).to.equal(null);
    });
  });

  describe('subscribe()', () => {
    it('stops establishing the source stream when aborted during subscribe', async () => {
      const abortController = new AbortController();
      const subscriptionPromise = subscribe({
        schema,
        document: parse('subscription { events }'),
        rootValue: {
          events: () => neverResolves<AsyncIterable<unknown>>(),
        },
        abortSignal: abortController.signal,
      });

      abortController.abort();

      const result = await subscriptionPromise;
      assert(!(Symbol.asyncIterator in Object(result)));
      expectJSON(result).toDeepEqual({
        errors: [
          {
            message: 'This operation was aborted',
            locations: [{ line: 1, column: 16 }],
            path: ['events'],
          },
        ],
      });
    });

    it('rejects a pending consumption when aborted, propagating the reason', async () => {
      const abortController = new AbortController();
      const reason = new Error('socket closed');
      const { promise: nextStarted, resolve: signalNextStarted } =
        // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
        promiseWithResolvers<void>();

      const source = {
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          signalNextStarted();
          return neverResolves<IteratorResult<unknown>>();
        },
        return() {
          return Promise.resolve({ value: undefined, done: true as const });
        },
      };

      const subscription = await subscribe({
        schema,
        document: parse('subscription { events }'),
        rootValue: { events: () => source },
        abortSignal: abortController.signal,
      });
      assert(Symbol.asyncIterator in subscription);

      const nextPromise = subscription.next();
      await nextStarted;
      abortController.abort(reason);

      const error = await expectPromise(nextPromise).toReject();
      expect(error).to.equal(reason);
    });

    it('delivers events normally when the signal is never aborted', async () => {
      const abortController = new AbortController();

      async function* events() {
        yield await Promise.resolve({ events: 'first' });
        yield await Promise.resolve({ events: 'second' });
      }

      const subscription = await subscribe({
        schema,
        document: parse('subscription { events }'),
        rootValue: { events: () => events() },
        abortSignal: abortController.signal,
      });
      assert(Symbol.asyncIterator in subscription);

      expectJSON(await subscription.next()).toDeepEqual({
        done: false,
        value: { data: { events: 'first' } },
      });
      expectJSON(await subscription.next()).toDeepEqual({
        done: false,
        value: { data: { events: 'second' } },
      });
      expectJSON(await subscription.next()).toDeepEqual({
        done: true,
        value: undefined,
      });
    });
  });

  describe('experimentalExecuteIncrementally() with @defer / @stream', () => {
    it('produces an initial result and rejects a pending subsequent payload once aborted', async () => {
      const abortController = new AbortController();
      const { promise: authorStarted, resolve: signalAuthorStarted } =
        // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
        promiseWithResolvers<void>();

      const result = await experimentalExecuteIncrementally({
        schema,
        document: parse('{ todo { id ... @defer { author { id } } } }'),
        rootValue: {
          todo: {
            id: '1',
            author() {
              signalAuthorStarted();
              return neverResolves<{ id: string }>();
            },
          },
        },
        enableEarlyExecution: true,
        abortSignal: abortController.signal,
      });
      assert('initialResult' in result);

      expectJSON(result.initialResult).toDeepEqual({
        data: { todo: { id: '1' } },
        pending: [{ id: '0', path: ['todo'] }],
        hasNext: true,
      });

      const iterator = result.subsequentResults[Symbol.asyncIterator]();
      const nextPromise = iterator.next();
      await authorStarted;
      abortController.abort();

      await expectPromise(nextPromise).toRejectWith('This operation was aborted');
    });

    it('completes the whole incremental response when never aborted', async () => {
      const abortController = new AbortController();
      const result = await experimentalExecuteIncrementally({
        schema,
        document: parse('{ todo { id ... @defer { author { id } } } }'),
        rootValue: {
          todo: { id: '1', author: { id: '2' } },
        },
        abortSignal: abortController.signal,
      });
      assert('initialResult' in result);

      const payloads = await collectIncremental(result);
      expectJSON(payloads).toDeepEqual([
        {
          data: { todo: { id: '1' } },
          pending: [{ id: '0', path: ['todo'] }],
          hasNext: true,
        },
        {
          incremental: [{ data: { author: { id: '2' } }, id: '0' }],
          completed: [{ id: '0' }],
          hasNext: false,
        },
      ]);
    });

    it('cleans up a pending @stream source iterator exactly once when aborted', async () => {
      const abortController = new AbortController();
      const { promise: nextStarted, resolve: signalNextStarted } =
        // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
        promiseWithResolvers<void>();

      let exhausted = false;
      const source = {
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          if (exhausted) {
            return Promise.resolve({ value: undefined, done: true as const });
          }
          exhausted = true;
          signalNextStarted();
          return neverResolves<IteratorResult<string>>();
        },
        return() {
          return Promise.resolve({ value: undefined, done: true as const });
        },
      };
      const returnSpy = spyOnMethod(source, 'return');

      const result = await experimentalExecuteIncrementally({
        schema,
        document: parse('{ scalarList @stream(initialCount: 0) }'),
        rootValue: { scalarList: () => source },
        abortSignal: abortController.signal,
      });
      assert('initialResult' in result);

      const iterator = result.subsequentResults[Symbol.asyncIterator]();
      const nextPromise = iterator.next();
      await nextStarted;
      abortController.abort();

      await expectPromise(nextPromise).toRejectWith('This operation was aborted');

      // Draining afterwards completes and does not re-invoke cleanup.
      await resolveOnNextTick();
      const followUp = await iterator.next();
      expect(followUp.done).to.equal(true);
      expect(returnSpy.callCount).to.equal(1);

      // Repeated aborts are ignored and never trigger a second cleanup.
      abortController.abort();
      await resolveOnNextTick();
      expect(returnSpy.callCount).to.equal(1);
    });

    it('does not surface a late deferred rejection as an unhandled rejection', async () => {
      const abortController = new AbortController();
      const { promise: authorStarted, resolve: signalAuthorStarted } =
        // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
        promiseWithResolvers<void>();
      const { promise: authorResult, reject: failAuthorLater } =
        promiseWithResolvers<{ id: string }>();

      const unhandled = await captureUnhandledRejection(async () => {
        const result = await experimentalExecuteIncrementally({
          schema,
          document: parse('{ todo { id ... @defer { author { id } } } }'),
          rootValue: {
            todo: {
              id: '1',
              author() {
                signalAuthorStarted();
                return authorResult;
              },
            },
          },
          enableEarlyExecution: true,
          abortSignal: abortController.signal,
        });
        assert('initialResult' in result);

        const iterator = result.subsequentResults[Symbol.asyncIterator]();
        const nextPromise = iterator.next();
        await authorStarted;
        abortController.abort();

        await expectPromise(nextPromise).toRejectWith(
          'This operation was aborted',
        );

        // The deferred field rejects only after cancellation settled.
        failAuthorLater(new Error('late deferred failure'));
        await expectPromise(authorResult).toRejectWith('late deferred failure');
      });

      expect(unhandled).to.equal(null);
    });
  });
});
