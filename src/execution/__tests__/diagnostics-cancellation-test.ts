import { describe, it } from 'node:test';

import { assert, expect } from 'chai';

import type { TestTracingChannel } from '../../__testUtils__/diagnosticsTracing.ts';
import { expectPromise } from '../../__testUtils__/expectPromise.ts';
import { getTracingChannel } from '../../__testUtils__/getTracingChannel.ts';
import { resolveOnNextTick } from '../../__testUtils__/resolveOnNextTick.ts';
import { spyOnMethod } from '../../__testUtils__/spyOn.ts';

import { isAsyncIterable } from '../../jsutils/isAsyncIterable.ts';
import { promiseWithResolvers } from '../../jsutils/promiseWithResolvers.ts';

import { parse } from '../../language/parser.ts';

import { buildSchema } from '../../utilities/buildASTSchema.ts';

import { experimentalExecuteIncrementally, subscribe } from '../execute.ts';
import type {
  InitialIncrementalExecutionResult,
  SubsequentIncrementalExecutionResult,
} from '../incremental/IncrementalExecutor.ts';

/**
 * Diagnostics-channel regression for the client-disconnect cancellation
 * feature.
 *
 * This unit layer confirms that a cancellable incremental query and a
 * cancellable subscription, driven through the public execution exports,
 * publish a well-formed `graphql:execute` / `graphql:subscribe` lifecycle on
 * the real `node:diagnostics_channel` across three paths: normal completion,
 * user cancellation, and resolver failure. For each path it asserts that the
 * lifecycle terminates exactly once (a single `start`, a single terminal
 * `end`/`asyncEnd`, and no repeated terminal events) and that every async
 * iterator involved is cleaned up without leaking active work.
 *
 * Timing is driven only by hand-controlled Promises and AsyncIterators - no
 * network, real timers, or executor internals - matching the round-1 rules.
 *
 * The process-level counterpart in `resources/diagnostics-cancellation-*`
 * runs the same three paths against the built `npmDist` ESM entry.
 */

const schema = buildSchema(`
  type Widget {
    id: ID
    slow: String
    items: [String]
    nonNullName: String!
  }

  type Query {
    widget: Widget
  }

  type Subscription {
    ticks: String
  }
`);

const executeChannel = getTracingChannel('graphql:execute');
const subscribeChannel = getTracingChannel('graphql:subscribe');

type LifecyclePhase = 'start' | 'end' | 'asyncStart' | 'asyncEnd' | 'error';

/**
 * Subscribe to a tracing channel and record the ordered lifecycle phases
 * emitted while a scope is active. Always unsubscribes.
 */
async function recordLifecycle<TContext>(
  channel: TestTracingChannel<TContext>,
  fn: () => Promise<void>,
): Promise<Array<LifecyclePhase>> {
  const phases: Array<LifecyclePhase> = [];
  const handler = {
    start: () => phases.push('start'),
    end: () => phases.push('end'),
    asyncStart: () => phases.push('asyncStart'),
    asyncEnd: () => phases.push('asyncEnd'),
    error: () => phases.push('error'),
  };
  channel.subscribe(handler);
  try {
    await fn();
  } finally {
    channel.unsubscribe(handler);
  }
  return phases;
}

/** A promise that never settles; used to hold a resolver open until aborted. */
function neverResolves<T = unknown>(): Promise<T> {
  return new Promise<T>(() => {
    /* never resolves */
  });
}

/**
 * A hand-controlled async iterator for `@stream` fixtures that yields a fixed
 * prefix, then parks on a never-resolving `next()`. Exposes a signal for when
 * the parked call begins and a spy over `return()` cleanup.
 */
function controllableStream(prefix: ReadonlyArray<string>): {
  iterable: AsyncIterable<string>;
  parkedNextStarted: Promise<void>;
  returnCallCount: () => number;
} {
  const { promise: parkedNextStarted, resolve: signalParked } =
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
    promiseWithResolvers<void>();
  let index = 0;
  const iterable = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next(): Promise<IteratorResult<string>> {
      if (index < prefix.length) {
        const value = prefix[index];
        index++;
        return Promise.resolve({ value, done: false });
      }
      signalParked();
      return neverResolves<IteratorResult<string>>();
    },
    return(): Promise<IteratorResult<string>> {
      return Promise.resolve({ value: undefined, done: true });
    },
  };
  const returnSpy = spyOnMethod(iterable, 'return');
  return {
    iterable,
    parkedNextStarted,
    returnCallCount: () => returnSpy.callCount,
  };
}

/** Assert a lifecycle has exactly one start and one terminal event. */
function expectSingleTermination(phases: ReadonlyArray<LifecyclePhase>): void {
  expect(phases.filter((p) => p === 'start')).to.have.lengthOf(1);

  const terminalCount =
    phases.filter((p) => p === 'end').length +
    phases.filter((p) => p === 'asyncEnd').length;
  // `end` closes the synchronous span; `asyncEnd` closes an async tail. A
  // promise-returning call emits both exactly once; a synchronous call emits
  // only `end`. Either way there must be no repeated terminal of a given kind.
  expect(phases.filter((p) => p === 'end')).to.have.lengthOf(1);
  expect(phases.filter((p) => p === 'asyncEnd').length).to.be.lessThanOrEqual(
    1,
  );
  expect(terminalCount).to.be.greaterThan(0);
}

const incrementalDocument = parse(`
  query {
    widget {
      id
      ... @defer(label: "slow") {
        slow
      }
      items @stream(initialCount: 0)
    }
  }
`);

describe('diagnostics lifecycle for cancellable execution (unit)', () => {
  describe('cancellable incremental query on graphql:execute', () => {
    it('normal completion emits a single async lifecycle and drains all iterators', async () => {
      // An async source stream makes the initial phase asynchronous, so the
      // traced call resolves a promise and the async tail fires.
      async function* items() {
        yield await Promise.resolve('a');
        yield 'b';
      }

      let payloadCount = 0;
      const phases = await recordLifecycle(executeChannel, async () => {
        const abortController = new AbortController();
        const result = await experimentalExecuteIncrementally({
          schema,
          document: incrementalDocument,
          rootValue: {
            widget: { id: '1', slow: () => 'slow-value', items: () => items() },
          },
          abortSignal: abortController.signal,
        });
        assert('initialResult' in result);
        for await (const _patch of result.subsequentResults) {
          payloadCount++;
        }
      });

      // Deferred + streamed payloads were all delivered.
      expect(payloadCount).to.be.greaterThan(0);
      expect(phases).to.deep.equal(['start', 'end', 'asyncStart', 'asyncEnd']);
      expectSingleTermination(phases);
      expect(phases).to.not.include('error');
    });

    it('user cancellation emits a single terminal lifecycle and cleans up the stream once', async () => {
      const stream = controllableStream(['a']);
      const { promise: slowStarted, resolve: signalSlowStarted } =
        // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
        promiseWithResolvers<void>();
      const { promise: slow } = promiseWithResolvers<string>();

      let cancellationError: unknown;
      const phases = await recordLifecycle(executeChannel, async () => {
        const abortController = new AbortController();
        const result = await experimentalExecuteIncrementally({
          schema,
          document: incrementalDocument,
          rootValue: {
            widget: {
              id: '1',
              slow() {
                signalSlowStarted();
                return slow;
              },
              items: () => stream.iterable,
            },
          },
          enableEarlyExecution: true,
          abortSignal: abortController.signal,
        });
        assert('initialResult' in result);

        const iterator = result.subsequentResults[Symbol.asyncIterator]();
        // Deliver the one streamed item, then race cancellation against the
        // parked stream and pending deferred field.
        await iterator.next();
        const nextPromise = iterator.next();
        await slowStarted;
        await stream.parkedNextStarted;
        abortController.abort();

        cancellationError = await expectPromise(nextPromise).toReject();

        // Draining after cancellation completes and does not re-clean up.
        await resolveOnNextTick();
        const followUp = await iterator.next();
        expect(followUp.done).to.equal(true);
      });

      expect((cancellationError as Error).message).to.equal(
        'This operation was aborted',
      );
      // The initial phase is async here (the parked stream makes the traced
      // call return a promise), so the async tail closes it - each phase once.
      expect(phases).to.deep.equal(['start', 'end', 'asyncStart', 'asyncEnd']);
      expectSingleTermination(phases);
      // The parked stream was returned exactly once - no leaked active iterator.
      expect(stream.returnCallCount()).to.equal(1);
    });

    it('resolver failure inside a deferred patch reports on the patch, not the traced error lifecycle', async () => {
      const document = parse(`
        query {
          widget {
            id
            ... @defer(label: "boom") {
              nonNullName
            }
          }
        }
      `);

      const patches: Array<
        InitialIncrementalExecutionResult | SubsequentIncrementalExecutionResult
      > = [];
      const phases = await recordLifecycle(executeChannel, async () => {
        const result = await experimentalExecuteIncrementally({
          schema,
          document,
          rootValue: {
            widget: {
              id: '1',
              nonNullName: () => Promise.reject(new Error('resolver exploded')),
            },
          },
        });
        assert('initialResult' in result);
        patches.push(result.initialResult);
        for await (const patch of result.subsequentResults) {
          patches.push(patch);
        }
      });

      // The deferred field's non-null failure is reported on the patch, and
      // the overall execution still completes (no thrown error lifecycle).
      const completed = patches
        .flatMap((patch) => ('completed' in patch ? patch.completed : []))
        .find((entry) => entry.id === '0');
      assert(completed !== undefined);
      assert('errors' in completed && completed.errors !== undefined);
      expect(completed.errors[0].message).to.equal('resolver exploded');

      // The initial phase resolves synchronously (the root object is plain and
      // the failing field is deferred), so only start/end fire - exactly once.
      expect(phases).to.deep.equal(['start', 'end']);
      expectSingleTermination(phases);
      // A collected field error is not the traced-call `error` lifecycle.
      expect(phases).to.not.include('error');
    });

    it('cancelling before the initial payload publishes the error lifecycle exactly once', async () => {
      // When the root selection resolves via a promise, aborting before the
      // initial payload rejects the traced call, so the `error` lifecycle and
      // the async tail each fire once with no duplicate terminal.
      const { promise: widgetStarted, resolve: signalWidgetStarted } =
        // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
        promiseWithResolvers<void>();
      const { promise: widget, resolve: resolveWidget } = promiseWithResolvers<{
        id: string;
      }>();

      let cancellationError: unknown;
      const phases = await recordLifecycle(executeChannel, async () => {
        const abortController = new AbortController();
        const resultPromise = experimentalExecuteIncrementally({
          schema,
          document: parse(`
            query {
              widget {
                id
                ... @defer(label: "slow") { slow }
              }
            }
          `),
          rootValue: {
            widget() {
              signalWidgetStarted();
              return widget;
            },
          },
          abortSignal: abortController.signal,
        });

        await widgetStarted;
        abortController.abort();

        cancellationError = await expectPromise(resultPromise).toReject();

        // A late resolution is ignored and must not reopen the lifecycle.
        resolveWidget({ id: '1' });
        await resolveOnNextTick();
      });

      expect((cancellationError as Error).message).to.equal(
        'This operation was aborted',
      );
      expect(phases).to.deep.equal([
        'start',
        'end',
        'error',
        'asyncStart',
        'asyncEnd',
      ]);
      expectSingleTermination(phases);
      expect(phases.filter((p) => p === 'error')).to.have.lengthOf(1);
    });
  });

  describe('cancellable subscription on graphql:subscribe', () => {
    async function* ticks(): AsyncIterable<{ ticks: string }> {
      await Promise.resolve();
      yield { ticks: 'one' };
      yield { ticks: 'two' };
    }

    it('normal completion emits a single start/end and the stream returns cleanly', async () => {
      let returnResult: IteratorResult<unknown> | undefined;
      const phases = await recordLifecycle(subscribeChannel, async () => {
        const abortController = new AbortController();
        const subscription = await subscribe({
          schema,
          document: parse('subscription { ticks }'),
          rootValue: { ticks },
          abortSignal: abortController.signal,
        });
        assert(isAsyncIterable(subscription));

        const first = await subscription.next();
        expect(first).to.deep.equal({
          done: false,
          value: { data: { ticks: 'one' } },
        });

        returnResult = await subscription.return();
      });

      expect(returnResult).to.deep.equal({ done: true, value: undefined });
      // Synchronous subscription setup: start + end only.
      expect(phases).to.deep.equal(['start', 'end']);
      expectSingleTermination(phases);
    });

    it('user cancellation rejects the pending consumption without a double terminate', async () => {
      const { promise: nextStarted, resolve: signalNextStarted } =
        // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
        promiseWithResolvers<void>();
      const source = {
        [Symbol.asyncIterator]() {
          return this;
        },
        next(): Promise<IteratorResult<{ ticks: string }>> {
          signalNextStarted();
          return neverResolves<IteratorResult<{ ticks: string }>>();
        },
        return(): Promise<IteratorResult<{ ticks: string }>> {
          return Promise.resolve({ value: undefined, done: true as const });
        },
      };
      const returnSpy = spyOnMethod(source, 'return');

      let cancellationError: unknown;
      const phases = await recordLifecycle(subscribeChannel, async () => {
        const abortController = new AbortController();
        const subscription = await subscribe({
          schema,
          document: parse('subscription { ticks }'),
          rootValue: { ticks: () => source },
          abortSignal: abortController.signal,
        });
        assert(isAsyncIterable(subscription));

        const nextPromise = subscription.next();
        await nextStarted;
        abortController.abort();

        cancellationError = await expectPromise(nextPromise).toReject();
      });

      expect((cancellationError as Error).message).to.equal(
        'This operation was aborted',
      );
      // Setup completed synchronously and terminated once; consumption-time
      // cancellation does not publish extra lifecycle events on this channel.
      expect(phases).to.deep.equal(['start', 'end']);
      expectSingleTermination(phases);
      // Cancellation does not itself invoke the source `return`; the pending
      // consumption simply rejects, leaving no duplicate cleanup.
      expect(returnSpy.callCount).to.equal(0);
    });

    it('resolver failure at subscription setup emits a single lifecycle and yields an error result', async () => {
      let result: unknown;
      const phases = await recordLifecycle(subscribeChannel, async () => {
        const abortController = new AbortController();
        result = await subscribe({
          schema,
          document: parse('subscription { ticks }'),
          rootValue: {
            ticks: () => {
              throw new Error('subscription setup failed');
            },
          },
          abortSignal: abortController.signal,
        });
      });

      // Setup failures are returned as an errors-only result, not thrown.
      assert(!isAsyncIterable(result));
      expect(
        (result as { errors: ReadonlyArray<{ message: string }> }).errors[0]
          .message,
      ).to.equal('subscription setup failed');
      expect(phases).to.deep.equal(['start', 'end']);
      expectSingleTermination(phases);
    });
  });
});
