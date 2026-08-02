import { describe, it } from 'node:test';

import { assert, expect } from 'chai';

import { expectJSON } from '../../__testUtils__/expectJSON.ts';
import { expectPromise } from '../../__testUtils__/expectPromise.ts';
import type { MethodSpy } from '../../__testUtils__/spyOn.ts';
import { spyOnMethod } from '../../__testUtils__/spyOn.ts';

import type { PromiseOrValue } from '../../jsutils/PromiseOrValue.ts';
import { promiseWithResolvers } from '../../jsutils/promiseWithResolvers.ts';

import { GraphQLError } from '../../error/GraphQLError.ts';

import { parse } from '../../language/parser.ts';

import type { GraphQLResolveInfo } from '../../type/definition.ts';

import { buildSchema } from '../../utilities/buildASTSchema.ts';

import { AbortedGraphQLExecutionError } from '../AbortedGraphQLExecutionError.ts';
import {
  execute,
  experimentalExecuteIncrementally,
  subscribe,
} from '../execute.ts';
import type { ExecutionResult } from '../Executor.ts';

const schema = buildSchema(`
  type Todo {
    id: ID
    author: User
    items: [String]
  }

  type User {
    id: ID
    name: String
    nonNullName: String!
  }

  type Query {
    todo: Todo
    nonNullableTodo: Todo!
    scalarList: [String]
  }

  type Mutation {
    first: Todo
    second: Todo
  }

  type Subscription {
    counter: Int
  }
`);

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

/**
 * Observes process-level unhandled rejections raised while the watched scope
 * runs, so tests can prove that late failures after cancellation stay
 * handled. The watcher must always be stopped.
 */
function watchForUnhandledRejection(): {
  reason: () => unknown;
  stop: () => void;
} {
  let unhandledReason: unknown = null;
  const listener = (reason: unknown) => {
    unhandledReason = reason;
  };
  // eslint-disable-next-line no-undef
  process.on('unhandledRejection', listener);
  return {
    reason: () => unhandledReason,
    stop: () => {
      // eslint-disable-next-line no-undef
      process.removeListener('unhandledRejection', listener);
    },
  };
}

describe('execute: cancellation', () => {
  it('throws the abort reason synchronously when the signal is already aborted', () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    abortController.abort(reason);

    let resolverCalled = false;
    let thrown: unknown;
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

    expect(thrown).to.equal(reason);
    expect(resolverCalled).to.equal(false);
  });

  it('throws the default abort reason when the signal was aborted without one', () => {
    const abortController = new AbortController();
    abortController.abort();

    let thrown: unknown;
    try {
      execute({
        schema,
        document: parse('{ todo { id } }'),
        rootValue: { todo: { id: '1' } },
        abortSignal: abortController.signal,
      });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).to.have.property('message', 'This operation was aborted');
  });

  it('ignores aborting after execution has already completed', async () => {
    const abortController = new AbortController();

    const result = await execute({
      schema,
      document: parse('{ todo { id } }'),
      rootValue: { todo: { id: '1' } },
      abortSignal: abortController.signal,
    });

    abortController.abort(new Error('client disconnected'));

    expectJSON(result).toDeepEqual({ data: { todo: { id: '1' } } });
  });

  it('rejects with an AbortedGraphQLExecutionError when aborted while a resolver is pending', async () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    const todo = promiseWithResolvers<{ id: string }>();

    const resultPromise = execute({
      schema,
      document: parse('{ todo { id } }'),
      rootValue: { todo: () => todo.promise },
      abortSignal: abortController.signal,
    });

    abortController.abort(reason);

    const error = (await expectPromise(
      resultPromise,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;
    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(error.cause).to.equal(reason);
    expect(error.message).to.equal('client disconnected');

    // Late resolution of the pending resolver settles the partial result.
    todo.resolve({ id: '1' });
    expectJSON(await error.abortedResult).toDeepEqual({
      data: { todo: null },
      errors: [
        {
          message: 'Aborted!',
          locations: [{ line: 1, column: 3 }],
          path: ['todo'],
        },
      ],
    });
  });

  it('bubbles late non-null field failures into the partial result without unhandled rejections', async () => {
    const watcher = watchForUnhandledRejection();
    try {
      const abortController = new AbortController();
      const reason = new Error('client disconnected');
      const todo = promiseWithResolvers<{ id: string }>();

      const resultPromise = execute({
        schema,
        document: parse('{ nonNullableTodo { id } }'),
        rootValue: { nonNullableTodo: () => todo.promise },
        abortSignal: abortController.signal,
      });

      abortController.abort(reason);

      const error = (await expectPromise(
        resultPromise,
      ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;
      expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
      expect(error.cause).to.equal(reason);

      // A failure arriving after cancellation still bubbles through the
      // non-nullable field up to the root of the partial result.
      todo.reject(new Error('todo failed'));
      expectJSON(await error.abortedResult).toDeepEqual({
        data: null,
        errors: [
          {
            message: 'todo failed',
            locations: [{ line: 1, column: 3 }],
            path: ['nonNullableTodo'],
          },
        ],
      });

      await drainEventLoop();
    } finally {
      watcher.stop();
    }
    expect(watcher.reason()).to.equal(null);
  });

  it('rejects when aborted while a list item promise is pending', async () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    const item = promiseWithResolvers<string>();

    const resultPromise = execute({
      schema,
      document: parse('{ scalarList }'),
      rootValue: { scalarList: () => ['a', item.promise] },
      abortSignal: abortController.signal,
    });

    abortController.abort(reason);

    const error = (await expectPromise(
      resultPromise,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;
    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(error.cause).to.equal(reason);

    // Late settlement of the pending item completes the partial result.
    item.resolve('b');
    expectJSON(await error.abortedResult).toDeepEqual({
      data: { scalarList: ['a', null] },
      errors: [
        {
          message: 'Aborted!',
          locations: [{ line: 1, column: 3 }],
          path: ['scalarList', 1],
        },
      ],
    });
  });

  it('closes an abandoned async iterable list source exactly once', async () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    const pendingItem = promiseWithResolvers<IteratorResult<string>>();
    const iterationStarted =
      // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
      promiseWithResolvers<void>();

    let nextCount = 0;
    const asyncIterator = {
      [Symbol.asyncIterator]() {
        return this;
      },
      next(): Promise<IteratorResult<string>> {
        nextCount++;
        if (nextCount === 1) {
          return Promise.resolve({ value: 'a', done: false });
        }
        if (nextCount === 2) {
          iterationStarted.resolve();
          return pendingItem.promise;
        }
        return Promise.resolve({ value: undefined, done: true });
      },
      return(): Promise<IteratorResult<string>> {
        return Promise.resolve({ value: undefined, done: true });
      },
    };
    const returnSpy = spyOnMethod(asyncIterator, 'return');

    const resultPromise = execute({
      schema,
      document: parse('{ scalarList }'),
      rootValue: { scalarList: () => asyncIterator },
      abortSignal: abortController.signal,
    });

    await iterationStarted.promise;
    abortController.abort(reason);

    const error = await expectPromise(resultPromise).toReject();
    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect((error as { cause?: unknown }).cause).to.equal(reason);

    // Releasing the pending iteration lets the abandoned source close.
    pendingItem.resolve({ value: 'b', done: false });
    await drainEventLoop();
    expect(returnSpy.callCount).to.equal(1);
  });
});

describe('subscribe: cancellation', () => {
  it('surfaces the abort reason as a field error when the source stream is still being created', async () => {
    const abortController = new AbortController();
    const reason = new Error('client disconnected');
    const streamSource = promiseWithResolvers<unknown>();

    const resultPromise = subscribe({
      schema,
      document: parse('subscription { counter }'),
      rootValue: { counter: () => streamSource.promise },
      abortSignal: abortController.signal,
    });

    abortController.abort(reason);

    const result = await resultPromise;
    assert(!('next' in result));

    const fieldError = result.errors?.[0];
    expect(fieldError).to.be.instanceOf(GraphQLError);
    expect(fieldError?.originalError).to.equal(reason);
    expectJSON(result).toDeepEqual({
      errors: [
        {
          message: 'client disconnected',
          locations: [{ line: 1, column: 16 }],
          path: ['counter'],
        },
      ],
    });
  });

  it('rejects a pending next() with the abort reason and closes the source stream exactly once', async () => {
    const watcher = watchForUnhandledRejection();
    try {
      const abortController = new AbortController();
      const reason = new Error('client disconnected');
      const pendingEvent = promiseWithResolvers<IteratorResult<unknown>>();

      let nextCount = 0;
      const source = {
        [Symbol.asyncIterator]() {
          return this;
        },
        next(): Promise<IteratorResult<unknown>> {
          nextCount++;
          if (nextCount === 1) {
            return Promise.resolve({ value: { counter: 1 }, done: false });
          }
          return pendingEvent.promise;
        },
        return(): Promise<IteratorResult<unknown>> {
          return Promise.resolve({ value: undefined, done: true });
        },
      };
      const returnSpy = spyOnMethod(source, 'return');

      const stream = await subscribe({
        schema,
        document: parse('subscription { counter }'),
        rootValue: { counter: () => source },
        abortSignal: abortController.signal,
      });
      assert('next' in stream);

      // Events flow normally until cancellation.
      expectJSON(await stream.next()).toDeepEqual({
        value: { data: { counter: 1 } },
        done: false,
      });

      const nextPromise = stream.next();
      abortController.abort(reason);

      const error = await expectPromise(nextPromise).toReject();
      expect(error).to.equal(reason);

      // A late failure of the pending source event stays handled.
      pendingEvent.reject(new Error('late source failure'));
      await drainEventLoop();

      // Closing the response stream closes the source stream exactly once.
      await stream.return();
      await stream.return();
      expect(returnSpy.callCount).to.equal(1);

      // Cancellation is sticky: later next() calls reject instead of hanging.
      const lateNextError = await expectPromise(stream.next()).toReject();
      expect(lateNextError).to.equal(reason);

      await drainEventLoop();
    } finally {
      watcher.stop();
    }
    expect(watcher.reason()).to.equal(null);
  });
});

describe('incremental execution: cancellation', () => {
  it('rejects pending subsequent results with the abort reason after the initial result', async () => {
    const watcher = watchForUnhandledRejection();
    try {
      const abortController = new AbortController();
      const reason = new Error('client disconnected');
      const author = promiseWithResolvers<{ id: string }>();

      const result = await experimentalExecuteIncrementally({
        schema,
        document: parse('query { todo { id ... @defer { author { id } } } }'),
        rootValue: { todo: { id: 'todo-1', author: () => author.promise } },
        abortSignal: abortController.signal,
      });
      assert('initialResult' in result);

      // The initial result is delivered before cancellation.
      expectJSON(result.initialResult).toDeepEqual({
        data: { todo: { id: 'todo-1' } },
        pending: [{ id: '0', path: ['todo'] }],
        hasNext: true,
      });

      const iterator = result.subsequentResults[Symbol.asyncIterator]();
      const nextPromise = iterator.next();
      abortController.abort(reason);

      const error = await expectPromise(nextPromise).toReject();
      expect(error).to.equal(reason);

      // Late resolution of deferred work is ignored and the stream is done.
      author.resolve({ id: 'author-1' });
      await drainEventLoop();
      expect(await iterator.next()).to.deep.equal({
        value: undefined,
        done: true,
      });
    } finally {
      watcher.stop();
    }
    expect(watcher.reason()).to.equal(null);
  });

  it('ignores late rejections of deferred work without unhandled rejections', async () => {
    const watcher = watchForUnhandledRejection();
    try {
      const abortController = new AbortController();
      const reason = new Error('client disconnected');
      const author = promiseWithResolvers<{ id: string }>();

      const result = await experimentalExecuteIncrementally({
        schema,
        document: parse('query { todo { id ... @defer { author { id } } } }'),
        rootValue: { todo: { id: 'todo-1', author: () => author.promise } },
        abortSignal: abortController.signal,
      });
      assert('initialResult' in result);

      const iterator = result.subsequentResults[Symbol.asyncIterator]();
      const nextPromise = iterator.next();
      abortController.abort(reason);

      const error = await expectPromise(nextPromise).toReject();
      expect(error).to.equal(reason);

      author.reject(new Error('late author failure'));
      // The late rejection remains observable on the caller's own promise.
      await expectPromise(author.promise).toRejectWith('late author failure');

      await drainEventLoop();
    } finally {
      watcher.stop();
    }
    expect(watcher.reason()).to.equal(null);
  });

  it('closes a pending @stream source iterator exactly once and ignores late items', async () => {
    const watcher = watchForUnhandledRejection();
    try {
      const abortController = new AbortController();
      const reason = new Error('client disconnected');
      const pendingItem = promiseWithResolvers<IteratorResult<string>>();
      const iterationStarted =
        // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
        promiseWithResolvers<void>();

      let nextCount = 0;
      const asyncIterator = {
        [Symbol.asyncIterator]() {
          return this;
        },
        next(): Promise<IteratorResult<string>> {
          nextCount++;
          if (nextCount === 1) {
            iterationStarted.resolve();
            return pendingItem.promise;
          }
          return Promise.resolve({ value: undefined, done: true });
        },
        return(): Promise<IteratorResult<string>> {
          return Promise.resolve({ value: undefined, done: true });
        },
      };
      const returnSpy = spyOnMethod(asyncIterator, 'return');

      const result = await experimentalExecuteIncrementally({
        schema,
        document: parse('{ scalarList @stream(initialCount: 0) }'),
        rootValue: { scalarList: () => asyncIterator },
        abortSignal: abortController.signal,
      });
      assert('initialResult' in result);

      // The initial result is delivered before cancellation.
      expectJSON(result.initialResult).toDeepEqual({
        data: { scalarList: [] },
        pending: [{ id: '0', path: ['scalarList'] }],
        hasNext: true,
      });

      const iterator = result.subsequentResults[Symbol.asyncIterator]();
      const nextPromise = iterator.next();
      await iterationStarted.promise;
      abortController.abort(reason);

      const error = await expectPromise(nextPromise).toReject();
      expect(error).to.equal(reason);

      // Late items are ignored and the abandoned source is closed once.
      pendingItem.resolve({ value: 'late', done: false });
      await drainEventLoop();
      expect(returnSpy.callCount).to.equal(1);
    } finally {
      watcher.stop();
    }
    expect(watcher.reason()).to.equal(null);
  });
});

const combinedDocument = parse(`mutation {
  first { id }
  second {
    id
    ... @defer {
      author { name }
    }
    items @stream(initialCount: 0)
  }
}`);

const combinedNonNullDocument = parse(`mutation {
  first { id }
  second {
    id
    ... @defer {
      author { name nonNullName }
    }
    items @stream(initialCount: 0)
  }
}`);

interface Controlled<T> {
  promise: Promise<T>;
  resolve: (value: PromiseOrValue<T>) => void;
  reject: (reason?: unknown) => void;
}

interface ControllableAsyncIterator extends AsyncIterable<unknown> {
  next: () => Promise<IteratorResult<unknown>>;
  return: () => Promise<IteratorResult<unknown>>;
}

interface CombinedFixture {
  /** Resolver invocations in the order they started. */
  calls: Array<string>;
  /** Resolver abort-signal cleanup callbacks in firing order. */
  cleanups: Array<string>;
  /** Abort signal observed by each started resolver. */
  signals: {
    first: AbortSignal | undefined;
    second: AbortSignal | undefined;
    author: AbortSignal | undefined;
    items: AbortSignal | undefined;
  };
  first: Controlled<{ id: string }>;
  second: Controlled<undefined>;
  author: Controlled<{ name: string; nonNullName?: unknown }>;
  authorStarted: Promise<void>;
  itemsStarted: Promise<void>;
  /** Source iterator requested by the streamed `items` field. */
  streamSource: ControllableAsyncIterator;
  streamReturnSpy: MethodSpy;
  /** Item pulls issued by the stream, in request order. */
  pulls: Array<Controlled<IteratorResult<unknown>>>;
  /** Waits for the next not-yet-observed item pull from the stream. */
  nextPull: () => Promise<Controlled<IteratorResult<unknown>>>;
  rootValue: {
    first: (
      args: unknown,
      contextValue: unknown,
      info: GraphQLResolveInfo,
    ) => Promise<{ id: string }>;
    second: (
      args: unknown,
      contextValue: unknown,
      info: GraphQLResolveInfo,
    ) => Promise<{
      id: string;
      author: (
        args: unknown,
        contextValue: unknown,
        info: GraphQLResolveInfo,
      ) => Promise<{ name: string; nonNullName?: unknown }>;
      items: (
        args: unknown,
        contextValue: unknown,
        info: GraphQLResolveInfo,
      ) => ControllableAsyncIterator;
    }>;
  };
}

/**
 * Builds a root value driving one combined operation: two serial mutation
 * root fields, the second holding a deferred `author` fragment and a
 * streamed `items` list. Every resolver start, observed abort signal, and
 * source item pull is recorded so tests can assert ordering and cleanup
 * without relying on internal implementation details.
 */
function makeCombinedFixture(): CombinedFixture {
  const calls: Array<string> = [];
  const cleanups: Array<string> = [];
  const signals = {
    first: undefined as AbortSignal | undefined,
    second: undefined as AbortSignal | undefined,
    author: undefined as AbortSignal | undefined,
    items: undefined as AbortSignal | undefined,
  };
  const first = promiseWithResolvers<{ id: string }>();
  const second = promiseWithResolvers<undefined>();
  const author = promiseWithResolvers<{
    name: string;
    nonNullName?: unknown;
  }>();
  const authorStarted =
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
    promiseWithResolvers<void>();
  const itemsStarted =
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type
    promiseWithResolvers<void>();

  const pulls: Array<Controlled<IteratorResult<unknown>>> = [];
  const pullWaiters: Array<
    (pull: Controlled<IteratorResult<unknown>>) => void
  > = [];
  let awaitedPulls = 0;

  const streamSource: ControllableAsyncIterator = {
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
  const streamReturnSpy = spyOnMethod(streamSource, 'return');

  function nextPull(): Promise<Controlled<IteratorResult<unknown>>> {
    const pull = pulls[awaitedPulls++];
    if (pull !== undefined) {
      return Promise.resolve(pull);
    }
    return new Promise((resolve) => {
      pullWaiters.push(resolve);
    });
  }

  function track(
    name: 'first' | 'second' | 'author' | 'items',
    info: GraphQLResolveInfo,
  ): void {
    calls.push(name);
    const signal = info.getAbortSignal();
    signals[name] = signal;
    signal?.addEventListener('abort', () => {
      cleanups.push(name);
    });
  }

  const rootValue = {
    first: (
      _args: unknown,
      _contextValue: unknown,
      info: GraphQLResolveInfo,
    ) => {
      track('first', info);
      return first.promise;
    },
    second: (
      _args: unknown,
      _contextValue: unknown,
      info: GraphQLResolveInfo,
    ) => {
      track('second', info);
      return second.promise.then(() => ({
        id: 'second-id',
        author: (
          _authorArgs: unknown,
          _authorContext: unknown,
          authorInfo: GraphQLResolveInfo,
        ) => {
          track('author', authorInfo);
          authorStarted.resolve();
          return author.promise;
        },
        items: (
          _itemArgs: unknown,
          _itemContext: unknown,
          itemsInfo: GraphQLResolveInfo,
        ) => {
          track('items', itemsInfo);
          itemsStarted.resolve();
          return streamSource;
        },
      }));
    },
  };

  return {
    calls,
    cleanups,
    signals,
    first,
    second,
    author,
    authorStarted: authorStarted.promise,
    itemsStarted: itemsStarted.promise,
    streamSource,
    streamReturnSpy,
    pulls,
    nextPull,
    rootValue,
  };
}

describe('combined mutation operation: cancellation', () => {
  it('stops before the initial payload while a serial root field is pending', async () => {
    const fixture = makeCombinedFixture();
    const abortController = new AbortController();
    const reason = new Error('client disconnected');

    const resultPromise = experimentalExecuteIncrementally({
      schema,
      document: combinedDocument,
      rootValue: fixture.rootValue,
      abortSignal: abortController.signal,
      enableEarlyExecution: true,
    });

    // Serial root fields start one at a time: only the first is running.
    expect(fixture.calls).to.deep.equal(['first']);

    abortController.abort(reason);

    const error = (await expectPromise(
      resultPromise,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;
    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(error.cause).to.equal(reason);

    // Late settlement of the pending root field completes the partial
    // result; the second serial field and all incremental work never start.
    fixture.first.resolve({ id: 'first-id' });
    expectJSON(await error.abortedResult).toDeepEqual({
      data: null,
      errors: [
        {
          message: 'Aborted!',
          locations: [{ line: 2, column: 3 }],
          path: ['first'],
        },
        { message: 'Aborted!' },
      ],
    });

    await drainEventLoop();
    expect(fixture.calls).to.deep.equal(['first']);
    expect(fixture.cleanups).to.deep.equal(['first']);
    expect(fixture.signals.first?.aborted).to.equal(true);
    expect(fixture.streamReturnSpy.callCount).to.equal(0);
  });

  it('aborts after the initial payload while deferred and streamed work is pending', async () => {
    const watcher = watchForUnhandledRejection();
    try {
      const fixture = makeCombinedFixture();
      const abortController = new AbortController();
      const reason = new Error('client disconnected');

      const resultPromise = experimentalExecuteIncrementally({
        schema,
        document: combinedDocument,
        rootValue: fixture.rootValue,
        abortSignal: abortController.signal,
        enableEarlyExecution: true,
      });
      fixture.first.resolve({ id: 'first-id' });
      fixture.second.resolve(undefined);
      const result = await resultPromise;
      assert('initialResult' in result);

      // The client has seen the initial payload with both incremental
      // deliveries still pending.
      expectJSON(result.initialResult).toDeepEqual({
        data: {
          first: { id: 'first-id' },
          second: { id: 'second-id', items: [] },
        },
        pending: [
          { id: '0', path: ['second'] },
          { id: '1', path: ['second', 'items'] },
        ],
        hasNext: true,
      });
      await fixture.authorStarted;
      expect(fixture.calls).to.deep.equal([
        'first',
        'second',
        'items',
        'author',
      ]);

      const iterator = result.subsequentResults[Symbol.asyncIterator]();
      const nextPromise = iterator.next();
      const pull = await fixture.nextPull();
      abortController.abort(reason);

      const error = await expectPromise(nextPromise).toReject();
      expect(error).to.equal(reason);

      // Late settlement of deferred and streamed work is ignored.
      fixture.author.resolve({ name: 'Ada' });
      pull.resolve({ value: 'late', done: false });
      await drainEventLoop();

      expect(await iterator.next()).to.deep.equal({
        value: undefined,
        done: true,
      });
      expect(fixture.streamReturnSpy.callCount).to.equal(1);
      expect(fixture.cleanups).to.deep.equal([
        'first',
        'second',
        'items',
        'author',
      ]);
      expect(fixture.signals.author?.aborted).to.equal(true);
      expect(fixture.signals.items?.aborted).to.equal(true);
    } finally {
      watcher.stop();
    }
    expect(watcher.reason()).to.equal(null);
  });

  it('discards a queued deferred patch when aborted before its delivery', async () => {
    const fixture = makeCombinedFixture();
    const abortController = new AbortController();
    const reason = new Error('client disconnected');

    const resultPromise = experimentalExecuteIncrementally({
      schema,
      document: combinedDocument,
      rootValue: fixture.rootValue,
      abortSignal: abortController.signal,
      enableEarlyExecution: true,
    });
    fixture.first.resolve({ id: 'first-id' });
    fixture.second.resolve(undefined);
    const result = await resultPromise;
    assert('initialResult' in result);

    // The deferred fragment finishes while the consumer is not reading:
    // its patch queues up behind the pending stream.
    await fixture.authorStarted;
    fixture.author.resolve({ name: 'Ada' });
    await drainEventLoop();

    abortController.abort(reason);

    const iterator = result.subsequentResults[Symbol.asyncIterator]();
    const error = await expectPromise(iterator.next()).toReject();
    expect(error).to.equal(reason);

    // Neither the queued patch nor anything else is delivered afterwards.
    expect(await iterator.next()).to.deep.equal({
      value: undefined,
      done: true,
    });
    fixture.pulls[0]?.resolve({ value: 'late', done: false });
    await drainEventLoop();
    expect(fixture.streamReturnSpy.callCount).to.equal(1);
  });

  it('delivers payloads in resolver completion order until aborted mid-stream', async () => {
    const watcher = watchForUnhandledRejection();
    try {
      const fixture = makeCombinedFixture();
      const abortController = new AbortController();
      const reason = new Error('client disconnected');

      const resultPromise = experimentalExecuteIncrementally({
        schema,
        document: combinedDocument,
        rootValue: fixture.rootValue,
        abortSignal: abortController.signal,
        enableEarlyExecution: true,
      });
      fixture.first.resolve({ id: 'first-id' });
      fixture.second.resolve(undefined);
      const result = await resultPromise;
      assert('initialResult' in result);

      const iterator = result.subsequentResults[Symbol.asyncIterator]();
      const delivered: Array<unknown> = [];

      // A stream item completes before the deferred fragment, so the
      // client sees payloads in completion order, not document order.
      const itemPatchPromise = iterator.next();
      await fixture.itemsStarted;
      const firstPull = await fixture.nextPull();
      firstPull.resolve({ value: 'a', done: false });
      delivered.push((await itemPatchPromise).value);

      const deferPatchPromise = iterator.next();
      await fixture.authorStarted;
      fixture.author.resolve({ name: 'Ada' });
      delivered.push((await deferPatchPromise).value);

      // Cancel while the next stream item is still pending.
      const pendingPatchPromise = iterator.next();
      const secondPull = await fixture.nextPull();
      abortController.abort(reason);

      const error = await expectPromise(pendingPatchPromise).toReject();
      expect(error).to.equal(reason);

      // Late items are ignored and the abandoned source closes once.
      secondPull.resolve({ value: 'b', done: false });
      await drainEventLoop();
      expect(await iterator.next()).to.deep.equal({
        value: undefined,
        done: true,
      });
      expect(fixture.streamReturnSpy.callCount).to.equal(1);

      expectJSON(delivered).toDeepEqual([
        { hasNext: true, incremental: [{ id: '1', items: ['a'] }] },
        {
          hasNext: true,
          incremental: [{ id: '0', data: { author: { name: 'Ada' } } }],
          completed: [{ id: '0' }],
        },
      ]);
    } finally {
      watcher.stop();
    }
    expect(watcher.reason()).to.equal(null);
  });

  it('bubbles a non-null failure inside a deferred patch, then aborts the remaining stream', async () => {
    const fixture = makeCombinedFixture();
    const abortController = new AbortController();
    const reason = new Error('client disconnected');

    const resultPromise = experimentalExecuteIncrementally({
      schema,
      document: combinedNonNullDocument,
      rootValue: fixture.rootValue,
      abortSignal: abortController.signal,
      enableEarlyExecution: true,
    });
    fixture.first.resolve({ id: 'first-id' });
    fixture.second.resolve(undefined);
    const result = await resultPromise;
    assert('initialResult' in result);

    const iterator = result.subsequentResults[Symbol.asyncIterator]();
    const patchPromise = iterator.next();
    await fixture.authorStarted;
    fixture.author.resolve({
      name: 'Ada',
      nonNullName: () => Promise.reject(new Error('name failed')),
    });

    // The non-null failure bubbles to the nullable `author` boundary inside
    // the delivered patch; the stream remains pending afterwards.
    const patch = await patchPromise;
    expectJSON(patch.value).toDeepEqual({
      hasNext: true,
      incremental: [
        {
          id: '0',
          data: { author: null },
          errors: [
            {
              message: 'name failed',
              locations: [{ line: 6, column: 21 }],
              path: ['second', 'author', 'nonNullName'],
            },
          ],
        },
      ],
      completed: [{ id: '0' }],
    });

    const pendingPatchPromise = iterator.next();
    abortController.abort(reason);

    const error = await expectPromise(pendingPatchPromise).toReject();
    expect(error).to.equal(reason);

    await drainEventLoop();
    expect(fixture.streamReturnSpy.callCount).to.equal(1);
  });
});
