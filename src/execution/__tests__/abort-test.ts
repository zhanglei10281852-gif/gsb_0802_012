import { describe, it } from 'node:test';

import { assert, expect } from 'chai';

import { expectPromise } from '../../__testUtils__/expectPromise.ts';
import { resolveOnNextTick } from '../../__testUtils__/resolveOnNextTick.ts';

import { isPromise } from '../../jsutils/isPromise.ts';
import { promiseWithResolvers } from '../../jsutils/promiseWithResolvers.ts';

import { parse } from '../../language/parser.ts';

import {
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
} from '../../type/definition.ts';
import { GraphQLString } from '../../type/scalars.ts';
import { GraphQLSchema } from '../../type/schema.ts';

import { buildSchema } from '../../utilities/buildASTSchema.ts';

import { AbortedGraphQLExecutionError } from '../AbortedGraphQLExecutionError.ts';
import {
  execute,
  experimentalExecuteIncrementally,
  subscribe,
} from '../execute.ts';
import type { ExecutionResult } from '../Executor.ts';

const abortReason = new Error('client disconnected');

function createAbortController(): AbortController {
  return new AbortController();
}

interface ControllableAsyncIterator<T> {
  iterator: AsyncGenerator<T, void, void>;
  resolveNext: (value: IteratorResult<T, void>) => void;
  returnSpy: { callCount: number };
}

function createControllableAsyncIterator<T>(): ControllableAsyncIterator<T> {
  let pendingResolve:
    | ((value: IteratorResult<T, void>) => void)
    | undefined;
  let returnCallCount = 0;

  const iterator: AsyncGenerator<T, void, void> = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      const { promise, resolve } =
        promiseWithResolvers<IteratorResult<T, void>>();
      pendingResolve = resolve;
      return promise;
    },
    return() {
      returnCallCount += 1;
      if (pendingResolve) {
        const resolve = pendingResolve;
        pendingResolve = undefined;
        resolve({ value: undefined, done: true });
      }
      return Promise.resolve({ value: undefined, done: true });
    },
    throw(reason?: unknown) {
      if (pendingResolve) {
        const resolve = pendingResolve;
        pendingResolve = undefined;
        resolve({ value: undefined, done: true });
      }
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
      return Promise.reject(reason);
    },
    async [Symbol.asyncDispose]() {
      await this.return();
    },
  };

  return {
    iterator,
    resolveNext: (value) => {
      if (pendingResolve) {
        const resolve = pendingResolve;
        pendingResolve = undefined;
        resolve(value);
      }
    },
    returnSpy: {
      get callCount() {
        return returnCallCount;
      },
    },
  };
}

async function flushMicrotasks(count = 3): Promise<void> {
  for (let i = 0; i < count; i++) {
    // eslint-disable-next-line no-await-in-loop
    await resolveOnNextTick();
  }
}

function withUnhandledRejectionCapture<T>(
  fn: () => Promise<T>,
): Promise<{ result: T | undefined; error: unknown; unhandled: Array<unknown> }> {
  return new Promise((resolve) => {
    const unhandled: Array<unknown> = [];
    const listener = (reason: unknown) => {
      unhandled.push(reason);
    };
    // eslint-disable-next-line no-undef
    process.on('unhandledRejection', listener);
    fn()
      .then((result) => {
        // eslint-disable-next-line no-undef
        process.removeListener('unhandledRejection', listener);
        resolve({ result, error: undefined, unhandled });
      })
      .catch((error: unknown) => {
        // eslint-disable-next-line no-undef
        process.removeListener('unhandledRejection', listener);
        resolve({ result: undefined, error, unhandled });
      });
  });
}

const queryType = new GraphQLObjectType({
  name: 'Query',
  fields: () => ({
    pending: {
      type: GraphQLString,
      resolve: () => new Promise<string>(() => {
        /* never resolves */
      }),
    },
    pendingNonNull: {
      type: new GraphQLNonNull(GraphQLString),
      resolve: () => new Promise<string>(() => {
        /* never resolves */
      }),
    },
    immediate: {
      type: GraphQLString,
      resolve: () => 'value',
    },
    nested: {
      type: nestedType,
      resolve: () => ({
        pending: new Promise<string>(() => {
          /* never resolves */
        }),
      }),
    },
  }),
});

const nestedType = new GraphQLObjectType({
  name: 'Nested',
  fields: {
    pending: {
      type: GraphQLString,
    },
  },
});

const executeSchema = new GraphQLSchema({ query: queryType });

const subscriptionSchema = new GraphQLSchema({
  query: new GraphQLObjectType({
    name: 'Query',
    fields: { dummy: { type: GraphQLString } },
  }),
  subscription: new GraphQLObjectType({
    name: 'Subscription',
    fields: {
      events: { type: GraphQLString },
    },
  }),
});

const incrementalSchema = buildSchema(`
  type Todo {
    id: ID
    title: String
    items: [String]
  }

  type Query {
    todo: Todo
    slowList: [String]
  }
`);

describe('AbortSignal: execute()', () => {
  it('rejects with AbortedGraphQLExecutionError when signal is already aborted before execution starts', async () => {
    const controller = createAbortController();
    controller.abort(abortReason);

    const result = execute({
      schema: executeSchema,
      document: parse('{ immediate }'),
      abortSignal: controller.signal,
    });

    expect(isPromise(result)).to.equal(true);
    const error = (await expectPromise(
      result,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;

    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(error.cause).to.equal(abortReason);
    expect(error.message).to.equal(abortReason.message);

    const abortedResult = await error.abortedResult;
    expect(abortedResult).to.deep.equal({ data: null });
  });

  it('rejects with AbortedGraphQLExecutionError when aborted while a root resolver is pending', async () => {
    const controller = createAbortController();
    const { promise: resolverPromise, resolve: resolveResolver } =
      promiseWithResolvers<string>();

    const schema = new GraphQLSchema({
      query: new GraphQLObjectType({
        name: 'Query',
        fields: {
          slow: {
            type: GraphQLString,
            resolve: () => resolverPromise,
          },
        },
      }),
    });

    const resultPromise = execute({
      schema,
      document: parse('{ slow }'),
      abortSignal: controller.signal,
    });

    controller.abort(abortReason);

    const error = (await expectPromise(
      resultPromise,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;

    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(error.cause).to.equal(abortReason);

    resolveResolver('late value');
    await flushMicrotasks();

    const abortedResult = await error.abortedResult;
    expect(abortedResult).to.have.property('data');
  });

  it('rejects with AbortedGraphQLExecutionError when aborted while a nested field resolver is pending', async () => {
    const controller = createAbortController();

    const resultPromise = execute({
      schema: executeSchema,
      document: parse('{ nested { pending } }'),
      abortSignal: controller.signal,
    });

    await flushMicrotasks();
    controller.abort(abortReason);

    const error = (await expectPromise(
      resultPromise,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;

    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(error.cause).to.equal(abortReason);
  });

  it('bubbles abort to a rejection when the aborted field is non-null', async () => {
    const controller = createAbortController();

    const resultPromise = execute({
      schema: executeSchema,
      document: parse('{ pendingNonNull }'),
      abortSignal: controller.signal,
    });

    await flushMicrotasks();
    controller.abort(abortReason);

    const error = (await expectPromise(
      resultPromise,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;

    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(error.cause).to.equal(abortReason);
  });

  it('calls return() on an async iterable list exactly once when aborted while waiting for items', async () => {
    const controller = createAbortController();
    const controlled = createControllableAsyncIterator<string>();

    const source = {
      [Symbol.asyncIterator]() {
        return controlled.iterator;
      },
    };

    const schema = new GraphQLSchema({
      query: new GraphQLObjectType({
        name: 'Query',
        fields: {
          items: {
            type: new GraphQLList(GraphQLString),
            resolve: () => source,
          },
        },
      }),
    });

    const resultPromise = execute({
      schema,
      document: parse('{ items }'),
      abortSignal: controller.signal,
    });

    controlled.resolveNext({ value: 'first', done: false });
    await flushMicrotasks();

    controller.abort(abortReason);

    const error = (await expectPromise(
      resultPromise,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;

    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(controlled.returnSpy.callCount).to.equal(1);

    controlled.resolveNext({ value: 'late', done: false });
    await flushMicrotasks();
    expect(controlled.returnSpy.callCount).to.equal(1);
  });

  it('does not produce an unhandled rejection when a pending resolver rejects after abort', async () => {
    const controller = createAbortController();
    const { promise: resolverPromise, reject: rejectResolver } =
      promiseWithResolvers<string>();

    const schema = new GraphQLSchema({
      query: new GraphQLObjectType({
        name: 'Query',
        fields: {
          slow: {
            type: GraphQLString,
            resolve: () => resolverPromise,
          },
        },
      }),
    });

    const captured = await withUnhandledRejectionCapture(async () => {
      const resultPromise = execute({
        schema,
        document: parse('{ slow }'),
        abortSignal: controller.signal,
      });

      controller.abort(abortReason);
      await expectPromise(resultPromise).toReject();

      rejectResolver(new Error('late resolver failure'));
      await flushMicrotasks(5);
    });

    expect(captured.error).to.equal(undefined);
    expect(captured.unhandled).to.deep.equal([]);
  });

  it('does not produce an unhandled rejection when a list iterator rejects after abort', async () => {
    const controller = createAbortController();
    const { promise: nextPromise, reject: rejectNext } =
      promiseWithResolvers<IteratorResult<string, void>>();
    let returnCallCount = 0;

    const iterator: AsyncIterator<string> = {
      next: () => nextPromise,
      return: () => {
        returnCallCount += 1;
        return Promise.resolve({ value: undefined, done: true });
      },
    };

    const source = {
      [Symbol.asyncIterator]: () => iterator,
    };

    const schema = new GraphQLSchema({
      query: new GraphQLObjectType({
        name: 'Query',
        fields: {
          items: {
            type: new GraphQLList(GraphQLString),
            resolve: () => source,
          },
        },
      }),
    });

    const captured = await withUnhandledRejectionCapture(async () => {
      const resultPromise = execute({
        schema,
        document: parse('{ items }'),
        abortSignal: controller.signal,
      });

      await flushMicrotasks();
      controller.abort(abortReason);
      await expectPromise(resultPromise).toReject();

      rejectNext(new Error('late iterator failure'));
      await flushMicrotasks(5);
    });

    expect(captured.error).to.equal(undefined);
    expect(captured.unhandled).to.deep.equal([]);
    expect(returnCallCount).to.equal(1);
  });
});

describe('AbortSignal: subscribe()', () => {
  it('produces an error result when aborted while the subscribe resolver is pending', async () => {
    const controller = createAbortController();
    const { promise: subscribePromise } = promiseWithResolvers<
      AsyncIterable<string>
    >();

    const schema = new GraphQLSchema({
      query: new GraphQLObjectType({
        name: 'Query',
        fields: { dummy: { type: GraphQLString } },
      }),
      subscription: new GraphQLObjectType({
        name: 'Subscription',
        fields: {
          events: {
            type: GraphQLString,
            subscribe: () => subscribePromise,
          },
        },
      }),
    });

    const resultPromise = subscribe({
      schema,
      document: parse('subscription { events }'),
      abortSignal: controller.signal,
    });

    controller.abort(abortReason);

    const result = (await resultPromise) as ExecutionResult;
    assert(result.errors !== undefined);
    expect(result.errors).to.have.lengthOf(1);
    expect(result.errors?.[0]?.message).to.equal(abortReason.message);
  });

  it('rejects a pending next() when aborted while consuming events', async () => {
    const controller = createAbortController();
    const controlled = createControllableAsyncIterator<{ events: string }>();

    const result = subscribe({
      schema: subscriptionSchema,
      document: parse('subscription { events }'),
      rootValue: {
        events: () => controlled.iterator,
      },
      abortSignal: controller.signal,
    });

    expect(isPromise(result)).to.equal(false);
    const generator = result as AsyncGenerator<ExecutionResult, void, void>;

    const nextPromise = generator.next();
    await flushMicrotasks();

    controller.abort(abortReason);

    const error = await expectPromise(nextPromise).toReject();
    expect(error).to.equal(abortReason);

    controlled.resolveNext({ value: { events: 'late' }, done: false });
    await flushMicrotasks();

    expect(controlled.returnSpy.callCount).to.equal(1);
  });

  it('calls source return() exactly once when aborted and then the consumer returns', async () => {
    const controller = createAbortController();
    const controlled = createControllableAsyncIterator<{ events: string }>();

    const result = subscribe({
      schema: subscriptionSchema,
      document: parse('subscription { events }'),
      rootValue: {
        events: () => controlled.iterator,
      },
      abortSignal: controller.signal,
    });

    const generator = result as AsyncGenerator<ExecutionResult, void, void>;

    const nextPromise = generator.next();
    await flushMicrotasks();

    controller.abort(abortReason);
    await expectPromise(nextPromise).toReject();

    await generator.return();

    expect(controlled.returnSpy.callCount).to.equal(1);
  });

  it('cleans up the source when signal is already aborted', async () => {
    const controller = createAbortController();
    const controlled = createControllableAsyncIterator<{ events: string }>();

    controller.abort(abortReason);

    const result = subscribe({
      schema: subscriptionSchema,
      document: parse('subscription { events }'),
      rootValue: {
        events: () => controlled.iterator,
      },
      abortSignal: controller.signal,
    });

    const generator = result as AsyncGenerator<ExecutionResult, void, void>;

    await expectPromise(generator.next()).toReject();
    expect(controlled.returnSpy.callCount).to.equal(1);
  });

  it('does not produce an unhandled rejection when a late source event arrives after abort', async () => {
    const controller = createAbortController();
    const { promise: nextPromise, resolve: resolveNext } =
      promiseWithResolvers<IteratorResult<{ events: string }, void>>();
    let returnCallCount = 0;

    const iterator: AsyncIterator<{ events: string }> = {
      next: () => nextPromise,
      return: () => {
        returnCallCount += 1;
        return Promise.resolve({ value: undefined, done: true });
      },
    };

    const source = {
      [Symbol.asyncIterator]: () => iterator,
    };

    const captured = await withUnhandledRejectionCapture(async () => {
      const result = subscribe({
        schema: subscriptionSchema,
        document: parse('subscription { events }'),
        rootValue: { events: () => source },
        abortSignal: controller.signal,
      });

      const generator = result as AsyncGenerator<ExecutionResult, void, void>;
      const pending = generator.next();
      await flushMicrotasks();

      controller.abort(abortReason);
      await expectPromise(pending).toReject();

      resolveNext({ value: { events: 'late' }, done: false });
      await flushMicrotasks(5);
    });

    expect(captured.error).to.equal(undefined);
    expect(captured.unhandled).to.deep.equal([]);
    expect(returnCallCount).to.equal(1);
  });
});

describe('AbortSignal: experimentalExecuteIncrementally()', () => {
  it('rejects the initial result promise with AbortedGraphQLExecutionError when aborted before initial result completes', async () => {
    const controller = createAbortController();
    const { promise: slowPromise } = promiseWithResolvers<string>();

    const schema = new GraphQLSchema({
      query: new GraphQLObjectType({
        name: 'Query',
        fields: {
          slow: {
            type: GraphQLString,
            resolve: () => slowPromise,
          },
        },
      }),
    });

    const resultPromise = experimentalExecuteIncrementally({
      schema,
      document: parse('{ slow }'),
      abortSignal: controller.signal,
    });

    controller.abort(abortReason);

    const error = (await expectPromise(
      resultPromise,
    ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;

    expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
    expect(error.cause).to.equal(abortReason);
  });

  it('delivers an initial result then rejects subsequent next() when aborted during deferred field execution', async () => {
    const controller = createAbortController();
    const { promise: deferredPromise, resolve: resolveDeferred } =
      promiseWithResolvers<string>();

    const schema = buildSchema(`
      type Query {
        immediate: String
        slow: String
      }
    `);

    const result = await experimentalExecuteIncrementally({
      schema,
      document: parse(`
        query {
          immediate
          ... @defer {
            slow
          }
        }
      `),
      rootValue: {
        immediate: 'now',
        slow: () => deferredPromise,
      },
      abortSignal: controller.signal,
    });

    expect('initialResult' in result).to.equal(true);
    if (!('initialResult' in result)) {
      return;
    }

    expect(result.initialResult.data).to.deep.equal({ immediate: 'now' });
    expect(result.initialResult.hasNext).to.equal(true);

    const subsequentPromise = result.subsequentResults.next();
    await flushMicrotasks();

    controller.abort(abortReason);

    await expectPromise(subsequentPromise).toRejectWith(abortReason.message);

    resolveDeferred('late');
    await flushMicrotasks();
  });

  it('rejects subsequent next() and cleans up the stream iterator exactly once when aborted during @stream', async () => {
    const controller = createAbortController();
    const controlled = createControllableAsyncIterator<string>();

    const result = await experimentalExecuteIncrementally({
      schema: incrementalSchema,
      document: parse('{ slowList @stream(initialCount: 0) }'),
      rootValue: {
        slowList: () => controlled.iterator,
      },
      abortSignal: controller.signal,
    });

    expect('initialResult' in result).to.equal(true);
    if (!('initialResult' in result)) {
      return;
    }

    const subsequentPromise = result.subsequentResults.next();
    await flushMicrotasks();

    controller.abort(abortReason);

    await expectPromise(subsequentPromise).toRejectWith(abortReason.message);
    expect(controlled.returnSpy.callCount).to.equal(1);

    controlled.resolveNext({ value: 'late', done: false });
    await flushMicrotasks();
    expect(controlled.returnSpy.callCount).to.equal(1);
  });

  it('does not produce an unhandled rejection when a deferred resolver rejects after abort', async () => {
    const controller = createAbortController();
    const { promise: deferredPromise, reject: rejectDeferred } =
      promiseWithResolvers<string>();

    const schema = buildSchema(`
      type Query {
        immediate: String
        slow: String
      }
    `);

    const captured = await withUnhandledRejectionCapture(async () => {
      const result = await experimentalExecuteIncrementally({
        schema,
        document: parse(`
          query {
            immediate
            ... @defer {
              slow
            }
          }
        `),
        rootValue: {
          immediate: 'now',
          slow: () => deferredPromise,
        },
        abortSignal: controller.signal,
      });

      expect('initialResult' in result).to.equal(true);
      if (!('initialResult' in result)) {
        return;
      }

      const subsequentPromise = result.subsequentResults.next();
      await flushMicrotasks();

      controller.abort(abortReason);
      await expectPromise(subsequentPromise).toReject();

      rejectDeferred(new Error('late deferred failure'));
      await flushMicrotasks(5);
    });

    expect(captured.error).to.equal(undefined);
    expect(captured.unhandled).to.deep.equal([]);
  });

  it('does not produce an unhandled rejection when a stream iterator rejects after abort', async () => {
    const controller = createAbortController();
    const { promise: nextPromise, reject: rejectNext } =
      promiseWithResolvers<IteratorResult<string, void>>();
    let returnCallCount = 0;

    const iterator: AsyncIterator<string> = {
      next: () => nextPromise,
      return: () => {
        returnCallCount += 1;
        return Promise.resolve({ value: undefined, done: true });
      },
    };

    const source = {
      [Symbol.asyncIterator]: () => iterator,
    };

    const captured = await withUnhandledRejectionCapture(async () => {
      const result = await experimentalExecuteIncrementally({
        schema: incrementalSchema,
        document: parse('{ slowList @stream(initialCount: 0) }'),
        rootValue: {
          slowList: () => source,
        },
        abortSignal: controller.signal,
      });

      expect('initialResult' in result).to.equal(true);
      if (!('initialResult' in result)) {
        return;
      }

      const subsequentPromise = result.subsequentResults.next();
      await flushMicrotasks();

      controller.abort(abortReason);
      await expectPromise(subsequentPromise).toReject();

      rejectNext(new Error('late stream failure'));
      await flushMicrotasks(5);
    });

    expect(captured.error).to.equal(undefined);
    expect(captured.unhandled).to.deep.equal([]);
    expect(returnCallCount).to.equal(1);
  });
});

describe('AbortSignal: combined mutation with parallel fields, @defer, and @stream', () => {
  const combinedSchema = buildSchema(`
    type ActionResult {
      sync: String
      slow: String
      nonNullSlow: String!
      items: [String]
    }

    type Query {
      noop: String
    }

    type Mutation {
      first: ActionResult
      second: ActionResult
    }
  `);

  const combinedDocument = parse(`
    mutation Combined {
      first {
        sync
        slow
        ... @defer {
          nonNullSlow
        }
        items @stream(initialCount: 0)
      }
      second {
        sync
        slow
      }
    }
  `);

  interface CombinedFixture {
    rootValue: { [key: string]: unknown };
    stream: ControllableAsyncIterator<string>;
    firstSlow: {
      promise: Promise<string>;
      resolve: (value: string) => void;
      reject: (reason?: unknown) => void;
    };
    firstDeferred: {
      promise: Promise<string | null>;
      resolve: (value: string | null) => void;
      reject: (reason?: unknown) => void;
    };
    secondSlow: {
      promise: Promise<string>;
      resolve: (value: string) => void;
      reject: (reason?: unknown) => void;
    };
    resolverSignals: Array<AbortSignal>;
  }

  function createFixture(): CombinedFixture {
    const stream = createControllableAsyncIterator<string>();
    const resolverSignals: Array<AbortSignal> = [];

    const firstSlow = promiseWithResolvers<string>();
    const firstDeferred = promiseWithResolvers<string | null>();
    const secondSlow = promiseWithResolvers<string>();

    const trackSignal = (info: {
      getAbortSignal: () => AbortSignal | undefined;
    }) => {
      const signal = info.getAbortSignal();
      if (signal) {
        resolverSignals.push(signal);
      }
    };

    const rootValue = {
      first: {
        sync: 'first-sync',
        slow: (
          _args: unknown,
          _ctx: unknown,
          info: { getAbortSignal: () => AbortSignal | undefined },
        ) => {
          trackSignal(info);
          return firstSlow.promise;
        },
        nonNullSlow: (
          _args: unknown,
          _ctx: unknown,
          info: { getAbortSignal: () => AbortSignal | undefined },
        ) => {
          trackSignal(info);
          return firstDeferred.promise;
        },
        items: () => stream.iterator,
      },
      second: {
        sync: 'second-sync',
        slow: (
          _args: unknown,
          _ctx: unknown,
          info: { getAbortSignal: () => AbortSignal | undefined },
        ) => {
          trackSignal(info);
          return secondSlow.promise;
        },
      },
    };

    return {
      rootValue,
      stream,
      firstSlow,
      firstDeferred,
      secondSlow,
      resolverSignals,
    };
  }

  function executeCombined(
    fixture: CombinedFixture,
    controller: AbortController,
  ) {
    return experimentalExecuteIncrementally({
      schema: combinedSchema,
      document: combinedDocument,
      rootValue: fixture.rootValue,
      abortSignal: controller.signal,
      enableEarlyExecution: true,
    });
  }

  async function resolveInitialPayload(fixture: CombinedFixture): Promise<void> {
    fixture.firstSlow.resolve('first-slow');
    await flushMicrotasks();
    fixture.secondSlow.resolve('second-slow');
    await flushMicrotasks();
  }

  it('aborts before the initial payload is delivered', async () => {
    const controller = createAbortController();
    const fixture = createFixture();

    const captured = await withUnhandledRejectionCapture(async () => {
      const resultPromise = executeCombined(fixture, controller);

      await flushMicrotasks();
      controller.abort(abortReason);

      const error = (await expectPromise(
        resultPromise,
      ).toReject()) as AbortedGraphQLExecutionError<ExecutionResult>;

      expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);
      expect(error.cause).to.equal(abortReason);

      fixture.firstSlow.resolve('late-first-slow');
      fixture.firstDeferred.resolve('late-deferred');
      fixture.secondSlow.resolve('late-second-slow');
      fixture.stream.resolveNext({ value: 'late-item', done: false });
      await flushMicrotasks(5);

      return error;
    });

    expect(captured.error).to.equal(undefined);
    expect(captured.unhandled).to.deep.equal([]);
    expect(fixture.stream.returnSpy.callCount).to.equal(1);
    for (const signal of fixture.resolverSignals) {
      expect(signal.aborted).to.equal(true);
    }
  });

  it('aborts after the initial payload but before consuming subsequent results', async () => {
    const controller = createAbortController();
    const fixture = createFixture();

    const resultPromise = executeCombined(fixture, controller);
    await flushMicrotasks();
    await resolveInitialPayload(fixture);
    const result = await resultPromise;

    assert('initialResult' in result);
    if (!('initialResult' in result)) {
      return;
    }

    expect(result.initialResult.data).to.deep.equal({
      first: { sync: 'first-sync', slow: 'first-slow', items: [] },
      second: { sync: 'second-sync', slow: 'second-slow' },
    });
    expect(result.initialResult.hasNext).to.equal(true);
    expect(result.initialResult.pending).to.have.lengthOf(2);

    controller.abort(abortReason);

    await expectPromise(result.subsequentResults.next()).toRejectWith(
      abortReason.message,
    );

    const captured = await withUnhandledRejectionCapture(async () => {
      fixture.firstDeferred.resolve('late-deferred');
      fixture.stream.resolveNext({ value: 'late-item', done: false });
      await flushMicrotasks(5);
    });

    expect(captured.unhandled).to.deep.equal([]);
    expect(fixture.stream.returnSpy.callCount).to.equal(1);
  });

  it('discards a queued deferred patch when abort arrives before consumption', async () => {
    const controller = createAbortController();
    const fixture = createFixture();

    const resultPromise = executeCombined(fixture, controller);
    await flushMicrotasks();
    await resolveInitialPayload(fixture);
    const result = await resultPromise;

    assert('initialResult' in result);
    if (!('initialResult' in result)) {
      return;
    }

    fixture.firstDeferred.resolve('deferred-value');
    await flushMicrotasks();

    controller.abort(abortReason);

    const nextResult = result.subsequentResults.next();
    await expectPromise(nextResult).toRejectWith(abortReason.message);

    expect(fixture.stream.returnSpy.callCount).to.equal(1);

    fixture.stream.resolveNext({ value: 'late-item', done: false });
    await flushMicrotasks(3);
    expect(fixture.stream.returnSpy.callCount).to.equal(1);
  });

  it('cancels after the stream has delivered partial items', async () => {
    const controller = createAbortController();
    const fixture = createFixture();

    const resultPromise = executeCombined(fixture, controller);
    await flushMicrotasks();
    await resolveInitialPayload(fixture);
    const result = await resultPromise;

    assert('initialResult' in result);
    if (!('initialResult' in result)) {
      return;
    }

    expect(result.initialResult.data).to.deep.equal({
      first: { sync: 'first-sync', slow: 'first-slow', items: [] },
      second: { sync: 'second-sync', slow: 'second-slow' },
    });

    const iterator = result.subsequentResults[Symbol.asyncIterator]();

    const firstPatchPromise = iterator.next();
    await flushMicrotasks();
    fixture.stream.resolveNext({ value: 'item-1', done: false });
    const firstPatch = await firstPatchPromise;
    expect(firstPatch.done).to.equal(false);
    const firstIncremental = firstPatch.value?.incremental?.[0];
    assert(firstIncremental !== undefined && 'items' in firstIncremental);
    expect(firstIncremental.items).to.deep.equal(['item-1']);

    const secondPatchPromise = iterator.next();
    await flushMicrotasks();
    fixture.stream.resolveNext({ value: 'item-2', done: false });
    const secondPatch = await secondPatchPromise;
    expect(secondPatch.done).to.equal(false);
    const secondIncremental = secondPatch.value?.incremental?.[0];
    assert(secondIncremental !== undefined && 'items' in secondIncremental);
    expect(secondIncremental.items).to.deep.equal(['item-2']);

    const thirdPatchPromise = iterator.next();
    await flushMicrotasks();

    controller.abort(abortReason);

    await expectPromise(thirdPatchPromise).toRejectWith(abortReason.message);

    expect(fixture.stream.returnSpy.callCount).to.equal(1);

    fixture.stream.resolveNext({ value: 'item-3', done: false });
    fixture.firstDeferred.resolve('late-deferred');
    await flushMicrotasks(5);
    expect(fixture.stream.returnSpy.callCount).to.equal(1);
  });

  it('bubbles non-null errors from deferred fields and cleans up on external abort', async () => {
    const controller = createAbortController();
    const fixture = createFixture();

    const resultPromise = executeCombined(fixture, controller);
    await flushMicrotasks();
    await resolveInitialPayload(fixture);
    const result = await resultPromise;

    assert('initialResult' in result);
    if (!('initialResult' in result)) {
      return;
    }

    expect(result.initialResult.errors).to.equal(undefined);

    const iterator = result.subsequentResults[Symbol.asyncIterator]();

    const patchPromise = iterator.next();
    await flushMicrotasks();

    fixture.firstDeferred.resolve(null);
    await flushMicrotasks(3);

    const patch = await patchPromise;
    expect(patch.done).to.equal(false);

    const completed = patch.value?.completed;
    assert(completed !== undefined && completed.length > 0);
    assert(completed[0]?.errors !== undefined);
    expect(completed[0]?.errors?.[0]?.message).to.include(
      'non-nullable field',
    );

    expect(fixture.stream.returnSpy.callCount).to.equal(0);

    const nextAfterPatch = iterator.next();
    await flushMicrotasks();

    controller.abort(abortReason);

    await expectPromise(nextAfterPatch).toRejectWith(abortReason.message);
    expect(fixture.stream.returnSpy.callCount).to.equal(1);

    fixture.stream.resolveNext({ value: 'late', done: false });
    await flushMicrotasks(3);
    expect(fixture.stream.returnSpy.callCount).to.equal(1);
  });

  it('does not leak unhandled rejections when resolvers reject in different orders after abort', async () => {
    const controller = createAbortController();
    const fixture = createFixture();

    const captured = await withUnhandledRejectionCapture(async () => {
      const resultPromise = executeCombined(fixture, controller);
      await flushMicrotasks();
      await resolveInitialPayload(fixture);
      const result = await resultPromise;
      assert('initialResult' in result);
      if (!('initialResult' in result)) {
        return;
      }

      const nextPromise = result.subsequentResults.next();
      await flushMicrotasks();

      controller.abort(abortReason);
      await expectPromise(nextPromise).toReject();

      fixture.firstDeferred.reject(new Error('deferred rejected late'));
      fixture.stream.resolveNext({ value: 'late', done: false });
      await flushMicrotasks(5);
    });

    expect(captured.error).to.equal(undefined);
    expect(captured.unhandled).to.deep.equal([]);
    expect(fixture.stream.returnSpy.callCount).to.equal(1);
  });
});
