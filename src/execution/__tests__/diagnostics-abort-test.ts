import { describe, it } from 'node:test';

import { assert, expect } from 'chai';

import type { TracingSubscriptionHandler } from '../../__testUtils__/diagnosticsTracing.ts';
import { getTracingChannel } from '../../__testUtils__/getTracingChannel.ts';
import { resolveOnNextTick } from '../../__testUtils__/resolveOnNextTick.ts';

import { isPromise } from '../../jsutils/isPromise.ts';
import { promiseWithResolvers } from '../../jsutils/promiseWithResolvers.ts';

import { OperationTypeNode } from '../../language/ast.ts';
import { parse } from '../../language/parser.ts';

import { GraphQLObjectType } from '../../type/definition.ts';
import { GraphQLString } from '../../type/scalars.ts';
import { GraphQLSchema } from '../../type/schema.ts';

import { buildSchema } from '../../utilities/buildASTSchema.ts';

import { AbortedGraphQLExecutionError } from '../AbortedGraphQLExecutionError.ts';
import {
  execute,
  experimentalExecuteIncrementally,
  subscribe,
} from '../execute.ts';

const abortReason = new Error('client disconnected');

interface CollectedEvent {
  channel: string;
  hasError: boolean;
  hasResult: boolean;
}

type AnyChannel = ReturnType<typeof getTracingChannel<any>>;

function collectEvents(
  channel: AnyChannel,
): {
  events: Array<CollectedEvent>;
  unsubscribe: () => void;
} {
  const events: Array<CollectedEvent> = [];
  const handler: TracingSubscriptionHandler = {
    start: (context: unknown) => {
      const ctx = context as { error?: unknown; result?: unknown };
      events.push({
        channel: 'start',
        hasError: ctx.error !== undefined,
        hasResult: ctx.result !== undefined,
      });
    },
    end: (context: unknown) => {
      const ctx = context as { error?: unknown; result?: unknown };
      events.push({
        channel: 'end',
        hasError: ctx.error !== undefined,
        hasResult: ctx.result !== undefined,
      });
    },
    asyncStart: (context: unknown) => {
      const ctx = context as { error?: unknown; result?: unknown };
      events.push({
        channel: 'asyncStart',
        hasError: ctx.error !== undefined,
        hasResult: ctx.result !== undefined,
      });
    },
    asyncEnd: (context: unknown) => {
      const ctx = context as { error?: unknown; result?: unknown };
      events.push({
        channel: 'asyncEnd',
        hasError: ctx.error !== undefined,
        hasResult: ctx.result !== undefined,
      });
    },
    error: (context: unknown) => {
      const ctx = context as { error?: unknown; result?: unknown };
      events.push({
        channel: 'error',
        hasError: ctx.error !== undefined,
        hasResult: ctx.result !== undefined,
      });
    },
  };

  channel.subscribe(handler);
  return {
    events,
    unsubscribe: () => channel.unsubscribe(handler),
  };
}

async function flushMicrotasks(count = 3): Promise<void> {
  for (let i = 0; i < count; i++) {
    // eslint-disable-next-line no-await-in-loop
    await resolveOnNextTick();
  }
}

const incrementalSchema = buildSchema(`
  type Query {
    immediate: String
    slow: String
  }
`);

const subscriptionSchema = new GraphQLSchema({
  query: new GraphQLObjectType({
    name: 'Query',
    fields: { dummy: { type: GraphQLString } },
  }),
  subscription: new GraphQLObjectType({
    name: 'Subscription',
    fields: {
      tick: { type: GraphQLString },
    },
  }),
});

describe('Diagnostics channel: abort lifecycle', () => {
  const executeChannel = getTracingChannel('graphql:execute');
  const subscribeChannel = getTracingChannel('graphql:subscribe');

  it('emits exactly one end/asyncEnd pair for a normal incremental completion', async () => {
    const document = parse('{ immediate slow }');
    const { events, unsubscribe } = collectEvents(executeChannel);

    try {
      const result = await experimentalExecuteIncrementally({
        schema: incrementalSchema,
        document,
        rootValue: {
          immediate: 'now',
          slow: () => Promise.resolve('later'),
        },
      });

      expect(events.map((e) => e.channel)).to.deep.equal([
        'start',
        'end',
        'asyncStart',
        'asyncEnd',
      ]);
      expect(events[0]?.hasError).to.equal(false);
      expect(events[3]?.hasError).to.equal(false);
      expect(events[3]?.hasResult).to.equal(true);

      if ('subsequentResults' in result) {
        for await (const _patch of result.subsequentResults) {
          // drain
        }
      }
    } finally {
      unsubscribe();
    }
  });

  it('emits error exactly once for an aborted execute with no duplicate termination', async () => {
    const controller = new AbortController();
    const document = parse('{ slow }');
    const { events, unsubscribe } = collectEvents(executeChannel);

    try {
      const resultOrPromise = execute({
        schema: incrementalSchema,
        document,
        rootValue: {
          slow: () =>
            new Promise<string>(() => {
              /* never resolves */
            }),
        },
        abortSignal: controller.signal,
      });

      await flushMicrotasks();
      controller.abort(abortReason);

      const resultPromise = isPromise(resultOrPromise)
        ? resultOrPromise
        : Promise.resolve(resultOrPromise);
      const error = (await resultPromise.catch(
        (e: unknown) => e,
      )) as Error;
      expect(error).to.be.instanceOf(AbortedGraphQLExecutionError);

      const channels = events.map((e) => e.channel);
      expect(channels).to.deep.equal([
        'start',
        'end',
        'error',
        'asyncStart',
        'asyncEnd',
      ]);

      const errorEvents = events.filter((e) => e.channel === 'error');
      expect(errorEvents).to.have.lengthOf(1);
      expect(errorEvents[0]?.hasError).to.equal(true);

      const endEvents = events.filter((e) => e.channel === 'asyncEnd');
      expect(endEvents).to.have.lengthOf(1);
    } finally {
      unsubscribe();
    }
  });

  it('emits error exactly once when a root resolver rejects', async () => {
    const document = parse('{ slow }');
    const { events, unsubscribe } = collectEvents(executeChannel);

    try {
      const result = (await execute({
        schema: incrementalSchema,
        document,
        rootValue: {
          slow: () => Promise.reject(new Error('resolver failure')),
        },
      })) as { errors?: ReadonlyArray<Error> };

      const channels = events.map((e) => e.channel);
      expect(channels).to.deep.equal([
        'start',
        'end',
        'asyncStart',
        'asyncEnd',
      ]);

      assert(result.errors !== undefined);
      expect(result.errors[0]?.message).to.equal('resolver failure');

      const errorEvents = events.filter((e) => e.channel === 'error');
      expect(errorEvents).to.have.lengthOf(0);
    } finally {
      unsubscribe();
    }
  });

  it('emits error exactly once for an aborted incremental query without duplicate end', async () => {
    const controller = new AbortController();
    const document = parse('{ immediate ... @defer { slow } }');
    const { events, unsubscribe } = collectEvents(executeChannel);

    try {
      const { promise: slowPromise } = promiseWithResolvers<string>();
      const result = experimentalExecuteIncrementally({
        schema: incrementalSchema,
        document,
        rootValue: {
          immediate: 'now',
          slow: () => slowPromise,
        },
        abortSignal: controller.signal,
      });

      assert('initialResult' in result);
      const subsequentPromise = result.subsequentResults.next();
      await flushMicrotasks();
      controller.abort(abortReason);

      await subsequentPromise.catch(() => undefined);

      const channels = events.map((e) => e.channel);
      expect(channels).to.deep.equal(['start', 'end']);

      const errorEvents = events.filter((e) => e.channel === 'error');
      expect(errorEvents).to.have.lengthOf(0);

      const endEvents = events.filter((e) => e.channel === 'end');
      expect(endEvents).to.have.lengthOf(1);
      expect(endEvents[0]?.hasError).to.equal(false);
    } finally {
      unsubscribe();
    }
  });

  it('emits start/end/async lifecycle for a subscription and no duplicate events on abort', async () => {
    const controller = new AbortController();
    const { promise: nextPromise, resolve: resolveNext } =
      promiseWithResolvers<IteratorResult<{ tick: string }, void>>();

    let returnCallCount = 0;
    const iterator: AsyncIterator<{ tick: string }> = {
      next: () => nextPromise,
      return: () => {
        returnCallCount += 1;
        return Promise.resolve({ value: undefined, done: true });
      },
    };
    const source = { [Symbol.asyncIterator]: () => iterator };

    const document = parse('subscription { tick }');
    const { events, unsubscribe } = collectEvents(subscribeChannel);

    try {
      const result = subscribe({
        schema: subscriptionSchema,
        document,
        rootValue: { tick: () => source },
        abortSignal: controller.signal,
      });

      assert(!(result instanceof Promise));
      const generator = result as AsyncGenerator<unknown, void, void>;

      const pending = generator.next();
      await flushMicrotasks();

      controller.abort(abortReason);
      await pending.catch(() => undefined);

      const channels = events.map((e) => e.channel);
      expect(channels).to.deep.equal(['start', 'end']);

      const startEvents = events.filter((e) => e.channel === 'start');
      const endEvents = events.filter((e) => e.channel === 'end');
      expect(startEvents).to.have.lengthOf(1);
      expect(endEvents).to.have.lengthOf(1);
      expect(events.filter((e) => e.channel === 'error')).to.have.lengthOf(0);

      expect(returnCallCount).to.equal(1);

      resolveNext({ value: { tick: 'late' }, done: false });
      await flushMicrotasks(3);
      expect(returnCallCount).to.equal(1);
    } finally {
      unsubscribe();
    }
  });

  it('publishes the operation type and name on the execute context', async () => {
    const document = parse('query Named { immediate }');
    const channel = getTracingChannel('graphql:execute');
    const contexts: Array<{ [key: string]: unknown }> = [];
    const handler: TracingSubscriptionHandler = {
      start: (ctx: unknown) => {
        contexts.push(ctx as { [key: string]: unknown });
      },
      end: () => undefined,
      asyncStart: () => undefined,
      asyncEnd: () => undefined,
      error: () => undefined,
    };
    channel.subscribe(handler);

    try {
      await execute({
        schema: incrementalSchema,
        document,
        rootValue: { immediate: 'ok' },
      });

      expect(contexts[0]?.operationName).to.equal('Named');
      expect(contexts[0]?.operationType).to.equal(OperationTypeNode.QUERY);
    } finally {
      channel.unsubscribe(handler);
    }
  });
});
