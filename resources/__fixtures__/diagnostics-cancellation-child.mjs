// Process-level diagnostics fixture. Runs in a fresh Node process against the
// BUILT graphql-js ESM entry (npmDist/index.mjs), subscribing to the real
// node:diagnostics_channel. It exercises a cancellable incremental query and a
// cancellable subscription across three paths - normal completion, user
// cancellation, and resolver failure - and prints a single JSON line
// describing, for each scenario, the ordered lifecycle phases observed on the
// tracing channel and whether every async iterator was cleaned up.
//
// The graphql module path is passed as argv[2] so the harness controls which
// build artifact is loaded.
/* eslint-disable */
import dc from 'node:diagnostics_channel';
import { pathToFileURL } from 'node:url';

const graphqlModulePath = process.argv[2];
const { buildSchema, parse, experimentalExecuteIncrementally, subscribe } =
  await import(pathToFileURL(graphqlModulePath).href);

const schema = buildSchema(`
  type Widget { id: ID slow: String items: [String] nonNullName: String! }
  type Query { widget: Widget }
  type Subscription { ticks: String }
`);

function promiseWithResolvers() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function neverResolves() {
  return new Promise(() => {});
}

// Record ordered lifecycle phases on a tracing channel while `fn` runs.
async function withPhases(channelName, fn) {
  const phases = [];
  const handler = {
    start: () => phases.push('start'),
    end: () => phases.push('end'),
    asyncStart: () => phases.push('asyncStart'),
    asyncEnd: () => phases.push('asyncEnd'),
    error: () => phases.push('error'),
  };
  const channel = dc.tracingChannel(channelName);
  channel.subscribe(handler);
  try {
    const extra = await fn();
    return { phases, ...extra };
  } finally {
    channel.unsubscribe(handler);
  }
}

const incrementalDocument = parse(`
  query {
    widget {
      id
      ... @defer(label: "slow") { slow }
      items @stream(initialCount: 0)
    }
  }
`);

// PATH 1: incremental query completes normally.
async function executeNormal() {
  async function* items() {
    yield await Promise.resolve('a');
    yield 'b';
  }
  return withPhases('graphql:execute', async () => {
    const abortController = new AbortController();
    const result = await experimentalExecuteIncrementally({
      schema,
      document: incrementalDocument,
      rootValue: {
        widget: { id: '1', slow: () => 'slow-value', items: () => items() },
      },
      abortSignal: abortController.signal,
    });
    let payloadCount = 0;
    for await (const _patch of result.subsequentResults) {
      payloadCount++;
    }
    return { payloadCount };
  });
}

// PATH 2: incremental query is cancelled by the client after the initial
// payload, while a stream is parked and a deferred field is pending.
async function executeCancelled() {
  let returnCallCount = 0;
  const parked = promiseWithResolvers();
  const slowStarted = promiseWithResolvers();
  const slow = promiseWithResolvers();
  let index = 0;
  const source = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (index < 1) {
        index++;
        return Promise.resolve({ value: 'a', done: false });
      }
      parked.resolve();
      return neverResolves();
    },
    return() {
      returnCallCount++;
      return Promise.resolve({ value: undefined, done: true });
    },
  };
  return withPhases('graphql:execute', async () => {
    const abortController = new AbortController();
    const result = await experimentalExecuteIncrementally({
      schema,
      document: incrementalDocument,
      rootValue: {
        widget: {
          id: '1',
          slow() {
            slowStarted.resolve();
            return slow.promise;
          },
          items: () => source,
        },
      },
      enableEarlyExecution: true,
      abortSignal: abortController.signal,
    });
    const iterator = result.subsequentResults[Symbol.asyncIterator]();
    await iterator.next();
    const nextPromise = iterator.next();
    await slowStarted.promise;
    await parked.promise;
    abortController.abort();

    let rejectedMessage = null;
    try {
      await nextPromise;
    } catch (error) {
      rejectedMessage = error.message;
    }

    await Promise.resolve();
    const followUp = await iterator.next();
    return { rejectedMessage, followUpDone: followUp.done, returnCallCount };
  });
}

// PATH 3: a deferred resolver rejects; the failure is reported on the patch and
// the traced call is not the `error` lifecycle.
async function executeResolverFailure() {
  const document = parse(`
    query {
      widget {
        id
        ... @defer(label: "boom") { nonNullName }
      }
    }
  `);
  return withPhases('graphql:execute', async () => {
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
    const patches = [result.initialResult];
    for await (const patch of result.subsequentResults) {
      patches.push(patch);
    }
    const completed = patches
      .flatMap((patch) => (patch.completed ? patch.completed : []))
      .find((entry) => entry.id === '0');
    const errorMessage =
      completed && completed.errors ? completed.errors[0].message : null;
    return { errorMessage };
  });
}

async function* ticks() {
  await Promise.resolve();
  yield { ticks: 'one' };
  yield { ticks: 'two' };
}

// PATH 1 (subscription): normal completion, client closes the stream.
async function subscribeNormal() {
  return withPhases('graphql:subscribe', async () => {
    const abortController = new AbortController();
    const subscription = await subscribe({
      schema,
      document: parse('subscription { ticks }'),
      rootValue: { ticks },
      abortSignal: abortController.signal,
    });
    const first = await subscription.next();
    const returned = await subscription.return();
    return {
      firstValue: first.value,
      returnDone: returned.done,
    };
  });
}

// PATH 2 (subscription): user cancellation rejects a pending consumption.
async function subscribeCancelled() {
  let returnCallCount = 0;
  const nextStarted = promiseWithResolvers();
  const source = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      nextStarted.resolve();
      return neverResolves();
    },
    return() {
      returnCallCount++;
      return Promise.resolve({ value: undefined, done: true });
    },
  };
  return withPhases('graphql:subscribe', async () => {
    const abortController = new AbortController();
    const subscription = await subscribe({
      schema,
      document: parse('subscription { ticks }'),
      rootValue: { ticks: () => source },
      abortSignal: abortController.signal,
    });
    const nextPromise = subscription.next();
    await nextStarted.promise;
    abortController.abort();
    let rejectedMessage = null;
    try {
      await nextPromise;
    } catch (error) {
      rejectedMessage = error.message;
    }
    await Promise.resolve();
    return { rejectedMessage, returnCallCount };
  });
}

// PATH 3 (subscription): the subscribe resolver throws during setup.
async function subscribeResolverFailure() {
  return withPhases('graphql:subscribe', async () => {
    const abortController = new AbortController();
    const result = await subscribe({
      schema,
      document: parse('subscription { ticks }'),
      rootValue: {
        ticks: () => {
          throw new Error('subscription setup failed');
        },
      },
      abortSignal: abortController.signal,
    });
    const isStream =
      result != null && typeof result[Symbol.asyncIterator] === 'function';
    const errorMessage =
      !isStream && result.errors ? result.errors[0].message : null;
    return { isStream, errorMessage };
  });
}

async function main() {
  const report = {
    execute: {
      normal: await executeNormal(),
      cancelled: await executeCancelled(),
      resolverFailure: await executeResolverFailure(),
    },
    subscribe: {
      normal: await subscribeNormal(),
      cancelled: await subscribeCancelled(),
      resolverFailure: await subscribeResolverFailure(),
    },
  };
  process.stdout.write('DIAGNOSTICS_REPORT:' + JSON.stringify(report) + '\n');
}

main().then(
  () => {
    process.exit(0);
  },
  (error) => {
    process.stderr.write(String(error && error.stack ? error.stack : error));
    process.exit(1);
  },
);
