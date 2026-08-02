import { isPromise } from '../jsutils/isPromise.ts';
import type { PromiseOrValue } from '../jsutils/PromiseOrValue.ts';

import { withConcurrentAbruptClose } from './withConcurrentAbruptClose.ts';

/**
 * Given an AsyncIterable and a callback function, return an AsyncIterator
 * which produces values mapped via calling the callback function.
 *
 * @internal
 */
export function mapAsyncIterable<T, U>(
  iterable: AsyncGenerator<T> | AsyncIterable<T>,
  callback: (value: T) => PromiseOrValue<U>,
): AsyncGenerator<U, void, void> {
  const iterator = iterable[Symbol.asyncIterator]();
  const returnFn = iterator.return?.bind(iterator);
  const throwFn = iterator.throw?.bind(iterator);

  let abruptClose = false;

  const onReturn = returnFn
    ? () => {
        abruptClose = true;
        return callIgnoringErrors(returnFn);
      }
    : () => {
        abruptClose = true;
        return Promise.resolve();
      };

  const onThrow = throwFn
    ? (reason?: unknown) => {
        abruptClose = true;
        return callIgnoringErrors(() => throwFn(reason));
      }
    : onReturn;

  return withConcurrentAbruptClose(
    mapAsyncIterableImpl(iterator, callback, () => abruptClose),
    onReturn,
    onThrow,
  );
}

async function callIgnoringErrors(fn: () => Promise<unknown>): Promise<void> {
  try {
    await fn();
  } catch {
    // ignore error
  }
}

async function* mapAsyncIterableImpl<T, U>(
  iterator: AsyncIterator<T>,
  mapFn: (value: T) => PromiseOrValue<U>,
  isAbruptClose: () => boolean,
): AsyncGenerator<U, void, void> {
  let earlyExit = true;
  try {
    while (true) {
      // eslint-disable-next-line no-await-in-loop
      const iteration = await iterator.next();
      if (iteration.done) {
        earlyExit = false;
        return;
      }
      const result = mapFn(iteration.value);
      if (isPromise(result)) {
        // eslint-disable-next-line no-await-in-loop
        yield await result;
        continue;
      }
      yield result;
    }
  } finally {
    if (earlyExit && !isAbruptClose()) {
      await callIgnoringErrors(async () => {
        await iterator.return?.();
      });
    }
  }
}
