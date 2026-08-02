import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { expect } from 'chai';

/**
 * Process-level integration for the client-disconnect cancellation feature.
 *
 * Where the unit layer imports the runtime from source, this layer runs a
 * separate Node process that imports GraphQL.js from the BUILT `npmDist` ESM
 * entry and subscribes to the real `node:diagnostics_channel`. It confirms the
 * feature survives the build/publish pipeline and behaves identically to the
 * source-level tests across the same three paths: normal completion, user
 * cancellation, and resolver failure - never double-terminating the tracing
 * lifecycle and never leaking an active iterator.
 *
 * The child prints a single `DIAGNOSTICS_REPORT:` JSON line describing each
 * scenario. To keep repeated runs order-independent, the build product is
 * produced on demand when the ESM entry is missing, so this test does not
 * depend on another script or test having run first.
 */

const repoRoot = path.resolve(
  fileURLToPath(import.meta.url),
  '..',
  '..',
  '..',
  '..',
);
const npmDistEsmEntry = path.join(repoRoot, 'npmDist', 'index.mjs');
const childFixture = path.join(
  repoRoot,
  'resources',
  '__fixtures__',
  'diagnostics-cancellation-child.mjs',
);

// eslint-disable-next-line no-undef
const isWindows = process.platform === 'win32';
const npmCommand = isWindows ? 'npm.cmd' : 'npm';

function ensureNpmDistBuilt(): void {
  if (existsSync(npmDistEsmEntry)) {
    return;
  }
  const build = spawnSync(npmCommand, ['run', 'build:npm'], {
    cwd: repoRoot,
    encoding: 'utf8',
    shell: isWindows,
  });
  assert(
    build.status === 0 && existsSync(npmDistEsmEntry),
    `Failed to build npmDist for integration test:\n${build.stdout ?? ''}\n${build.stderr ?? ''}`,
  );
}

interface ExecuteScenario {
  phases: Array<string>;
  payloadCount?: number;
  rejectedMessage?: string | null;
  followUpDone?: boolean;
  returnCallCount?: number;
  errorMessage?: string | null;
}

interface SubscribeScenario {
  phases: Array<string>;
  firstValue?: unknown;
  returnDone?: boolean;
  rejectedMessage?: string | null;
  returnCallCount?: number;
  isStream?: boolean;
  errorMessage?: string | null;
}

interface DiagnosticsReport {
  execute: {
    normal: ExecuteScenario;
    cancelled: ExecuteScenario;
    resolverFailure: ExecuteScenario;
  };
  subscribe: {
    normal: SubscribeScenario;
    cancelled: SubscribeScenario;
    resolverFailure: SubscribeScenario;
  };
}

let cachedReport: DiagnosticsReport | undefined;

/**
 * Run the child fixture once against the built ESM entry and parse its report.
 * The result is memoized so the (single) subprocess spawn is shared by all
 * assertions in this file without imposing an execution order between them.
 */
function runChildReport(): DiagnosticsReport {
  if (cachedReport !== undefined) {
    return cachedReport;
  }
  ensureNpmDistBuilt();

  const child = spawnSync(
    // eslint-disable-next-line no-undef
    process.execPath,
    [childFixture, npmDistEsmEntry],
    { cwd: repoRoot, encoding: 'utf8' },
  );

  assert(
    child.status === 0,
    `Child diagnostics fixture failed (status ${child.status}):\n${child.stdout}\n${child.stderr}`,
  );

  const line = child.stdout
    .split(/\r?\n/)
    .find((entry) => entry.startsWith('DIAGNOSTICS_REPORT:'));
  assert(
    line !== undefined,
    `Child did not emit a diagnostics report. stdout:\n${child.stdout}\nstderr:\n${child.stderr}`,
  );

  cachedReport = JSON.parse(
    line.slice('DIAGNOSTICS_REPORT:'.length),
  ) as DiagnosticsReport;
  return cachedReport;
}

/** Assert exactly one `start` and no repeated terminal events. */
function expectSingleTermination(phases: ReadonlyArray<string>): void {
  expect(phases.filter((p) => p === 'start')).to.have.lengthOf(1);
  expect(phases.filter((p) => p === 'end')).to.have.lengthOf(1);
  expect(phases.filter((p) => p === 'asyncEnd').length).to.be.lessThanOrEqual(
    1,
  );
  expect(phases.filter((p) => p === 'error').length).to.be.lessThanOrEqual(1);
}

describe('diagnostics lifecycle for cancellable execution (npmDist integration)', () => {
  describe('cancellable incremental query on graphql:execute', () => {
    it('normal completion terminates the lifecycle once and delivers all payloads', () => {
      const { normal } = runChildReport().execute;
      expect(normal.phases).to.deep.equal([
        'start',
        'end',
        'asyncStart',
        'asyncEnd',
      ]);
      expectSingleTermination(normal.phases);
      expect(normal.phases).to.not.include('error');
      expect(normal.payloadCount).to.be.greaterThan(0);
    });

    it('user cancellation terminates once and cleans up the stream exactly once', () => {
      const { cancelled } = runChildReport().execute;
      expect(cancelled.phases).to.deep.equal([
        'start',
        'end',
        'asyncStart',
        'asyncEnd',
      ]);
      expectSingleTermination(cancelled.phases);
      expect(cancelled.rejectedMessage).to.equal('This operation was aborted');
      expect(cancelled.followUpDone).to.equal(true);
      expect(cancelled.returnCallCount).to.equal(1);
    });

    it('resolver failure is reported on the patch, not as the traced error lifecycle', () => {
      const { resolverFailure } = runChildReport().execute;
      expect(resolverFailure.phases).to.deep.equal(['start', 'end']);
      expectSingleTermination(resolverFailure.phases);
      expect(resolverFailure.phases).to.not.include('error');
      expect(resolverFailure.errorMessage).to.equal('resolver exploded');
    });
  });

  describe('cancellable subscription on graphql:subscribe', () => {
    it('normal completion terminates once and returns the stream cleanly', () => {
      const { normal } = runChildReport().subscribe;
      expect(normal.phases).to.deep.equal(['start', 'end']);
      expectSingleTermination(normal.phases);
      expect(normal.firstValue).to.deep.equal({ data: { ticks: 'one' } });
      expect(normal.returnDone).to.equal(true);
    });

    it('user cancellation rejects the pending consumption without double terminating', () => {
      const { cancelled } = runChildReport().subscribe;
      expect(cancelled.phases).to.deep.equal(['start', 'end']);
      expectSingleTermination(cancelled.phases);
      expect(cancelled.rejectedMessage).to.equal('This operation was aborted');
      // Cancellation does not itself re-invoke the source cleanup.
      expect(cancelled.returnCallCount).to.equal(0);
    });

    it('resolver failure at setup yields an error result with a single lifecycle', () => {
      const { resolverFailure } = runChildReport().subscribe;
      expect(resolverFailure.phases).to.deep.equal(['start', 'end']);
      expectSingleTermination(resolverFailure.phases);
      expect(resolverFailure.isStream).to.equal(false);
      expect(resolverFailure.errorMessage).to.equal(
        'subscription setup failed',
      );
    });
  });
});
