import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { expect } from 'chai';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const npmDistEntry = path.join(repoRoot, 'npmDist', 'index.mjs');
const fixturePath = path.join(
  repoRoot,
  'src',
  '__testUtils__',
  'npmDist-abort-fixture.mjs',
);

const npmDistExists = fs.existsSync(npmDistEntry);

describe('npmDist ESM abort integration', { skip: !npmDistExists }, () => {
  it('runs cancellable incremental query and subscription against the built ESM entry', () => {
    // eslint-disable-next-line no-undef
    const output = execFileSync(process.execPath, [fixturePath], {
      cwd: repoRoot,
      env: {
        // eslint-disable-next-line no-undef
        ...process.env,
        GRAPHQL_NPMDIST_PATH: npmDistEntry,
      },
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    const lastLine = output.trim().split('\n').pop() ?? '';
    const result = JSON.parse(lastLine) as {
      ok: boolean;
      results: { [key: string]: { [key: string]: number } };
    };

    expect(result.ok).to.equal(true);

    const scenarios = Object.keys(result.results);
    expect(scenarios.length).to.be.greaterThan(0);

    for (const counts of Object.values(result.results)) {
      expect(counts.start).to.equal(1);
      expect(counts.end).to.be.at.most(1);
      expect(counts.asyncEnd ?? 0).to.be.at.most(1);
      expect(counts.error ?? 0).to.be.at.most(1);
    }
  });
});

if (!npmDistExists) {
  // eslint-disable-next-line no-console
  console.log(
    '  [skip] npmDist ESM abort integration tests (run `npm run build:npm` first)',
  );
}

