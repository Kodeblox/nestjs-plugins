/**
 * Lazy-load guard (spec 001 section 4.2 rule 1).
 *
 * Phase 2 guarantee: no module reachable from the package entry point
 * statically imports a driver package, so a modern-only install can import
 * this package without evaluating a driver require at load time. The legacy
 * driver loads via dynamic `import()` and the builder modules at call time
 * only. `import type` references are allowed — they are erased at compile
 * time and carry no runtime require.
 *
 * The built-dist counterpart is `scripts/check-no-static-nats`, which fails
 * on any module-scope driver require in the packaged output (spec 001
 * section 6); the modern CI cell runs it green.
 */
import { readFileSync } from 'fs';
import { join } from 'path';

const srcRoot = join(__dirname, '..');
const read = (rel: string) => readFileSync(join(srcRoot, rel), 'utf8');

const hasStaticNatsImport = (source: string) =>
  source.split('\n').some(
    (line) =>
      !/^\s*import\s+type\b/.test(line) &&
      /(?:from\s+['"]nats['"]|require\(\s*['"]nats['"]\s*\))/.test(line),
  );

describe('lazy driver loading (source-level guard)', () => {
  it.each([
    'driver/resolve-driver.ts',
    'driver/nats-driver.ts',
    'utils/consumer-config.ts',
    'nats-jetstream.context.ts',
    'client.ts',
    'server.ts',
  ])('module %s has no static nats import (import type only)', (rel) => {
    expect(hasStaticNatsImport(read(rel))).toBe(false);
  });
});
