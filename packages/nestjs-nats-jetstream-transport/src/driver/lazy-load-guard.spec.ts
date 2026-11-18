/**
 * Lazy-load guard (handback finding 1, spec 001 section 4.2 rule 1).
 *
 * Phase 1 guarantee: the new driver-neutral modules must never statically
 * import a driver package, so a future modern-only install can load them
 * without evaluating `require('nats')`.
 *
 * Pinned interim state: `client.ts` / `server.ts` still statically import
 * `nats`, so true lazy loading is NOT yet achieved — any app importing this
 * package evaluates `require('nats')` at load time, and the resolve-driver
 * errors are advisory-only on legacy installs. The adapter phase must remove
 * those static imports (dynamic `import()` / `loadPackage`) and flip the
 * second test below; the modern CI cell additionally enforces
 * `scripts/check-no-static-nats` against the built dist (spec 001 section 6).
 */
import { readFileSync } from 'fs';
import { join } from 'path';

const srcRoot = join(__dirname, '..');
const read = (rel: string) => readFileSync(join(srcRoot, rel), 'utf8');

const STATIC_NATS_IMPORT =
  /(?:from\s+['"]nats['"]|require\(\s*['"]nats['"]\s*\))/;

describe('lazy driver loading (source-level guard)', () => {
  it.each([
    'driver/resolve-driver.ts',
    'utils/consumer-config.ts',
    'nats-jetstream.context.ts',
  ])('driver-neutral module %s has no static nats import', (rel) => {
    expect(read(rel)).not.toMatch(STATIC_NATS_IMPORT);
  });

  it.each(['client.ts', 'server.ts'])(
    'INTERIM: %s still statically imports nats (adapter phase removes this)',
    (rel) => {
      expect(read(rel)).toMatch(STATIC_NATS_IMPORT);
    },
  );
});
