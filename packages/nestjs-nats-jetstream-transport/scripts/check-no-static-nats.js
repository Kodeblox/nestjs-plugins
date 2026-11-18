/**
 * Tarball guard for spec 001 section 6 (handback finding 1).
 *
 * Fails when the built output statically requires the legacy `nats` driver,
 * which would crash a modern-only (Nest 12) install at load time — before
 * lazy driver resolution ever runs. Run against the packaged output:
 *
 *   npm run build && node scripts/check-no-static-nats.js dist
 *
 * The modern CI cell must run this green. The legacy line is exempt until
 * the adapter phase removes the static `nats` imports from client.ts /
 * server.ts (see src/driver/lazy-load-guard.spec.ts); until then this
 * script is expected to report those two files.
 */
const { readdirSync, readFileSync, statSync } = require('fs');
const { join } = require('path');

const STATIC_NATS_REQUIRE = /require\(\s*['"]nats['"]\s*\)/;

function jsFiles(dir, out = []) {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      jsFiles(full, out);
    } else if (full.endsWith('.js')) {
      out.push(full);
    }
  }
  return out;
}

const target = process.argv[2] || 'dist';
let offenders = [];
for (const file of jsFiles(target)) {
  if (STATIC_NATS_REQUIRE.test(readFileSync(file, 'utf8'))) {
    offenders.push(file);
  }
}

if (offenders.length > 0) {
  console.error(
    `static nats driver import found in ${offenders.length} built file(s):`,
  );
  for (const file of offenders) {
    console.error(`  ${file}`);
  }
  process.exit(1);
}
console.log(`no static nats driver imports in ${target}`);
