/**
 * Tarball guard for spec 001 section 6.
 *
 * Fails when the built output would evaluate a legacy `nats` driver require
 * at load time, which would crash a modern-only (Nest 12) install on import
 * — before lazy driver resolution ever runs. Run against the packaged
 * output:
 *
 *   npm run build && node scripts/check-no-static-nats.js dist
 *
 * Rule: tsc emits module-scope statements at column 0, so only a
 * column-0 `require("nats")` runs at load time. Lazy loads stay green:
 * dynamic `import()` downlevels to an indented `.then(() => require(...))`,
 * and call-time requires inside functions are indented too. Both CI cells
 * (legacy and modern) run this green; the legacy line passes because its
 * driver access is lazy, not because the driver is installed.
 */
const { readdirSync, readFileSync, statSync } = require('fs');
const { join } = require('path');

const LOAD_TIME_NATS_REQUIRE = /^\S.*require\(\s*['"]nats['"]\s*\)/;

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
  const bad = readFileSync(file, 'utf8')
    .split('\n')
    .some((line) => LOAD_TIME_NATS_REQUIRE.test(line));
  if (bad) {
    offenders.push(file);
  }
}

if (offenders.length > 0) {
  console.error(
    `load-time nats driver import found in ${offenders.length} built file(s):`,
  );
  for (const file of offenders) {
    console.error(`  ${file}`);
  }
  process.exit(1);
}
console.log(`no load-time nats driver imports in ${target}`);
