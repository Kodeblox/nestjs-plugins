/**
 * Published-tarball guard for spec 001 section 6 (Phase 4 release gate).
 *
 * Packs this package exactly as `npm publish` would, then asserts the
 * artifact a modern-only (Nest 12) consumer installs:
 *
 *  1. no `require('nats')` / `require("nats")` string anywhere under the
 *     packed `dist/` — the lazy legacy loader downlevels to a computed
 *     `require(name)`, so even the lazy path leaves no literal behind;
 *  2. the packed manifest declares no hard `dependencies.nats` (both
 *     driver generations are optional peers since v3).
 *
 * Run: `npm run check:tarball:no-nats` (modern CI cell).
 */

const { execFileSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const LITERAL_NATS_REQUIRE = /require\(\s*['"]nats['"]\s*\)/;

function jsFiles(dir, out = []) {
  for (const entry of fs.readdirSync(dir)) {
    const full = path.join(dir, entry);
    if (fs.statSync(full).isDirectory()) {
      jsFiles(full, out);
    } else if (full.endsWith('.js')) {
      out.push(full);
    }
  }
  return out;
}

function main() {
  const pkgDir = __dirname
    ? path.join(__dirname, '..')
    : process.cwd();
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'jetstream-tarball-'));
  try {
    const out = execFileSync('npm', ['pack', '--pack-destination', tmp], {
      cwd: pkgDir,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const tgzName = out.trim().split('\n').pop();
    const tgz = path.join(tmp, tgzName);
    if (!fs.existsSync(tgz)) {
      throw new Error(`expected tarball not produced: ${tgzName}`);
    }
    const unpackDir = path.join(tmp, 'unpacked');
    fs.mkdirSync(unpackDir, { recursive: true });
    execFileSync('tar', ['-xzf', tgz, '-C', unpackDir], { stdio: 'inherit' });
    const root = path.join(unpackDir, 'package');

    const manifest = JSON.parse(
      fs.readFileSync(path.join(root, 'package.json'), 'utf8'),
    );
    if (manifest.dependencies && manifest.dependencies.nats) {
      throw new Error(
        `packed manifest still declares dependencies.nats=${manifest.dependencies.nats}; ` +
          `both driver generations must be optional peers`,
      );
    }

    const offenders = [];
    for (const file of jsFiles(path.join(root, 'dist'))) {
      const rel = path.relative(root, file);
      for (const [i, line] of fs.readFileSync(file, 'utf8').split('\n').entries()) {
        if (LITERAL_NATS_REQUIRE.test(line)) {
          offenders.push(`${rel}:${i + 1}: ${line.trim()}`);
        }
      }
    }
    if (offenders.length > 0) {
      throw new Error(
        `packed dist contains ${offenders.length} literal require('nats') occurrence(s):\n` +
          offenders.join('\n'),
      );
    }
    console.log(
      `tarball OK: ${tgzName} ships no dependencies.nats and no literal require('nats') in dist`,
    );
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

main();
