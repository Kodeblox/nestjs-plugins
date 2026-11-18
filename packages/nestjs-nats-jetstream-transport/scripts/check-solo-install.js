/**
 * Solo-install smoke check for the dual CI matrix: proves a consumer
 * installing exactly one driver generation resolves and loads the plugin.
 *
 * Both matrix cells install BOTH drivers and pin the driver explicitly, so
 * no cell proves a solo-generation install works at runtime. This script
 * fills that gap: it installs the packed tarball plus only one generation
 * (and its peers) in a fresh temp dir, then asserts the other generation
 * is absent, `resolveNatsDriver()` picks the installed one with no
 * ambiguity notice, and requiring the package entry does not evaluate the
 * missing driver at load time.
 *
 * Usage: node scripts/check-solo-install.js <tarball.tgz> <legacy|modern>
 * (as run by the `solo-install` CI job; also runnable locally — it only
 * needs registry access, no NATS server, since nothing connects).
 */
const { execFileSync } = require('child_process');
const fs = require('fs');
const { createRequire } = require('module');
const os = require('os');
const path = require('path');

const PLUGIN_ID = '@kodeblox/nestjs-nats-jetstream-transport';

const GENERATION_DEPS = {
  legacy: [
    'nats@^2',
    '@nestjs/common@^11',
    '@nestjs/core@^11',
    '@nestjs/microservices@^11',
    'rxjs@^7.4.0',
    'reflect-metadata@^0.1.13',
  ],
  modern: [
    '@nats-io/transport-node@^3',
    '@nats-io/jetstream@^3',
    '@nats-io/nats-core@^3',
    '@nestjs/common@^12',
    '@nestjs/core@^12',
    '@nestjs/microservices@^12',
    'rxjs@^7.4.0',
    'reflect-metadata@^0.1.13',
  ],
};

function main() {
  const [tarballArg, generation] = process.argv.slice(2);
  if (!tarballArg || !GENERATION_DEPS[generation]) {
    console.error(
      'usage: node scripts/check-solo-install.js <tarball.tgz> <legacy|modern>',
    );
    process.exit(2);
  }
  const tarball = path.resolve(tarballArg);
  if (!fs.existsSync(tarball)) {
    console.error(`solo-install check failed: tarball not found: ${tarball}`);
    process.exit(2);
  }

  const cellDir = fs.mkdtempSync(
    path.join(os.tmpdir(), `jetstream-solo-${generation}-`),
  );
  try {
    fs.writeFileSync(
      path.join(cellDir, 'package.json'),
      JSON.stringify({ name: `solo-${generation}`, private: true }),
    );
    try {
      execFileSync(
        'npm',
        [
          'install',
          '--no-audit',
          '--no-fund',
          '--no-update-notifier',
          tarball,
          ...GENERATION_DEPS[generation],
        ],
        { cwd: cellDir, stdio: ['ignore', 'pipe', 'pipe'], encoding: 'utf8' },
      );
    } catch (err) {
      console.error(`solo-install check failed: npm install failed:\n${err.stderr ?? err.message}`);
      process.exit(1);
    }

    const cellRequire = createRequire(path.join(cellDir, 'package.json'));
    const failures = [];
    const check = (cond, label) => {
      console.log(`${cond ? 'ok' : 'FAIL'}  ${label}`);
      if (!cond) failures.push(label);
    };
    const resolvable = (name) => {
      try {
        cellRequire.resolve(name);
        return true;
      } catch {
        return false;
      }
    };

    const wantLegacy = generation === 'legacy';
    check(
      resolvable('nats') === wantLegacy,
      `legacy driver (nats) ${wantLegacy ? 'present' : 'absent'}`,
    );
    check(
      resolvable('@nats-io/transport-node') === !wantLegacy,
      `modern driver (@nats-io/transport-node) ${wantLegacy ? 'absent' : 'present'}`,
    );
    const { resolveNatsDriver } = cellRequire(
      `${PLUGIN_ID}/dist/driver/resolve-driver.js`,
    );
    const resolution = resolveNatsDriver();
    check(
      resolution.driver === generation,
      `resolveNatsDriver() picks ${generation} (got ${resolution.driver})`,
    );
    check(
      resolution.notice === undefined,
      `no both-drivers ambiguity notice (got ${resolution.notice ?? 'none'})`,
    );
    cellRequire(PLUGIN_ID);
    check(
      true,
      'package entry loads without evaluating the missing driver',
    );
    // Version lookup walks up from the resolved main file: some packages
    // restrict the `./package.json` subpath via their exports map.
    const pkgVersion = (name) => {
      try {
        let dir = path.dirname(cellRequire.resolve(name));
        for (let i = 0; i < 6; i++) {
          const cand = path.join(dir, 'package.json');
          if (fs.existsSync(cand)) {
            const pkg = JSON.parse(fs.readFileSync(cand, 'utf8'));
            if (pkg.name === name) return pkg.version;
          }
          const parent = path.dirname(dir);
          if (parent === dir) break;
          dir = parent;
        }
      } catch {
        // fall through to unknown
      }
      return '?';
    };
    const versions = ['@nestjs/core', 'rxjs', 'reflect-metadata']
      .map((name) => `${name}@${pkgVersion(name)}`)
      .join(', ');
    const driverVersion = wantLegacy
      ? `nats@${pkgVersion('nats')}`
      : `@nats-io/transport-node@${pkgVersion('@nats-io/transport-node')}`;
    console.log(`solo ${generation} cell: ${driverVersion}, ${versions}`);
    if (failures.length > 0) {
      console.error(`\nsolo-install check failed: ${failures.length} check(s) failed`);
      process.exit(1);
    }
    console.log(`\nsolo-install check passed for ${generation}`);
  } finally {
    fs.rmSync(cellDir, { recursive: true, force: true });
  }
}

main();
