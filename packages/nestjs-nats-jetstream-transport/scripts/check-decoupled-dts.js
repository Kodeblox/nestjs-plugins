/**
 * Decoupled-types gate for spec 001 section 6 (handback finding 5).
 *
 * Typechecks `test/typecheck-modern-only` (strict + skipLibCheck:false)
 * against the BUILT package with the legacy `nats` package made
 * unresolvable. A bare `paths` mapping is not enough — TypeScript falls back
 * to node_modules — so this script shadows node_modules with symlinks to
 * only the packages a modern-only consumer installs, leaving `nats`
 * genuinely absent. Any driver import leaking into the published `.d.ts`
 * then fails the check.
 *
 * Run: `npm run check:decoupled-dts` (builds first). The shadowing
 * node_modules is removed afterwards; only its absence is load-bearing.
 */
const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const pkgDir = path.join(__dirname, '..');
const fixtureDir = path.join(pkgDir, 'test', 'typecheck-modern-only');
const shadowDir = path.join(fixtureDir, 'node_modules');
const rootModules = path.join(
  pkgDir,
  '..',
  '..',
  'node_modules',
);

// Symlinked into the shadow node_modules (workspace-hoisted install layout).
const LINKS = [
  '@nestjs/common',
  '@nestjs/core',
  '@nestjs/microservices',
  '@nats-io/jetstream',
  '@nats-io/nats-core',
  '@nats-io/transport-node',
  'reflect-metadata',
  'rxjs',
  'tslib',
];

// A blocking stub: with this present, `nats` resolves but exports nothing,
// so any `from 'nats'` in the reachable `.d.ts` fails loudly instead of
// falling through to a real install elsewhere in the tree.
const NATS_STUB_FILES = {
  'package.json': JSON.stringify({
    name: 'nats-stub-for-decoupled-dts-check',
    version: '0.0.0',
    types: './index.d.ts',
    main: './index.js',
  }),
  'index.js': `'use strict';\nmodule.exports = {};\n`,
  'index.d.ts': 'export {};\n',
};

function link(target, linkPath) {
  fs.mkdirSync(path.dirname(linkPath), { recursive: true });
  try {
    fs.unlinkSync(linkPath);
  } catch {
    // not present yet
  }
  fs.symlinkSync(
    target,
    linkPath,
    process.platform === 'win32' ? 'junction' : 'dir',
  );
}

function main() {
  fs.rmSync(shadowDir, { recursive: true, force: true });
  for (const name of LINKS) {
    link(path.join(rootModules, name), path.join(shadowDir, name));
  }
  const stubDir = path.join(shadowDir, 'nats');
  fs.mkdirSync(stubDir, { recursive: true });
  for (const [file, content] of Object.entries(NATS_STUB_FILES)) {
    fs.writeFileSync(path.join(stubDir, file), content);
  }
  try {
    execFileSync(
      process.execPath,
      [
        require.resolve('typescript/lib/tsc.js'),
        '-p',
        path.join(fixtureDir, 'tsconfig.json'),
      ],
      { stdio: 'inherit' },
    );
  } finally {
    fs.rmSync(shadowDir, { recursive: true, force: true });
  }
}

main();
