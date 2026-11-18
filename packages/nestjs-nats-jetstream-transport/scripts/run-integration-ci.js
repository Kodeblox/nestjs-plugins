/**
 * CI integration entry: the integration suite must RUN, not skip.
 *
 * Without NATS_URL every integration suite skips and jest still exits 0,
 * and the broker-restart test skips without docker control — both would
 * ship green while proving nothing. So this entry runs the suite with
 * --json and fails when any suite or test was skipped (or when nothing
 * ran at all). Local development keeps using `test:integration`, which
 * still skips with an explanatory message when no server is configured.
 *
 * Run: `npm run test:integration:ci` with NATS_URL (and NATS_CONTAINER
 * for broker-restart control) set, as the dual CI matrix does.
 */
const { execFileSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

function main() {
  const pkgDir = path.join(__dirname, '..');
  const out = path.join(
    fs.mkdtempSync(path.join(os.tmpdir(), 'jest-integration-')),
    'results.json',
  );
  // Resolve the CLI entry through the package directory, not the exports
  // map (jest-cli does not export its bin subpath). A resolution failure
  // here is a setup bug and must be loud, never a silent exit code.
  let jestBin;
  try {
    const jestCliPkg = require.resolve('jest-cli/package.json', {
      paths: [pkgDir],
    });
    jestBin = path.join(path.dirname(jestCliPkg), 'bin', 'jest.js');
    if (!fs.existsSync(jestBin)) {
      throw new Error(`jest bin missing at ${jestBin}`);
    }
  } catch (err) {
    console.error(`integration gate setup failed: ${err.message}`);
    process.exit(2);
  }
  try {
    execFileSync(
      process.execPath,
      [
        jestBin,
        '--config',
        './test/jest-integration.json',
        '--runInBand',
        '--json',
        '--outputFile',
        out,
      ],
      { cwd: pkgDir, stdio: 'inherit' },
    );
  } catch (err) {
    // Jest already printed the failures; propagate its exit code.
    process.exit(typeof err.status === 'number' ? err.status : 1);
  }
  const results = JSON.parse(fs.readFileSync(out, 'utf8'));
  fs.rmSync(path.dirname(out), { recursive: true, force: true });
  const total = results.numTotalTests ?? 0;
  const skippedTests = results.numPendingTests ?? 0;
  const skippedSuites = results.numPendingTestSuites ?? 0;
  if (total === 0 || skippedSuites > 0 || skippedTests > 0) {
    console.error(
      `integration gate failed: ${total} total tests, ` +
        `${skippedTests} skipped tests, ${skippedSuites} skipped suites — ` +
        `every integration test must run (set NATS_URL, and provide ` +
        `broker-restart control via NATS_CONTAINER)`,
    );
    process.exit(1);
  }
  console.log(`integration gate passed: ${total} tests, 0 skipped`);
}

main();
