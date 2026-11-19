# 002 — NATS startup policy for nestjs-nats-jetstream-transport

- **Status:** Proposed
- **Date:** 2026-11-18
- **Source:** authored against a consuming app’s installed copy and handed
  to this repo as the library-side spec. App-side paths below
  (`packages/modules/src/jetstream.ts`,
  `packages/modules/src/__test__/`) refer to that app, not this one.
- **Note on names:** the spec targets `@nestjs-plugins/nestjs-nats-jetstream-transport`
  v2.2.6 (the name under which the app installs the library) and nats.js
  2.29.3. All option/field names are proposals; this repo owns final naming.
  Implement against the v3 driver adapter on `main` (spec 001), not the
  installed v2.2.6 copy — in particular, startup retries must compose with
  the shared in-flight connect guard and the `driver` resolution errors, and
  per-attempt reconnects must not leak connections.

---

# Library-side spec: NATS startup policy for nestjs-nats-jetstream-transport

Target: `@nestjs-plugins/nestjs-nats-jetstream-transport` v2.2.6 (installed copy under `node_modules/`), nats.js 2.29.3. No implementation in this repo — spec only. All option/field names below are proposals; the library session owns final naming.

## 1. Root cause (brief, with evidence)

`NatsJetStreamServer.listen()` awaits `connect()` and throws `CONNECTION_REFUSED` before ever invoking its callback, while Nest's `NestMicroservice.listen()` settles only via that callback and never handles the returned promise. One failure therefore crashes node (unhandled rejection) AND hangs `startAllMicroservices()` forever.

Evidence pointers (installed copies, verified):

- `node_modules/@nestjs-plugins/nestjs-nats-jetstream-transport/dist/server.js`, `listen()` (lines 17-31): `await connect(...)`, `await jetstreamManager(...)`, `await setupStream()`, `await bindEventHandlers()` all run before the single `callback()` on line 30. Any throw above line 30 skips the callback entirely.
- `node_modules/@nestjs/microservices/nest-microservice.js`, `listen()` (line 166): wraps the transport call as `return new Promise((resolve, reject) => { this.serverInstance.listen((err, info) => { ... err ? reject(err) : resolve(info); }); })`. The promise settles only through the `(err, info)` callback; the async return value of the transport's `listen()` is discarded, so a throw becomes an unhandled rejection while this promise never settles.
- `node_modules/nats/lib/nats-base-client/protocol.js`, `dodialLoop()` (line 507): on first-connect failure with the default `waitOnFirstConnect: false` (`node_modules/nats/lib/nats-base-client/options.js` line 49), the loop calls `this.servers.removeCurrentServer()` and, with a single configured server now gone, `throw lastError` (lines 519-530 select-server-empty branch). So broker-down at boot surfaces as a single synchronous-looking `await connect()` throw, not a hang inside nats.js.
- Scratch probe result (recorded, not committed): awaiting the raw transport `listen()` with no broker rejects with a catchable `CONNECTION_REFUSED` error. The rejection itself is catchable — it is the Nest wrapper that drops it (see previous point).

## 2. Part A — upstream bug fix (no new config)

Wrap the `listen()` body in try/catch and route ANY failure — `connect()`, `jetstreamManager()`, `setupStream()`, `bindEventHandlers()` — to `callback(err)`. Nest's `(err, info)` callback signature already accepts an error (see `nest-microservice.js` above); the transport just never uses it.

Sketch (behavior, not final code):

```ts
async listen(callback: (err?: unknown, info?: unknown) => void) {
  try {
    // ... existing body unchanged ...
    callback();
  } catch (err) {
    callback(err);
  }
}
```

Notes for the implementer:

- Catch everything thrown by the startup path, including `bindMessageHandlers()` if it can throw, and the `connectedHook` call.
- Do not call `callback()` twice: success path calls `callback()` with no error, failure path calls `callback(err)` exactly once.
- On the failure path, release partial state if cheap (e.g. close a `nc` opened before a later step failed) but keep Part A minimal: the contract is "callback always fires exactly once". Full teardown-bounding is Part B / app-side concern.
- Stated guarantee after this fix: a startup failure rejects `startAllMicroservices()` cleanly instead of crashing the process with an unhandled rejection plus hanging the boot promise forever.

Behavior change flag: today a broker-down boot crashes node (unhandled rejection) and hangs. After Part A it becomes a catchable promise rejection. Migration note for consumers: anyone who currently relies on (or works around) the crash — e.g. a process manager restarting the container — must instead handle the `startAllMicroservices()` rejection (exit, or retry per Part B). This is the intended fix, not a regression.

## 3. Part B — configurable startup policy (new options)

Add an option block on `NatsJetStreamServerOptions` (current shape: `node_modules/@nestjs-plugins/nestjs-nats-jetstream-transport/dist/interfaces/nats-jetstream-server-options.interface.d.ts` — `connectionOptions`, `consumerOptions`, `jetStreamOptions?`, `streamConfig?`). Proposed shape (sketch only):

```ts
// Proposed names — library session owns final naming.
type StartupPolicyMode = 'fail-fast' | 'retry' | 'retry-forever';

interface StartupPolicyOptions {
  mode?: StartupPolicyMode;   // default: see below
  maxAttempts?: number;       // total attempts in 'retry' mode (default proposal: 5)
  delayMs?: number;           // delay between attempts (default proposal: from reconnectDelayHandler)
  onAttemptFailed?: (info: { attempt: number; maxAttempts?: number; err: unknown; recoverable: boolean }) => void;
}
```

Semantics:

- `fail-fast`: attempt once; on failure tear down partial state and route the error to `callback(err)` (i.e. Part A behavior, no loop).
- `retry`: attempt up to `maxAttempts` total; between attempts wait `delayMs` and log each failure with its error class; after the last attempt fails, route the last error to `callback(err)`. Attempts after a non-recoverable error stop immediately (see classifier).
- `retry-forever`: same as `retry` with no attempt cap; keeps logging each failed attempt; calls `callback()` (success) only once attach succeeds.

Error classifier (proposal, reuse the app-side taxonomy as a starting point):

- Recoverable (retry): connection-refused, connection timeout, DNS-style failures (`ENOTFOUND`, `EAI_AGAIN`, `ECONNRESET` during dial), server-unavailable NATS codes.
- Non-recoverable (fail fast even mid-loop): auth failures, bad stream config (unknown fields, invalid subjects), genuinely held port (`EADDRINUSE`).
- Log the error class per attempt (recoverable vs fatal) with attempt count, e.g. `[attempt 3/5] transient CONNECTION_REFUSED, retrying in <delay>ms` vs `fatal <code>, aborting`.

Reuse guidance:

- Default `delayMs` from the existing `reconnectDelayHandler` (`connectionOptions.reconnectDelayHandler`, nats.js `DEFAULT_RECONNECT_TIME_WAIT` fallback): call it per gap so a user-supplied backoff function keeps working for startup gaps too.
- Do NOT reuse `maxReconnectAttempts` for the startup attempt cap. It governs post-connect reconnects inside nats.js (`dodialLoop` lines ~547-551); overloading it conflates initial-attach retries with steady-state reconnects. Separate field, separate counter.
- Recommended default that preserves current success-path behavior: `mode: 'fail-fast'` (or equivalently, no retry unless opted in). Success path is byte-identical to today — single attempt, `callback()` on success. Operators who want boot resilience opt into `retry` / `retry-forever` explicitly. If the library prefers resilience-by-default, `retry-forever` with the `reconnectDelayHandler` delay is the defensible alternative, but that changes boot behavior for all consumers and must be flagged as such.

Idempotency constraints for the implementer:

- Each attempt must be safe to repeat: reset or reuse `this.nc` consistently (do not leak one connection per attempt), and subscription binding must not accumulate duplicates across attempts — broker-returns-mid-retry must end in a single attach with one subscription per subject.
- `callback` fires exactly once overall (on final success or terminal failure), never per attempt.

## 4. Acceptance criteria

- Broker down at boot, `fail-fast`: `startAllMicroservices()` rejects via the callback (no unhandled rejection, no hang). Process does not crash on its own; the caller observes the rejection.
- Broker down at boot, `retry` with N attempts: rejects after exactly N attempts; attempt log shows N failures with error classes; no unhandled rejection; process still responsive until the rejection propagates.
- Broker down at boot, `retry-forever`: never rejects, never crashes; keeps logging each failed attempt with its delay; process stays alive.
- Broker returns mid-retry (any retry mode): single attach — one ready log, one subscription per subject, no duplicate client-side subscriptions; `callback()` fires once with success.
- Zero unhandled rejections in all modes (assert via `process.on('unhandledRejection')` in the test harness — none fire).
- Non-recoverable error (bad credentials, invalid stream config, held port) in any retry mode: aborts after the first attempt and routes the error to `callback(err)` — no pointless retries.
- Existing library tests unaffected: full suite passes unmodified.

## 5. Migration note for our apps

Our local workaround (on `dev` post-MR-493; app-side write-up at `docs/nats-startup-resilience.md` (in the consuming app)) stays partially in place after upstream ships. Mapping of each local piece to its upstream counterpart:

- `ResilientNatsJetStreamServer` callback-routing hack (`packages/modules/src/jetstream.ts`, `listen` override routing connect/`setupStream`/binding failures to `callback(err)`) — DROPPABLE once upstream ships Part A. It exists only because upstream `listen()` never calls back with an error. Keep ours until the upgraded version is vendored; then delete the override and use the stock server.
- App-side retry loop (`startWithRetry` / `startNatsMicroserviceWithRetry`, same file) — STAYS regardless. Upstream Part B covers in-transport retries, but our loop also owns the bootstrap ordering contract (HTTP binds independently of NATS, gRPC binds exactly once outside the loop). Even with upstream retries enabled, the loop is the belt-and-braces that keeps `app.listen()` reachable.
- Error taxonomy (`isRecoverableStartupError`: retry refused/timeout/DNS, fail fast on auth/bad-config/`EADDRINUSE`) — STAYS as the app-side classifier. Propose it as the seed for upstream's Part B classifier, but keep the local copy authoritative for our bootstrap until upstream's taxonomy is reviewed against ours.
- Teardown bounding (`TEARDOWN_DRAIN_TIMEOUT_MS` drain-vs-close race, per-subscription tracking with rejection handlers) — STAYS regardless. It guards the half-open failure path (subscriptions created before a later step throws) that Part A alone does not bound.
- Env wiring (`NATS_STARTUP_RETRY_MS` / `DEFAULT_NATS_STARTUP_RETRY_MS`, plus unchanged `NATS_RECONNECT_WAIT_MS` / `NATS_MAX_RECONNECT_ATTEMPTS`) — STAYS. If upstream Part B lands with its own delay/attempt knobs, wire our env values into the upstream options rather than running two independent retry layers; until then the env drives our loop only.

Upgrade checklist (when consuming the fixed library version): bump the dependency, remove the `listen` try/catch override, keep the retry loop + taxonomy + teardown + env, re-run the `packages/modules/src/__test__/` regression specs plus a broker-down boot smoke test.
