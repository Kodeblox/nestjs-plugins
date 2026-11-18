# 001 — Supporting NestJS 12 / NATS v3 alongside NestJS ≤11 / NATS v2

- **Status:** Proposed
- **Date:** 2026-10-20
- **Package in scope:** `@kodeblox/nestjs-nats-jetstream-transport`
  (`packages/nestjs-nats-jetstream-transport`), plus its demo apps
  `packages/nats-listener` and `packages/nats-publisher`
- **Question:** NestJS 12 moved its NATS transport to the updated nats.js
  library. How do we support both the old and the new stack?

## TL;DR / Recommendation

Do **not** try to make one import of `nats` work for both worlds — the
package itself is gone in the new world. Instead:

1. **Introduce one major release of this plugin (v3) that supports both
   stacks from a single codebase**, by putting the NATS driver behind an
   internal adapter interface and loading exactly one driver at runtime.
2. Declare **both** NATS drivers as **optional peer dependencies**
   (`nats` for legacy, `@nats-io/transport-node` + `@nats-io/jetstream`
   for modern), and widen the `@nestjs/*` peers to cover both Nest
   generations. The consumer installs only the driver matching their Nest
   major.
3. Keep the plugin's **public API driver-neutral** (own option/context
   types, JSON handled internally) so the public `.d.ts` never imports
   either `nats` or `@nats-io/*`.
4. Ship it as a **major bump with a migration guide**, keep the current
   v2.x line on a maintenance branch for Nest ≤11-only fixes, and test
   both ends in a CI matrix against a real NATS server.

The alternative "just upgrade to NATS v3 and drop v2" is simpler and is
listed as Option D, but it abandons every Nest ≤11 user, which is what
"support both" rules out. Splitting into two entry points (Option C) is
the fallback if the adapter proves too leaky.

---

## 1. What actually changed (verified facts)

### 1.1 NestJS 12 targets NATS v3

NestJS 12's own migration guide has a dedicated "NATS v3" section:

> "The microservices package now targets NATS v3, which includes a
> breaking dependency change. If you use the NATS transport, replace the
> old `nats` package with `@nats-io/transport-node`"
> — [NestJS migration guide (11 → 12)](https://docs.nestjs.com/migration-guide)

```bash
$ npm uninstall nats
$ npm install @nats-io/transport-node
```

The registry metadata agrees. Verified on 2026-10-20 with
`npm view @nestjs/microservices@<major> peerDependencies`:

| `@nestjs/microservices` | NATS peer (optional) | `@nestjs/common` / `core` peer |
| --- | --- | --- |
| 8.x – 11.x | `nats: *` | `^8` … `^11` respectively |
| 12.x | `@nats-io/transport-node: *` (no `nats`) | `^12` (12.0.0 briefly said `^11`; fixed from 12.0.1) |

Both peers are marked optional via `peerDependenciesMeta`, i.e. Nest
itself already demonstrates the "optional peer, install only what you
use" pattern this spec recommends for our plugin.

Two further Nest 12 changes matter to consumers of this plugin:

- **Serialization:** "Starting with NestJS v12, Nest serializes NATS
  packets as JSON strings, and custom NATS deserializers receive the
  full NATS message object instead of a raw `Uint8Array`" — read payloads
  with `msg.json()`. This affects anyone who combined Nest's built-in
  `Transport.NATS` with this plugin's records/headers.
  ([migration guide](https://docs.nestjs.com/migration-guide),
  [NATS transport docs](https://docs.nestjs.com/microservices/nats))
- **Helpers moved:** `headers()` and friends now come from
  `@nats-io/nats-core` (`import * as nats from '@nats-io/nats-core'`),
  and `StringCodec` / `JSONCodec` were dropped from NATS v3 entirely
  (see §1.2). Nest's `NatsRecordBuilder` examples were updated
  accordingly.

### 1.2 nats.js v3 splits `nats` into scoped packages

The legacy npm package `nats` (last release 2.29.3) is deprecated on the
registry:

> "Package moved. Use `@nats-io/transport-node` from
> https://github.com/nats-io/nats.js"
> — `npm view nats deprecated`, verified 2026-10-20

Per the official [nats.js migration guide](https://github.com/nats-io/nats.js/blob/main/migration.md),
v3 (all scoped packages at 3.4.0 as of this writing) splits the monolith:

| Concern | v2 (`nats@2.x`) | v3 (`@nats-io/*@3.x`) |
| --- | --- | --- |
| Connect (Node) | `connect()` from `nats` | `connect()` from `@nats-io/transport-node` |
| Core types (`Msg`, `RequestOptions`, `SubscriptionOptions`, `headers()`, `createInbox()`) | `nats` | `@nats-io/nats-core` (also re-exported by the transport package) |
| JetStream client/manager | `nc.jetstream()`, `nc.jetstreamManager()` methods | Free functions `jetstream(nc)`, `jetstreamManager(nc)` from `@nats-io/jetstream` |
| JetStream types (`JsMsg`, `PubAck`, `StreamConfig`, `JetStreamOptions`, `JetStreamPublishOptions`, Ack/Deliver/Replay policies) | `nats` | `@nats-io/jetstream` |
| KV / ObjectStore / Services | Built into `nats` | Separate `@nats-io/kv`, `@nats-io/obj`, `@nats-io/services` (not needed by this plugin today) |

Breaking API removals/changes that hit this plugin directly
([migration.md](https://github.com/nats-io/nats.js/blob/main/migration.md),
"Changes to Clients / JetStream"):

- `JSONCodec()` / `StringCodec()` **removed**. Encode with
  `JSON.stringify(...)` / `TextEncoder`; decode with `msg.json()` /
  `msg.string()`.
- `NatsConnection#jetstream()` and `#jetstreamManager()` **removed**;
  replaced by `jetstream(nc)` / `await jetstreamManager(nc)`.
- `JetStreamClient#subscribe()`, `#fetch()`, `#pull()` **removed**.
  Consuming now goes through the **Consumers API**:
  `js.consumers.get(stream, name?)` returns a `Consumer` offering
  `next()`, `fetch()`, and `consume()` (async iterators). Durable
  consumers are created/managed via `jetstreamManager(nc).consumers.add(stream, config)`
  or implicitly by `get()` for ordered consumers.
- `consumerOpts()` builder and `ConsumerOptsBuilder` **removed**.
  Consumer configuration is now a plain `ConsumerConfig` object with
  snake_case fields (`ack_policy`, `deliver_policy`, `max_deliver`,
  `filter_subject` / `filter_subjects`, `backoff`,
  `inactive_threshold`, `mem_storage`, …).
- `NatsError` **removed** in favour of typed errors (`RequestError`,
  `TimeoutError`, `JetStreamError`, `JetStreamAPIError`, …).
- Status enums `Events` / `DebugEvents` **removed**; `nc.status()`
  yields typed status objects discriminated by a string `type` field.
- `JsMsg.info.redeliveryCount` renamed to `JsMsg.info.deliveryCount`.
- Consumer notification enums replaced by `ConsumerNotification`
  (discriminated by `type`); `Consumer#status()` now returns an
  `AsyncIterable` directly instead of `Promise<AsyncIterable<…>>`.

The **wire protocol and the NATS server did not change**: a v2 client
and a v3 client talk to the same `nats-server` with JetStream enabled,
and streams created by one are visible to the other. "Support both" is
therefore a client-library problem, not a server-migration problem.

### 1.3 NestJS 12 platform changes that also reach this plugin

These are not NATS-specific, but they land in the same upgrade and the
plugin must survive them:

- **ESM-only framework packages.** All core `@nestjs/*` v12 packages
  ship as ESM. CommonJS apps (including this plugin's current
  `"module": "commonjs"` build) keep working via Node's `require(esm)`,
  which requires **Node ≥ 20.19 or ≥ 22.12** at runtime.
  ([migration guide](https://docs.nestjs.com/migration-guide))
- **Node / TypeScript floors rise.** Running a Nest 12 app needs
  Node 20.19+ / 22.12+; the v12 CLI/schematics need newer still.
  `nest upgrade` bumps TypeScript to v6.
- **Jest caveat:** Jest can only load the ESM-only v12 packages on
  Node ≥ 24.9; Nest recommends Vitest for ESM projects and bumps Jest
  to v30 otherwise. Our (currently minimal) test setup must pick a side
  per CI cell.
- **The Nest transport base classes are largely stable.** Comparing the
  published `.d.ts` of `@nestjs/microservices` 11.0.1 vs 12.0.1
  (verified via unpkg on 2026-10-20): `ClientProxy` keeps the same
  abstract surface (`connect`, `close`, `publish`, `dispatchEvent`,
  `on`, `unwrap`, `send`, `emit`), and `Server` / `BaseRpcContext`
  likewise. A single plugin build *can* therefore satisfy both Nest
  generations at the TypeScript level — the blocker is the NATS driver,
  not Nest's abstract classes.
  - One existing gap this exposes: `Server` (already in v11) declares
    abstract `on()` and `unwrap()`, and `ClientProxy` declares abstract
    `on()` / `unwrap()` — the current `NatsJetStreamServer` /
    `NatsJetStreamClientProxy` implement neither. Under modern Nest +
    strict `tsc` this must be fixed (small stubs returning the
    underlying connection) as part of any support work.

---

## 2. Current state of this repository

The publishable package is on the **legacy stack only**:

- `packages/nestjs-nats-jetstream-transport/package.json`:
  `nats: ^2.28.2` as a hard **runtime dependency**, `@nestjs/common`,
  `@nestjs/core`, `@nestjs/microservices` only as **devDependencies**
  pinned to `^8.2.0` (not declared as peers at all), TypeScript `^4.4.4`,
  CommonJS output.
- The demo apps (`nats-listener`, `nats-publisher`) are on Nest 8 +
  `nats: ^2.6.1`.

Every direct use of the legacy `nats` package (all of these break or
move under v3):

| File | Legacy usage | v3 fate |
| --- | --- | --- |
| `src/client.ts` | `connect`, `JSONCodec`, `Codec`, `NatsConnection`, `RequestOptions`; `nc.jetstream()`, `nc.request()`, `codec.encode/decode` | `connect` → `@nats-io/transport-node`; codec removed → `JSON.stringify` + `msg.json()`; `jetstream(nc)` → `@nats-io/jetstream` |
| `src/server.ts` | `connect`, `JSONCodec`, `JetStreamManager`, `SubscriptionOptions`; `nc.jetstreamManager()`, `nc.jetstream()`, `js.subscribe(subject, consumerOpts)`, `jsm.streams.list().next()`, `jsm.streams.add/update`, `msg.term()`, `nc.subscribe(subject, { callback })` | Manager/client become free functions; `js.subscribe` + `consumerOpts` removed → Consumers API (`consumers.add/get` + `consume()`) — the largest single rewrite |
| `src/utils/server-consumer-options-builder.ts` | `consumerOpts(config)` chainable builder + `createInbox()` | Builder **deleted upstream**. Replace with a pure function returning a plain `ConsumerConfig` (snake_case) for v3, while the v2 path still needs the builder |
| `src/utils/nats-jetstream-record.builder.ts` | `JetStreamPublishOptions`, `headers()` from `nats` | Types/`headers` → `@nats-io/jetstream` / `@nats-io/nats-core` |
| `src/utils/nats-request-record-builder.ts` | `RequestOptions`, `headers()` from `nats` | → `@nats-io/nats-core` |
| `src/nats-jetstream.context.ts` | `JsMsg`, `Msg` from `nats`; exposes `context.message` publicly | `JsMsg` → `@nats-io/jetstream`, `Msg` → `@nats-io/nats-core`; the public context type leaks the driver, so it changes with the driver |
| `src/interfaces/nats-connection-options.interface.ts` | `extends ConnectionOptions` from `nats`; `connectedHook(nc: NatsConnection)` | `ConnectionOptions` → `@nats-io/nats-core` / transport package; hook type leaks the driver |
| `src/interfaces/nats-jetstream-client-options.interface.ts`, `…-server-options.interface.ts` | `ConnectionOptions`, `JetStreamOptions` from `nats` | As above; `JetStreamOptions` → `@nats-io/jetstream` |
| `src/interfaces/nats-stream-config.interface.ts` | `Pick<StreamConfig, …>` from `nats` | `StreamConfig` → `@nats-io/jetstream` (field names largely unchanged) |
| `src/utils/server-consumer-options-builder.spec.ts` | Mocks `consumerOpts`/`createInbox` from `nats` | Rewrite against the new pure config-mapping function; no upstream mock needed |
| `src/index.ts` | Re-exports all of the above | Public API surface transitively re-exports driver types; a driver swap is a breaking change for consumers' types |
| Demo apps | `PubAck`, `JetStreamPublishOptions`, `headers`, `Events`, `DebugEvents`, `RetentionPolicy` from `nats` | `Events`/`DebugEvents` removed; rest move to scoped packages |

Net assessment: this is not an import-rename. Two of the three runtime
pillars of the plugin (`JSONCodec`, and `js.subscribe` + `consumerOpts`)
were **deleted** in v3 and need re-implementation on the Consumers API,
and the public typings are coupled to the driver everywhere.

---

## 3. Options considered

### Option A — Single package, internal driver adapter, both drivers as optional peers ⭐ Recommended

One published package. All NATS access goes through a small internal
`NatsDriver` interface (connect, publish/request, JetStream publish,
JetStream consume, stream admin, close). Two implementations:
`legacy` (on `nats@2`) and `modern` (on `@nats-io/*@3`). The driver is
resolved lazily at `connect()`/`listen()` time — never by a top-level
static import of both packages — and the public API exposes only
driver-neutral types defined by this plugin.

- ✅ One install, one API, one changelog for consumers; "support both"
  is answered literally — a Nest 11 app and a Nest 12 app use the same
  plugin version.
- ✅ Mirrors the framework's own pattern: `@nestjs/microservices`
  declares its NATS driver as an *optional peer* (`nats: *` in ≤11,
  `@nats-io/transport-node: *` in 12), so consumers already expect to
  install the driver themselves.
- ✅ The two drivers are different package names, so both may legally
  coexist in a consumer's `node_modules` during migration; the adapter
  picks exactly one and errors clearly if neither (or ambiguously
  both without configuration) is present.
- ✅ Forces the healthy cleanup this package needs anyway: driver types
  stop leaking into the public `.d.ts`, and the consumer-options
  builder becomes a pure, unit-testable mapping.
- ❌ Largest engineering effort of the options that keep one package:
  the server-side consume path must be re-designed (push-via-
  `js.subscribe` vs pull-via-`consumers.get().consume()` — see §4.3),
  plus duplicated driver-level integration tests.
- ❌ Runtime (not type) errors become possible: choosing/loading the
  wrong driver fails at startup, so resolution errors must be
  first-class, actionable messages (e.g. "found `nats@2` but Nest 12
  detected… install `@nats-io/transport-node`").

### Option B — Two release lines: v2.x for legacy, v3.x for modern (split majors)

Keep the current line as-is for Nest ≤11 + `nats@2`, and cut a new
major that only supports Nest 12 + `@nats-io/*@3`, maintained on
separate branches.

- ✅ Each line is simple, statically typed against one driver, no
  runtime resolution, no adapter indirection.
- ✅ This is what Nest itself did (a major per driver generation), and
  what the `nats` package's deprecation forces eventually anyway.
- ❌ "Support both" becomes "maintain two branches forever": every bug
  fix in the shared logic (stream setup, ack handling, backoff — note
  the repo's current `implement-backoff-fork` work) must be ported
  twice, and the lines will drift.
- ❌ Consumers get no single version to upgrade to; documentation and
  support must always first ask "which line are you on?".

### Option C — One package, two entry points (`/legacy`, `/modern` subpath exports)

Single package and version, but consumers import
`@kodeblox/nestjs-nats-jetstream-transport/legacy` (Nest ≤11) or
`…/modern` (Nest 12). Shared logic is factored underneath; each entry
point statically imports only its own driver.

- ✅ No lazy loading / runtime detection; bundlers and TypeScript see
  exactly one driver per entry point. Failed installs fail at
  `npm install`/import time, not at boot.
- ✅ Both drivers can stay optional peers; tree-shaking is clean.
- ❌ Two public APIs to keep in sync; consumers must know which entry
  point matches their Nest major (a new footgun), and our own module
  class (`NatsJetStreamTransport.register(...)`) exists twice or needs
  an entry-point-agnostic re-export strategy.
- ❌ Doubles the published surface that semver applies to.

### Option D — Drop legacy: Nest 12 / NATS v3 only (single major bump)

Rewrite imports to `@nats-io/*`, replace codec/consumer code, declare
`@nestjs/*: ^12` peers, done.

- ✅ Smallest, cleanest codebase; rides the upstream deprecation
  (`nats@2` will receive no fixes) and Nest's own direction.
- ❌ Explicitly does **not** support both: every Nest ≤11 user is
  stranded on an unmaintained line with a deprecated driver. Rejected
  as the primary answer, but it is the correct *fallback* if maintainer
  capacity cannot cover Option A's test matrix — in that case it should
  be chosen honestly over an untested "dual" release.

### Option E — Alias/shim tricks without an adapter (rejected)

E.g. asking consumers to alias `nats` → `@nats-io/transport-node`
(`"nats": "npm:@nats-io/transport-node@^3"`), or shipping both drivers
as hard dependencies and re-exporting whichever loads.

- ❌ Aliasing cannot work: the APIs are not name-compatible
  (`JSONCodec`, `consumerOpts`, `nc.jetstream()` are *gone*, not
  renamed), so old code throws at runtime. Hard-depending on both
  drivers also installs the deprecated `nats@2` into every Nest 12
  app — the exact outcome Nest 12 exists to remove.

### Comparison

| Option | One version for both stacks? | Static typing | Effort | Maintenance cost |
| --- | --- | --- | --- | --- |
| A. Adapter + optional peers ⭐ | Yes | Driver-neutral public types; structural internals | High (once) | One line, dual test matrix |
| B. Split release lines | No (two lines) | Fully static per line | Medium (twice, smaller) | Two branches, ported fixes |
| C. Dual entry points | Yes | Fully static per entry | Medium–high | Two public surfaces |
| D. Modern only | No | Fully static | Low | One line; abandons ≤11 |
| E. Alias/shim | Pretends to | Broken at runtime | Low | Rejected |

**Decision sought:** adopt **Option A**, with **Option C's entry-point
split as the contingency** if the adapter's consume-path abstraction
(§4.3) turns out to leak driver semantics into the public API, and
**Option B's maintenance branch** retained for the legacy line's
end-of-life period (§5).

---

## 4. Recommended design (Option A)

### 4.1 Package metadata

Move every framework/driver package out of `dependencies` into
*peers*, installable in either generation. Sketch for the plugin's
next major (`3.0.0`):

```jsonc
{
  "peerDependencies": {
    "@nestjs/common": "^10.0.0 || ^11.0.0 || ^12.0.0",
    "@nestjs/core": "^10.0.0 || ^11.0.0 || ^12.0.0",
    "@nestjs/microservices": "^10.0.0 || ^11.0.0 || ^12.0.0",
    // exactly one driver generation is required at runtime:
    "nats": "^2.28.0",
    "@nats-io/transport-node": "^3.0.0",
    "@nats-io/jetstream": "^3.0.0",
    "@nats-io/nats-core": "^3.0.0",
    "reflect-metadata": "^0.1.12 || ^0.2.0",
    "rxjs": "^7.1.0"
  },
  "peerDependenciesMeta": {
    "nats": { "optional": true },
    "@nats-io/transport-node": { "optional": true },
    "@nats-io/jetstream": { "optional": true },
    "@nats-io/nats-core": { "optional": true }
  },
  "engines": { "node": ">=18" }
}
```

Notes:

- The Nest range is deliberately **not** `^8 || …`: the repo's demos
  sit on Nest 8 today, but claiming 8–12 without testing 8/9 repeats
  the current mistake (devDeps claim `^8.2.0`, nothing verifies it).
  Support what CI tests (§6); widen later with evidence. Until CI
  proves otherwise, document Nest 10 as the legacy floor.
- `engines.node` needs a footnote, not a single number: the Nest 12
  end effectively requires Node ≥ 20.19 / ≥ 22.12 (because Nest 12 is
  ESM-only and is loaded via `require(esm)`), while the legacy end
  runs on older LTS lines. State this per-stack in the README instead
  of raising the global floor and lying to legacy users.
- Keep `@nats-io/nats-core` explicit even though
  `@nats-io/transport-node` re-exports it: our record builders need
  `headers()`, and depending on a re-export you don't own is brittle.
- For development, install **both** driver generations in
  `devDependencies` (they coexist fine as differently-named packages)
  so unit tests can exercise both adapters; integration jobs install
  the matching Nest major per cell (§6).

### 4.2 Internal driver adapter

Create `src/driver/` (internal, not exported from `src/index.ts`):

```text
src/driver/
  nats-driver.ts        # the interface + driver-neutral types
  legacy-driver.ts      # implements it on `nats` (v2)
  modern-driver.ts      # implements it on `@nats-io/*` (v3)
  resolve-driver.ts     # lazy resolution + actionable errors
```

Rules that make this work instead of leaking:

1. **No top-level static import of either driver package** outside its
   own `*-driver.ts` file, and those files are themselves only loaded
   lazily (dynamic `import()` / Nest's `loadPackage` pattern, which
   `Server` already exposes as `loadPackage`). A Nest 12 app must never
   evaluate `require('nats')`, and vice versa.
2. **The interface speaks this plugin's types only:** connection
   options as already defined by `NatsConnectionOptions`, payloads as
   `unknown`/`Uint8Array`/string, consumer settings as the existing
   `ServerConsumerOptions`. JSON encoding lives inside the client/
   server (replace `JSONCodec` with `JSON.stringify` on publish and
   `JSON.parse`/`msg.json()` on receipt — byte-identical on the wire,
   which is also what Nest 12's own transport now does).
3. **Public types stop re-exporting the driver.** `NatsJetStreamContext`
   today exposes `JsMsg` verbatim; under dual support it must expose a
   minimal structural surface (`subject`, `data`, `headers`,
   `ack()/nak()/term()/working()`, `info`) satisfied by both drivers'
   message types, with an escape hatch to reach the raw message.
   Same for `connectedHook(nc)`: type it against a minimal connection
   surface (`getServer()`, `status()`, `drain()`, `isClosed()`…).
   This is the one deliberate breaking change inside the "non-breaking"
   goal, and it belongs in the major bump (§5).
4. **Resolution errors are product UI.** If zero or both drivers are
   found, or the found driver contradicts an explicit
   `driver: 'legacy' | 'modern'` option on `NatsJetStreamClientOptions`
   / `NatsJetStreamServerOptions`, throw at startup naming the packages
   to install/uninstall. Auto-detection order: explicit option →
   `@nats-io/transport-node` present → `nats` present. (Preferring the
   modern driver when both exist matches the migration direction; the
   explicit option removes all ambiguity.)
5. **`on()` / `unwrap()`** (abstract on both Nest base classes, §1.3)
   are implemented on client and server, returning the underlying —
   driver-typed-as-`unknown` — connection, restoring parity with
   Nest's built-in transports (`client.unwrap<NatsConnection>()` is
   documented for the built-in NATS transport in exactly these terms).

### 4.3 The hard part: JetStream consuming

Publishing maps almost mechanically (`js.publish(subject, bytes, opts)`
exists in both generations; options type moves package). **Consuming
does not:**

- Legacy: `serverConsumerOptionsBuilder()` produces a `consumerOpts()`
  builder; `js.subscribe(subject, opts)` creates a (durable, push)
  consumer under the hood and yields an async-iterable subscription.
  Our server then does `msg.term()` on handler error.
- Modern: `consumerOpts` and `JetStreamClient#subscribe` are deleted.
  The supported flow is: build a plain `ConsumerConfig`, ensure the
  consumer exists (`jsm.consumers.add(stream, config)` — which first
  requires knowing the **stream name**, something the legacy flow
  never needed because push subscribe inferred it), then
  `js.consumers.get(stream, durableName)`, then iterate
  `await consumer.consume()` (or `fetch()`/`next()`).

Design consequences to settle before coding:

- `NatsJetStreamServerOptions` needs a way to name the stream per
  handler (or globally) on the modern path. Reuse `streamConfig.name`
  when present; otherwise add an optional `stream` option and fail
  loudly when neither is available. The legacy driver may ignore it.
- `serverConsumerOptionsBuilder` splits into a **pure mapping**
  `toConsumerConfig(ServerConsumerOptions, subject): ConsumerConfig`
  (snake_case fields, enum strings from `@nats-io/jetstream`'s
  `AckPolicy`/`DeliverPolicy`/`ReplayPolicy`, durable name suffixing
  logic preserved) used by the modern driver, and the existing builder
  kept inside the legacy driver. The pure function replaces the
  current mock-heavy spec with straightforward assertions — no
  `jest.mock('nats')`.
- **Push vs pull semantics must be validated, not assumed.** The
  legacy setup is a durable *push* consumer (`deliver_subject`,
  `deliver_group`); the modern default above is a *pull* consumer.
  Delivery, ordering, `deliverTo`/`deliverGroup`, flow control and
  heartbeat behaviour differ. v3 still ships `PushConsumer` types, so
  preserving push is possible, but the spec's acceptance tests (§6)
  must prove whichever semantic we choose against: durable resume
  after restart, wildcard subjects, queue-group load balancing across
  two server instances, and `manualAck` + `term()` on poison messages.
  If push semantics cannot be preserved on v3, that is a documented
  breaking change in the migration guide, not a silent drift.
- Ordered consumers (`orderedConsumer: true`) map to
  `js.consumers.get(stream)` with no name on v3 vs
  `opts.orderedConsumer()` on v2 — keep this behind the adapter.
- `JsMsg.info.redeliveryCount` (v2) vs `deliveryCount` (v3): normalize
  inside the context wrapper (§4.2.3) so user handlers see one name.

### 4.4 Build & module format

- Stay **CommonJS** for the plugin's own output: it loads under both
  Nest generations (Nest 12 apps load it directly; Nest 12's ESM core
  is consumed by *us* via Node's `require(esm)` on supported Node
  versions). Do **not** convert the plugin to ESM as part of this work.
- Raise the TypeScript devDependency to a version that understands
  both `@nestjs/*@12` types and `@nats-io/*@3` (current `^4.4.4` is
  too old); keep `skipLibCheck` posture unchanged otherwise.
- `src/index.ts` keeps exporting the same class/builder names; only
  the driver-derived *types* change (§4.2.3). Builders
  (`NatsJetStreamRecordBuilder`, `NatsRequestOptionsBuilder`) keep
  their API; `headers()` is sourced per-driver inside them.

---

## 5. Release & migration plan

1. **v2.x (current line) → maintenance branch.** Nest ≤11 + `nats@2`
   only. Critical fixes only; README gains a banner pointing Nest 12
   users to v3. This branch is Option B, time-boxed: propose support
   until the earlier of (a) 12 months after v3.0.0, or (b) Nest 11
   end-of-life — then archive it. Note honestly that its driver
   (`nats@2.29.3`) is already deprecated upstream and receives no
   fixes; that is the strongest argument for consumers to migrate.
2. **v3.0.0 (this spec).** Single package, dual driver (Option A).
   Breaking changes to declare in the changelog: `@nestjs/*` and the
   NATS driver(s) become (optional) peers — consumers must install
   them; driver types no longer leak from the public API
   (`NatsJetStreamContext.message`, `connectedHook` argument);
   Node floor per stack (§4.1). Everything else — class names,
   `register`/`registerAsync`, option field names, builder methods —
   stays source-compatible.
3. **Consumer migration guide** (in the package README, both paths):
   - *Staying on Nest ≤11:* bump plugin to v3, add `nats@^2` to your
     own dependencies (it was previously transitive via us), done.
   - *Going to Nest 12:* run Nest's `nest upgrade` (it already swaps
     `nats` → `@nats-io/transport-node` in *your* manifest), uninstall
     `nats`, install `@nats-io/transport-node @nats-io/jetstream
     @nats-io/nats-core`, bump plugin to v3. No application-code
     changes unless you touched `context.message`'s driver-specific
     fields, custom serializers (see Nest's `msg.json()` note, §1.1),
     or `Events`/`DebugEvents` enums (removed upstream).
   - Mixed fleets are safe at the **server** level: Nest 11 and Nest 12
     services can share one NATS server and one stream throughout a
     rolling upgrade (§1.2, wire compatibility).

---

## 6. Verification strategy

"Supports both" is only true where CI proves it. Minimum matrix
(GitHub Actions, `nats:2-alpine -js` service container or equivalent):

| Cell | Nest | Driver | Node | Proves |
| --- | --- | --- | --- | --- |
| legacy | `@nestjs/*@11` (plus one `@10` cell if the peer floor claims it) | `nats@^2.29` | 20 LTS | v2 regression safety |
| modern | `@nestjs/*@12` | `@nats-io/*@^3.4` | 22 LTS (≥22.12) | Nest 12 + NATS v3 |
| demos | each of `nats-listener`, `nats-publisher` rebuilt per cell | per cell | per cell | the repo's own examples stay honest |

Per cell, against a real JetStream server (mocking the driver is what
allowed the current `consumerOpts` spec to pass while saying nothing
about delivery):

- client `emit` → server `@EventPattern` round-trip, incl. headers
  (`NatsJetStreamRecordBuilder.appendHeader`) and `PubAck` contents;
- client `send` → `@MessagePattern` request/response, incl.
  `NatsRequestOptionsBuilder` timeout/reply-subject paths;
- stream auto-creation/update via `streamConfig` (one and many);
- durable consumer resume after server-process restart (same durable
  name ⇒ no redelivery of acked messages);
- poison message: handler throws ⇒ message termed, not redelivered;
- two server instances, same `deliverGroup`/queue ⇒ each event
  processed once across the pair (push/pull semantics check, §4.3);
- graceful `close()` drains without hanging either driver;
- unit tests for `toConsumerConfig` (pure mapping) and driver
  resolution errors (none installed / both installed / explicit
  override), runnable without a server.

Also: a **type-level smoke check** per cell — a fixture file using the
public API compiled with `tsc --noEmit` — so the driver-neutral `.d.ts`
claim (§4.2.3) is enforced, and a check that the published tarball of
the modern cell contains no `require('nats')` (guard against
accidental static imports creeping back).

## 7. Risks & open questions

1. **Push→pull consumer semantics (§4.3)** is the single largest
   unknown. Needs a small spike (one stream, one durable, both
   drivers, restart test) *before* the full adapter is built; if push
   cannot be preserved on v3, decide explicitly: accept pull
   (documented break) or use v3's `PushConsumer` support.
2. **Which stream does a handler consume from?** Legacy push-subscribe
   never required a stream name; modern consumer creation does.
   Decide the option shape (`stream` per server? per handler extras?)
   before freezing the v3 API.
3. **Legacy floor:** is Nest 10 a real requirement, or may v3 claim
   `^11 || ^12` and leave 8–10 to the v2.x branch? Needs a maintainer
   call; the CI matrix should not claim what it doesn't run.
4. **Simultaneous drivers installed:** during a consumer's migration
   both `nats` and `@nats-io/*` may be present (e.g. another library
   still needs `nats`). The adapter defaults to modern (§4.2.4); is a
   startup warning warranted when both are detected?
5. **Test runner on the modern cell:** Jest on Node < 24.9 cannot load
   Nest 12's ESM packages. Choose per-cell (Vitest for modern, Jest
   for legacy) or pin the modern cell to Node ≥ 24.9 — decide before
   writing CI, not during.
6. **Backoff feature in flight:** the repository's current branch
   (`implement-backoff-fork`, exponential backoff on consumer options)
   touches exactly the builder this spec replaces. Land it on the
   legacy line first, or re-express it as fields on the pure
   `ConsumerConfig` mapping — but do not let the two efforts diverge.

## 8. References

- [NestJS migration guide — NATS v3](https://docs.nestjs.com/migration-guide)
  (also: ESM packages, Node 20.19+/22.12+ floors, `nest upgrade`)
- [NestJS docs — NATS transport](https://docs.nestjs.com/microservices/nats)
  (`@nats-io/transport-node` install, `unwrap()` typed as
  `import('@nats-io/transport-node').NatsConnection`, `msg.json()`
  deserializer note)
- [nats.js migration guide](https://github.com/nats-io/nats.js/blob/main/migration.md)
  (package split, `JSONCodec`/`consumerOpts`/`js.subscribe` removals,
  free-function `jetstream(nc)` / `jetstreamManager(nc)`)
- [nats.js JetStream README — Consumers API](https://github.com/nats-io/nats.js/blob/main/jetstream/README.md)
  (`consumers.get`, `consume()` / `fetch()` / `next()`, ordered consumers)
- Registry evidence (2026-10-20): `npm view nats deprecated`
  ("Package moved. Use @nats-io/transport-node…", `nats@2.29.3`);
  `npm view @nestjs/microservices@{11,12} peerDependencies{,Meta}`
  (`nats: *` optional → `@nats-io/transport-node: *` optional);
  `@nats-io/{transport-node,jetstream,nats-core}@3.4.0`.
- Nest base-class stability: `@nestjs/microservices` 11.0.1 vs 12.0.1
  published `client/client-proxy.d.ts`, `server/server.d.ts`,
  `ctx-host/base-rpc.context.d.ts` (unpkg, 2026-10-20) — abstract
  surfaces unchanged; `Server`/`ClientProxy` require `on()`/`unwrap()`
  in both.


