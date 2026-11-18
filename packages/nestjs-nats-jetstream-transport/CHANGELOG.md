# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

# [3.0.0](https://github.com/Kodeblox/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@2.2.8...@kodeblox/nestjs-nats-jetstream-transport@3.0.0) (2026-11-18)

### ⚠ BREAKING CHANGES

* NATS drivers and NestJS packages are now (optional) peer dependencies. Install your generation explicitly: `nats@^2` for Nest ≤11, `@nats-io/transport-node` + `@nats-io/jetstream` + `@nats-io/nats-core` for Nest 12.
* The public API no longer leaks driver types (`NatsJetStreamContext.message`, `connectedHook` argument). See the README migration guide for the driver-surface notes (`msg.json()`, removed `Events`/`DebugEvents` enums, `StringCodec`/`JSONCodec` removal).
* `on()`/`unwrap()` are now implemented on the client and server (abstract on the Nest 11/12 base classes). `unwrap()` returns the underlying driver-typed-as-`unknown` connection; `on()` receives the driver's live connection status stream (`disconnect` / `reconnect` / `update`), forwarded the same way on both generations.
* The Node floor is per stack (legacy ≥18, modern ≥20.19 or ≥22.12), not global. Nest ≤10 is not claimed by v3 — stay on the `v2.x` maintenance line.

### Features

* Dual NestJS 11/12 + NATS v2/v3 support from a single codebase via the internal driver adapter (lazy `legacy`/`modern` resolution, pure `toConsumerConfig()` mapping, driver-neutral public types).
* Dual CI matrix (`.github/workflows/ci-dual-matrix.yaml`): legacy cell (Nest 11 + `nats` v2, Node 20) and modern cell (Nest 12 + `@nats-io/*` v3, Node 24) each run the full unit suite, both integration specs against a real JetStream server (no skips), the static gates (`check:no-static-nats`, `check:decoupled-dts`, `tsc`), and — modern cell — the published-tarball `require('nats')` check.

Lerna manages versions independently in this repo (`lerna.json`: `"version": "independent"`), so this bump is the direct `package.json` edit; no central version file exists.

## [2.2.8](https://github.com/Kodeblox/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@2.2.7...@kodeblox/nestjs-nats-jetstream-transport@2.2.8) (2026-09-20)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.4.5-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.4.4...@kodeblox/nestjs-nats-jetstream-transport@1.4.5-pre.0) (2023-07-13)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.3.13-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.3.12...@kodeblox/nestjs-nats-jetstream-transport@1.3.13-pre.0) (2023-03-09)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.3.5](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.3.4...@kodeblox/nestjs-nats-jetstream-transport@1.3.5) (2022-08-22)


### Bug Fixes

* TypeError when this.options.connectionOptions.connectedHook not set ([d1dbd5d](https://github.com/Redningsselskapet/nestjs-plugins/commit/d1dbd5d9f060b91ce822ba093503f0a103a8d3fd))





## [1.3.4](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.3.3...@kodeblox/nestjs-nats-jetstream-transport@1.3.4) (2022-08-19)


### Bug Fixes

* :bug: Property 'replaceAll' does not exist on type 'string'. [#31](https://github.com/Redningsselskapet/nestjs-plugins/issues/31) ([a6fad5d](https://github.com/Redningsselskapet/nestjs-plugins/commit/a6fad5d498c0869f4b75c6fb38a686c67c69da3e))





## [1.3.3](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.3.3-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.3.3) (2022-08-19)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.3.3-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.3.2...@kodeblox/nestjs-nats-jetstream-transport@1.3.3-pre.0) (2022-08-19)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





# [1.3.0-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.2.0...@kodeblox/nestjs-nats-jetstream-transport@1.3.0-pre.0) (2022-03-25)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.7](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.6...@kodeblox/nestjs-nats-jetstream-transport@1.1.7) (2021-12-06)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.6](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.6-pre.2...@kodeblox/nestjs-nats-jetstream-transport@1.1.6) (2021-12-06)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.6-pre.2](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.6-pre.1...@kodeblox/nestjs-nats-jetstream-transport@1.1.6-pre.2) (2021-12-06)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.6-pre.1](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.6-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.1.6-pre.1) (2021-12-06)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.6-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.4-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.1.6-pre.0) (2021-12-05)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.5](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.4...@kodeblox/nestjs-nats-jetstream-transport@1.1.5) (2021-12-05)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.4](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.4-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.1.4) (2021-12-04)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.4-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.3-pre.2...@kodeblox/nestjs-nats-jetstream-transport@1.1.4-pre.0) (2021-12-04)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.3](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.3-pre.2...@kodeblox/nestjs-nats-jetstream-transport@1.1.3) (2021-12-02)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.3-pre.2](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.3-pre.1...@kodeblox/nestjs-nats-jetstream-transport@1.1.3-pre.2) (2021-12-02)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.3-pre.1](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.3-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.1.3-pre.1) (2021-12-01)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.3-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.2-pre.1...@kodeblox/nestjs-nats-jetstream-transport@1.1.3-pre.0) (2021-12-01)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.2](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.2-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.1.2) (2021-11-30)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.1.2-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.0-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.1.2-pre.0) (2021-11-30)


### Bug Fixes

* removed console.log ([d60a71c](https://github.com/Redningsselskapet/nestjs-plugins/commit/d60a71c5535b46c8a63983a2063ac24113351eff))





## [1.1.1](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.0...@kodeblox/nestjs-nats-jetstream-transport@1.1.1) (2021-11-30)


### Bug Fixes

* removed console.log ([d60a71c](https://github.com/Redningsselskapet/nestjs-plugins/commit/d60a71c5535b46c8a63983a2063ac24113351eff))





# [1.1.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.1.0-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.1.0) (2021-11-29)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





# [1.1.0-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.12-pre.1...@kodeblox/nestjs-nats-jetstream-transport@1.1.0-pre.0) (2021-11-29)


### Features

* better config ([#11](https://github.com/Redningsselskapet/nestjs-plugins/issues/11)) ([c1a363a](https://github.com/Redningsselskapet/nestjs-plugins/commit/c1a363abb99ba513338da438704f6356813fed4b))





## [1.0.12-pre.1](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.12-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.0.12-pre.1) (2021-11-29)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.12-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.11-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.0.12-pre.0) (2021-11-29)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.11](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.11-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.0.11) (2021-11-25)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.11-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.10-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.0.11-pre.0) (2021-11-25)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.10](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.10-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.0.10) (2021-11-25)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.10-pre.0](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.6-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.0.10-pre.0) (2021-11-24)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.9](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.8...@kodeblox/nestjs-nats-jetstream-transport@1.0.9) (2021-11-24)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.8](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.7...@kodeblox/nestjs-nats-jetstream-transport@1.0.8) (2021-11-24)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.7](https://github.com/Redningsselskapet/nestjs-plugins/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.6...@kodeblox/nestjs-nats-jetstream-transport@1.0.7) (2021-11-24)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## [1.0.6](https://github.com/Redningsselskapet/nestjs-nats-jetstream-transport/compare/@kodeblox/nestjs-nats-jetstream-transport@1.0.6-pre.0...@kodeblox/nestjs-nats-jetstream-transport@1.0.6) (2021-11-24)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport





## 1.0.6-pre.0 (2021-11-24)

**Note:** Version bump only for package @kodeblox/nestjs-nats-jetstream-transport
