# Tech Specs

Research and design specifications for non-trivial changes to this repository.
Each spec is a decision record: it states the problem, the verified facts,
the options considered, and a recommendation. It is not shipped code.

| # | Spec | Status | Summary |
| --- | --- | --- | --- |
| 001 | [Supporting NestJS 12 / NATS v3 alongside NestJS ≤11 / NATS v2](001-dual-nestjs-nats-support.md) | Proposed | How `@kodeblox/nestjs-nats-jetstream-transport` can serve both the legacy `nats` (v2) ecosystem and the new `@nats-io/*` (v3) ecosystem after NestJS 12 |
| 002 | [NATS startup policy for nestjs-nats-jetstream-transport](002-nats-startup-policy.md) | Proposed | Library-side spec handed over from the consuming app: route all `listen()` startup failures to `callback(err)` (Part A) plus a configurable `fail-fast` / `retry` / `retry-forever` startup policy with error classifier (Part B) |

## Conventions

- One file per spec, named `NNN-short-title.md`, numbered in creation order.
- Every external claim should link to its primary source (official docs,
  migration guide, or registry metadata) so the spec can be re-verified later.
- A spec starts as `Proposed`. It becomes `Accepted` when the maintainers
  agree to implement it, and `Superseded` when a later spec replaces it.
