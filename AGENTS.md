# AGENTS.md — how work lands in this repo

Code reaches `main` **only via pull requests from feature branches**.
Never commit directly to `main`, and never push to it — not for
implementations, not for test-only changes, not for docs.

- One branch per unit of review, named `v3-phase-N-<slug>` (or
  `feat-<slug>` / `fix-<slug>` outside the v3 effort). Branch off an
  up-to-date `main`; rebase onto `main` if it moves, resolving conflicts
  in the branch.
- Open a PR against `main` on the Kodeblox/nestjs-plugins fork for every
  branch, linking the tracking issue (`Closes #N`). PRs are where review
  happens; a slice is done only when its PR is merged.
- Fleet lanes: do not use scratch mode here. Spawn implementing lanes with
  `--worktree` (branch `lane/<name>`) and land via `herd land`, or push the
  branch and open a PR per the project docs. The orchestrator verifies the
  diff and the test suite before any PR is opened.
- Never touch the upstream Redningsselskapet/nestjs-plugins repo — no
  pushes, PRs, issues, or milestones there. All tracking and code lives on
  this fork (`origin`).

## Lane watch discipline — panes are truth, watches are hints

A `herd watch` misses when the lane finishes in the gap (between send and
watch-attach, or an earlier watch already consumed the token) and then waits
on a token that will never come — while the finished report sits visible in
the pane. Never trust a single long watch; corroborate every timeout.

- After `herd send`, read the pane tail first (`herdr pane read`): report +
  token already there means consume them directly, no watch needed.
- Watch only genuinely pending work, with short timeouts (300–600s). On
  timeout, read the pane before anything else: report present means done,
  movement means re-arm, idle with no movement means refocus or resend —
  never blind re-arm.
- `herd status` is a second opinion, never the verdict: lanes have shown
  `working`/`idle` with complete reports waiting in their panes.
