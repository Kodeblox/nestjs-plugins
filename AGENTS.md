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
