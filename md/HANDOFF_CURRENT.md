# Current Handoff

- 2026-09-27 generic scheduler main is `880c6df13ac526b588172242aa3311055433d5df`.
  It is pushed to GitHub `main` and fast-forwarded in `Y:\git\slurm_scheduler`.
  The canonical development clone is `C:\Users\peets\work\slurm_scheduler`.
- Full local test run on that main: **893 passed, 2 skipped**, no failures,
  `python -m pytest -q --disable-warnings -x` (375.79 s). Generic AEDT
  loopback tests also passed separately (11 tests). The live AEDT 1:N
  output/runtime/license gate remains open; the pool stays disabled.
- Scheduler fixes in main: web task form uses the same project/dedupe expansion
  as the API, stale inventory is visible, Slurm-confirmed missing allocation
  jobs close without cancelling another job, and AEDT project-density evidence
  is specific to the requested N (2–8). AEDT family placement, canary input,
  and native solve order no longer infer workload from project names. Native
  solves remain serial until per-family live parallel evidence exists.
- MFT-specific pilot runners and stale campaign API/config documentation were
  removed. General usage is documented in `docs/USAGE_ko.md`; AEDT validation
  and rollback are in `docs/aedt_pool_runbook.md`.
- The active service on port 8002 is a separate pinned deployment (`724d38d`)
  with an active DB and workload-specific campaign API. It was not restarted or
  changed. Current generic main is not a drop-in replacement: a copied-DB
  trial found schema and behavior differences. Port 8000 has no local 8001
  listener; starting its old DB against the same accounts would create a
  second active scheduler. Do not deploy either route without isolation and
  a compatibility/rollback trial.
- Branch/worktree history is preserved in verified Git bundles under
  `C:\Users\peets\work\slurm_scheduler_archive_20260927`. Remote GitHub has
  only `main`. The local C clone has only one worktree and branch. A separate
  Y feature worktree with local config/data was deliberately retained.
- Current remaining work: generic real-AEDT 1:N pilot and license proof;
  deployment isolation/compatibility path; review any externally rejected
  orphan clone/temp-copy cleanup. Never cancel jobs submitted by other
  projects; only test jobs created and recorded by this improvement work may
  be cancelled.
