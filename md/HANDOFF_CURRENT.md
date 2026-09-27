# Current Handoff

- 2026-09-27: The canonical development clone is
  `C:\Users\peets\work\slurm_scheduler`. Generic changes belong on `main`;
  `Y:\git\slurm_scheduler` is the RaiDrive clone. GitHub has only remote `main`.
- A full local run after the observer and live-pilot harness merge passed
  **902 tests, with 2 skipped** (`python -m pytest -q --disable-warnings -x`,
  288.71 s). The last test-fixture wording cleanup is verified separately.
- The web task form and API share project/entrypoint/argument and dedupe
  expansion. Inventory staleness is visible. A missing Slurm allocation is
  confirmed through Slurm and closed locally without `scancel`. The default
  scheduler remains standalone.
- The optional AEDT pool is disabled by default. Density N=2..8 is gated by
  live evidence for that exact N, including output identity and license
  checkouts. Project names no longer imply workload family, placement, or
  native solve order. Native solves stay serial until separate live parallel
  evidence exists. Generic loopback tests pass, but no real 1:N AEDT run has
  passed yet.
- `scripts/aedt_pool_live_pilot.py` and `docs/aedt_pool_live_pilot.md` provide
  an independent Maxwell 3D baseline-versus-pooled A/B workload, verifier,
  and fault checks. This is preparation for a real trial, not proof of one.
  The verifier's four focused tests pass. Cluster login and six Slurm accounts
  are configured under the local/Y runtime `accounts.yaml`; the license
  monitor uses `r1jae262` and `lmutil` on the cluster. A live account-status
  query found idle accounts, but all six are also registered with the active
  8002 service. An idle account is not a reserved staging allocation. A
  separate scheduler DB, pilot-owned host allocation/session/task IDs,
  bootstrap token, and raw license-server samples are still needed. Pool
  activation remains blocked until the live runbook gates pass. The current
  `set_enabled` gate requires prior passing validation, so an isolated live
  pilot needs an explicit, bounded bootstrap path before starting its host;
  do not insert fabricated validation evidence into an operating database.
- Read-only `observer_mode` can inspect a **copy** of the active SQLite DB on
  a separate port without starting the scheduler, pool, relay, or maintenance
  services. It rejects HTTP mutations. See `docs/CONFIG.md` and
  `docs/USAGE_ko.md`.
- MFT-specific pilot runners and stale campaign API/config documentation were
  removed from generic main. General usage is in `docs/USAGE_ko.md`; AEDT
  validation and rollback are in `docs/aedt_pool_runbook.md`.
- The active 8002 service is a separate pinned deployment (`724d38d`) with an
  active DB and workload-specific campaign API. It was not restarted or
  changed. Generic main is not a drop-in replacement: a copied-DB trial found
  schema and behavior differences. Port 8000 has no local 8001 listener;
  starting its old DB against the same accounts would create a second active
  scheduler. An isolated compatibility/rollback trial is required before any
  deployment.
- Branch/worktree history is preserved in verified Git bundles under
  `C:\Users\peets\work\slurm_scheduler_archive_20260927`. The separate
  `C:\Users\peets\NEC\pe_worktree` has local config/data and was retained.
  Two orphan standalone clones and temporary DB copies also remain after
  automatic approval review rejected their removal.
- Never cancel jobs submitted by other projects. Only test jobs created and
  recorded by this scheduler-improvement work may be cancelled. No Slurm jobs
  were submitted or cancelled in this change set.
