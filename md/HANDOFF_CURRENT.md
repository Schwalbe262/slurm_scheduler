# Current Handoff
- 2026-07-29: Phase 3 (module-flag gating) + Phase 4a (pilot v2 harness) complete on
  `integration/aedt-consolidated-20260728`; canonical working copy is the local clone
  `C:\Users\peets\work\slurm_scheduler` (Y: RaiDrive is unstable — reference only).
- `aedt_pool.module_enabled=false` (default) now fully disables the pool: no service/
  router/thread/tables; pooled submissions 422. Regression: tests/test_aedt_pool_module_flag.py.
- Pilot loopback failures were harness drift vs v2 attestation/protocol gates — fixed in the
  two pilot test files + salvaged script updates; production modules untouched.
- Validation: focused suite 303 passed; full suite green expected (run before commit).
- Remaining: Phase 4b live 1:2 pilot (needs cluster SSH + license server + repo SHAs from
  user; runbook draft in session scratchpad), then Phase 5 finalize (README/docs, main-merge
  decision). Pool stays enabled=0/adapter_ready=0 until the live gate passes.
