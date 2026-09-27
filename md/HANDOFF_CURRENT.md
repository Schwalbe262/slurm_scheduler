# Current Handoff
- 2026-09-27 operability pass is on `improve/operability-20260927` from
  `01326ab`: task form uses the API's project/dedupe expansion, stale inventory
  is visible, and Slurm-confirmed missing allocation jobs are closed without
  cancelling any unrelated job. AEDT 1:N configuration is gated by evidence
  for the exact project count; offline 3/4-project host isolation tests pass.
  The production pool remains disabled until real AEDT/license validation.
- Current live port 8002 uses a separate pinned deployment (`724d38d`) and
  database. Do not replace it with this branch without a schema/config
  migration and rollback trial on a copy of that database. Port 8000 has no
  local 8001 target listener, and starting its old DB would run a second
  scheduler against shared accounts.
- The six FEA commits unique to `main` by hash were already ported into this
  integration line as `1ed9d42`, `0e30fb6`, `11d6b9a`, `66b1e02`,
  `029336a`, and `9274a39`; do not cherry-pick duplicate behavior.
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
