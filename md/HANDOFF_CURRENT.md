# Current Handoff
- 2026-07-28: MFT optimal-design campaign features removed from the generic scheduler.
- Branch remains `integration/aedt-consolidated-20260728`; no commit or push performed.
- Deleted pipeline status, campaign mutation lock, their tests, and the repair script.
- Removed campaign/simulation-policy APIs, persistence, dashboard controls, and pooled MFT contract.
- Kept generic AEDT pool/attach, control-plane relay, and `fea_bursty`; pool labels are neutralized.
- Added `aedt_pool.terminal_workspace_root` with `/gpfs/tmp_cpu2/aedt_pool` default.
- Compileall clean; pytest: 880 passed, 3 known pilot failures, 2 skipped.
- Required source/template/example grep is clean; `git diff --check` is clean.
- Next: orchestrator review and commit.
