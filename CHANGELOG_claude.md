## 2026-07-15 (Claude) - Reap dead-host unhealthy AEDT sessions (pool capacity deadlock fix)
- Root cause of the overnight zero-capacity stall: close_session requires the
  live host token, so sessions whose host died (usage-error exits, scancel)
  stayed 'unhealthy' forever. Their counted claim pinned recycle-drained
  allocations open, the pinned allocations occupied the node ledger, and
  open_allocation_record returned None silently on every tick - no new pool
  allocation was created for ~11 hours (last: 8932 @ 07-14 17:20) while the
  plan kept computing node_requests=10.
- reconcile() now reaps unhealthy sessions silent beyond
  max(900s, 5x session_heartbeat_timeout): session -> 'failed', its stuck
  leased/active/releasing leases -> 'failed'. Recycle-drained allocations then
  close via the existing empty-allocation janitor and the ledger frees.
- Recovery windows are preserved: a live host heartbeats every ~30s and the
  unhealthy->recovered registration path stays usable for 15+ minutes.
- Test: test_reaps_stale_unhealthy_session_and_frees_its_allocation_claim
  (pool suites 64 passed).

## 2026-07-14 (Claude) - Bug note: task cancel leaves pooled runner processes alive
- Observed during the pooled ramp: POST /api/tasks/{id}/cancel marked 27 MFT
  pooled client tasks cancelled, but their remote runner processes survived on
  compute nodes and kept requesting/holding AEDT pool leases (6 leased +
  10 queued minutes after cancellation; leases only cleared via heartbeat
  expiry). Same class as the 2026-07-09 packed-cancel child-leak, now in the
  srun-step/pooled-runner form. Zombies self-terminate at the next solver
  gate, so impact today was wasted session slots only — but a mass cancel at
  500-task scale would strand hundreds of leases. Needs: cancel path must
  verify the srun step/process tree is gone (scancel step + remote pkill of
  the task's process group), and the pool should cancel a task's leases when
  its task row goes terminal.

## 2026-07-14 (Codex) - Remove node-local AEDT canary
- Recorded the operator decision to unify pooled AEDT work on the central pool
  and removed the scheduler-managed node-local host/client admission exception.
- Removed task-backed node-local session data from the AEDT API and dashboard,
  along with the retired host script, runbook, and dedicated tests.
- New pooled tasks now require the central pool operational gate. Legacy task
  placement and reconciliation remain intact so already-running tasks are not
  cancelled or disrupted.

## 2026-07-14 (Claude) - Live deploy: relay enabled + FEA baseline-first assignment
- Deployed `live/node-canary-260714` to the live service (launcher: scheduled
  task SlurmSchedulerWebNative / start_web_y.cmd, config Y:/runtime/.../app.yaml):
  merged the control-plane relay (9bf7944) into the live snapshot (7b58966)
  and enabled it (account r1jae262, port 18790) — relay state "up", published
  URL http://172.16.10.37:18790. Central AEDT pool enabled at 2 sessions /
  4 projects / min_idle 1; bootstrap token injected via launcher env, cluster
  checkout + token deployed to r1jae262 workspace (aedt_pool_pkg @ this SHA).
- FEA baseline-first assignment (2bdfd6f): allocations below their 1x
  solver-CPU baseline are filled first (lowest requested/owned ratio; flips
  to fill-first under license scarcity), `_aedt_pool_hosts` infra tasks no
  longer consume solver baseline, and baseline attaches draw from
  fea_bursty.baseline_max_attach_per_loop (64) separate from the overcommit
  cap. Fixes: 25/38 allocations idled below 1x while others ran past it.
- Test suite green after harness updates for the new contract: 424 passed
  (test_core + test_fea_baseline_assignment + test_aedt_pool +
  test_control_plane_relay).
- Ops note: the previous scheduler process tree died at 13:20 with no exit
  record (external kill, owner unknown); service was down ~18 min until
  manual recovery. The scheduled-task launcher loop is the restart path —
  re-run it via `schtasks /Run /TN SlurmSchedulerWebNative` if the tree dies.


## 2026-07-14 (Codex) - Cluster-reachable AEDT control-plane relay (disabled)
- Added an opt-in supervisor that maintains an SSH reverse tunnel through one
  configured login account and deploys a marker-owned, stdlib-only TCP relay
  listening on the cluster network. The relay exposes only `/api/aedt-pool/`
  and `/healthz` by default, publishes its node-visible URL only while an
  end-to-end probe succeeds, and restarts failed tunnel/relay generations.
- Added relay health/status APIs and made the AEDT runtime consume the
  published URL dynamically, failing closed without creating pool capacity
  while the relay is down.
- All state-changing AEDT pool routes now require the shared bootstrap token in
  addition to existing lease/host-scoped tokens. The subsystem remains fully
  disabled and side-effect-free until explicitly configured and enabled.

## 2026-07-13 (Codex) - Configurable project concurrency ceiling
- Added `project_max_active_tasks_ceiling` with a backward-compatible default
  of 300 and made project create/upsert plus project-cap PATCH validation/errors
  use that configured value.
- Documented that a higher administrative ceiling does not bypass license
  admission or the scheduler's FEA CPU/RAM pressure guards.

## 2026-07-07 (MFT 캠페인)
- GET /api/tasks 에 limit(기본 200, 최대 10000) / name_prefix 쿼리 파라미터 추가
  - 이유: 400+ 태스크 캠페인 집계·결과 회수 시 200개 페이지 제한으로 누락 발생
  - 주의: 실행 중인 서비스에는 재시작 후 반영됨 (캠페인 웨이브 사이 안전 시점에 재시작 권장)

## 2026-07-09 (Claude)
- docs/env_profile_MFT_1MW_2026v1.md: MFT 캠페인용 env_profile 추가 요청 문서 (accounts.yaml 반영 + 재시작 필요)

## 2026-07-09 (Claude) - 추가
- docs/bug_packed_cancel_leaks_children.md: 태스크 취소가 AEDT 자식 트리를 회수하지 못하는 버그 리포트 (노드당 세션 44-86개 잔존 실측)

## 2026-07-09 (Claude) - 추가 2
- docs/design_projects_runs_items.md: Project/Run/Work-Item 3층 구조 설계 제안 (RL 대응 포함)

## 2026-07-09 (Claude) - 추가 3
- slurm.py shell_path: $HOME/ 프리픽스 경로가 통째로 quote되어 project run의 cd가 실패하던 버그 수정 (재시작 필요)

## 2026-07-10 (Codex) - MFT shared cleanup repair
- `scripts/repair_mft_cleanup_globs.py`: scheduler tick 사이의 휴지 구간에서 온라인 DB 백업과
  무결성 검사를 수행한 뒤, MFT active task/project의 공유 `simulation` cleanup glob을
  `*.aedtresults`로 원자 교체하는 guarded maintenance 도구 추가.
- 2026-07-10 05:48 KST 적용: active destructive rows 325 -> 0. 백업은
  `data/backups/manual-pre-cleanup-glob-20260710-054839.db`.

## 2026-07-10 (Codex) - MFT jji0930 capability repair
- Git에서 제외되는 운영 `config/accounts.yaml`의 `jji0930` 계정에 기존
  `pyaedt2026v1` 환경과 일치하는 `conda:pyaedt2026v1` capability를 추가.
- 설정 파싱과 scheduler 테스트 255개를 통과한 뒤 native worker만 재시작했다.
  기존 비-b630 태스크 16개와 allocation 4개는 보존됐고, placement dry-run에서
  `jji0930`이 eligible로 확인되어 대기 중이던 태스크 6개가 새 64 CPU pool에 연결됐다.
## 2026-07-13 (Codex) - Experimental AEDT session pool (disabled)
- Added an opt-in, validation-gated AEDT session/lease control plane, node-side
  session host, project attach client, API, and WEB UI ceiling control.
- Staged 250 AEDT / 500 derived project capacity without enabling or deploying
  it. Existing standalone tasks and the active MFT campaign remain unchanged.
- Added session-wide StopSimulations quarantine/sibling-grace policy and made a
  deliberate timeout fault test with sibling completion mandatory for enablement.
- See `docs/aedt_pool.md` and `docs/aedt_pool_runbook.md`.
- Added a parent TCP-listener watchdog for the 03:16 WinError 64 incident; it
  detects a wedged Uvicorn child without cancelling Slurm allocations. See
  `docs/incident_web_listener_winerror64.md`.

## 2026-07-13 (Codex) - Single scheduler restart authority
- The Python listener monitor now exits with its only worker instead of
  self-spawning replacement scheduler generations; `start_web.cmd` or the
  platform service supervisor is the sole restart authority.
- Python rejects an already-live configured listener and atomically reserves
  the Windows endpoint before application/DB startup; the Windows launcher
  also waits while `0.0.0.0:8000` is owned.
- The scheduler watchdog no longer re-enters SQLite while escalating a stall,
  so its terminal path reaches `os._exit` even when the database is wedged.

## 2026-07-13 (Codex) - Configurable SQLite journal mode
- Added `sqlite_journal_mode` with a backward-compatible `wal` default and
  documented `delete` for databases stored on network filesystems.
- Database cleanup checkpoints only in WAL mode, so non-WAL deployments do not
  reconnect solely to issue a WAL-specific pragma.

## 2026-07-13 (Codex) - Watchdog SQLite context
- Scheduler watchdog stack dumps now include the active database path and
  normalized SQLite journal mode on their first line.

## 2026-07-13 (Codex) - Node-local AEDT host-owned placement
- Added durable `requested_allocation_id` task placement so a node-local AEDT
  canary host can reserve an exact live allocation without depending on a
  short-lived `same_node_as_task_id` sibling.
- The host remains an active allocation owner until explicit cancel/drain or
  its own process exit; clients continue to co-locate through the host task ID.

## 2026-07-13 (Codex) - AEDT pool warm spare
- Added durable `min_idle_aedt_sessions` scaling (default `0`) so operators can
  keep ready, unleased AEDT sessions available without exceeding the existing
  session ceiling.
- Warm-spare starts are checked against fresh license-admission headroom before
  session rows or nodes are created; current idle count and any skip reason are
  exposed by the pool API and UI.

## 2026-07-12 (Claude)
- docs/bug_idle_active_allocation_leak.md: 태스크 0개 active 할당(6757, 63h)이 유휴 회수에서 누락되는 버그 리포트
