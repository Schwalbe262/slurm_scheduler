# AEDT 브랜치 전수 서베이 (2026-07-28)

> 결론: base head = `fix/attached-task-cpu-contract-260725` @ `1559a7c`.
> 두 후보(codex/aedt-integration-260717 vs 260725 라인)는 3de4379 이후 분기했고,
> integration-260717의 2개 커밋은 260725 라인에 patch-equivalent로 포함된다.
> aedt_pool.py는 260725 쪽이 +1,945/-135 우세(스토리지 인지 라우팅, quota/drain 안전장치,
> pending 재계획, strict-node 지원). 나머지 AEDT 모듈 3개는 두 라인이 동일.
>
> 통합 브랜치 `integration/aedt-consolidated-20260728` = base + cherry-pick 5건:
> `61b95c7`(client outage resilience), `6c3df6d`(task refresh starvation; base의 비동기
> 취소 경로에 맞춰 합성), `64d0f05`+`d30acf0`(8002 listener 복구), `4facdfe`(strict FEA
> demand pool). `35f5fdb`는 64d0f05와 동일 patch라 제외.
>
> MFT 캠페인 전용 코드는 base에 포함되어 있으며(Phase 2에서 제거):
> mft_pipeline_status.py, campaign_mutation_lock.py, /api/mft-pipeline, campaign-demand,
> simulation-policy, MFT_AEDT_* contract, dashboard MFT 패널. control_plane_relay.py는
> 범용이므로 유지.

---

# Survey Part A - AEDT-named branches

Generated using read-only Git history/diff commands. Counts under "patch-unique" exclude merge commits and omit patch-equivalent commits already present in the candidate head.

## `experiment/aedt-attach-1to2-20260713`

- Last commit date: **2026-07-13**; ahead of `main`: **34 commits**.
- Intent (inferred from top subjects): 620a301 fix: retain normal pilot process finalization; e5e81b3 fix: bound shared pilot process shutdown; 742c0fc fix: validate actual MFT leakage result field; bd3cc0d feat: add isolated shared AEDT 1:2 pilot
- Diff total (`main...branch`):  43 files changed, 11029 insertions(+), 300 deletions(-)
- Notable files: docs/aedt_pool.md, docs/aedt_pool_runbook.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, scripts/aedt_pool_fault_injection.py, slurm_scheduler/__main__.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/aedt_pool.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `experiment/aedt-attach-timeout-260713`

- Last commit date: **2026-07-13**; ahead of `main`: **35 commits**.
- Intent (inferred from top subjects): f3a65ee test: gate shared AEDT active-solve timeout; 620a301 fix: retain normal pilot process finalization; e5e81b3 fix: bound shared pilot process shutdown; 742c0fc fix: validate actual MFT leakage result field
- Diff total (`main...branch`):  44 files changed, 11357 insertions(+), 300 deletions(-)
- Notable files: docs/aedt_pool.md, docs/aedt_pool_runbook.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, scripts/aedt_pool_fault_injection.py, slurm_scheduler/__main__.py, slurm_scheduler/aedt_attach_client.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `experiment/aedt-host-lifetime-260713`

- Last commit date: **2026-07-14**; ahead of `main`: **50 commits**.
- Intent (inferred from top subjects): f3efd3e feat: make task_refresh_max_per_tick configurable; d066d7a feat: configure project task concurrency ceiling; e4718e4 fix: log SQLite context in watchdog dumps; 574b5ae fix: configure SQLite journal mode
- Diff total (`main...branch`):  54 files changed, 13263 insertions(+), 496 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_node_local_canary.md, docs/aedt_pool.md, docs/aedt_pool_runbook.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, scripts/aedt_pool_fault_injection.py
- Patch-unique vs integration-260717: **1**; vs 260725 line: **1**.
- Top unique vs integration-260717: f3efd3e feat: make task_refresh_max_per_tick configurable
- Top unique vs 260725 line: f3efd3e feat: make task_refresh_max_per_tick configurable
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/slurm.py, tests/test_core.py. Exclude these portions from consolidation.
- Spot-check interpretation: The unmatched f3efd3e patch is semantically carried by newer 8f5e7ac (`task_refresh_max_per_tick`) in both candidate heads; do not cherry-pick the older patch.
- Verdict: **SUPERSEDED - semantic successor 8f5e7ac is already in the 260725 line.**

## `experiment/aedt-node-canary-260713`

- Last commit date: **2026-07-13**; ahead of `main`: **44 commits**.
- Intent (inferred from top subjects): b5f8e72 document pooled attach canary handoff; bf273f4 fix task payload bootstrap on python3-only nodes; f22e3b1 fix: refresh licenses before fallible allocation stages; d66cf54 feat: admit bounded node-local pooled canaries
- Diff total (`main...branch`):  52 files changed, 12182 insertions(+), 385 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_node_local_canary.md, docs/aedt_pool.md, docs/aedt_pool_handoff_20260713.md, docs/aedt_pool_runbook.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **1**; vs 260725 line: **1**.
- Top unique vs integration-260717: b5f8e72 document pooled attach canary handoff
- Top unique vs 260725 line: b5f8e72 document pooled attach canary handoff
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/slurm.py, tests/test_core.py. Exclude these portions from consolidation.
- Spot-check interpretation: The sole unmatched commit b5f8e72 is a dated canary handoff document only, with no runtime fix to consolidate.
- Verdict: **SUPERSEDED - skip obsolete handoff-only documentation.**

## `feature/aedt-backend-selector-20260713`

- Last commit date: **2026-07-13**; ahead of `main`: **37 commits**.
- Intent (inferred from top subjects): 9f7b005 test: verify AEDT backend API and UI contract; bdbc6cc feat: add orthogonal AEDT task backend selector; f3a65ee test: gate shared AEDT active-solve timeout; 620a301 fix: retain normal pilot process finalization
- Diff total (`main...branch`):  47 files changed, 11702 insertions(+), 394 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_runbook.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, scripts/aedt_pool_fault_injection.py, slurm_scheduler/__main__.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/slurm.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `fix/aedt-exact-session-pin-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **119 commits**.
- Intent (inferred from top subjects): 95bb716 Fail exact AEDT cohorts when target sessions die; 61fe195 feat: reserve exact AEDT sessions for task cohorts; 5e9d7ac Add verified dead AEDT session reaper; 0c568dd Avoid backup worker churn between intervals
- Diff total (`main...branch`):  60 files changed, 33168 insertions(+), 1152 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `release/aedt-exact-pin-260716`

- Last commit date: **2026-07-17**; ahead of `main`: **138 commits**.
- Intent (inferred from top subjects): 720a3da Keep exact AEDT cohorts alive until deadline; efb507d Stabilize exact-session pooled AEDT scheduling; 592494b Allow pooled AEDT cohorts to fill for two hours; d7f4e1b Keep AEDT pool allocations off GPU nodes
- Diff total (`main...branch`):  65 files changed, 41269 insertions(+), 1144 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **2**; vs 260725 line: **2**.
- Top unique vs integration-260717: 720a3da Keep exact AEDT cohorts alive until deadline; efb507d Stabilize exact-session pooled AEDT scheduling
- Top unique vs 260725 line: 720a3da Keep exact AEDT cohorts alive until deadline; efb507d Stabilize exact-session pooled AEDT scheduling
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Spot-check interpretation: efb507d is a very large alternate stabilization squash and 720a3da is its stacked deadline fix. The 260725 line carries later/reworked exact-session routing and deadline protection, including 31711d5 and e5d10b6; wholesale cherry-picks would replay older scheduler/pool code and campaign-adjacent app/test changes.
- Verdict: **SUPERSEDED - retain the newer 260725 exact-session/deadline implementation; no wholesale cherry-pick.**

## `fix/aedt-solve-permit-7200-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **136 commits**.
- Intent (inferred from top subjects): 90e8366 Allow pooled AEDT cohorts to fill for two hours; d7f4e1b Keep AEDT pool allocations off GPU nodes; e8f14c5 Allow AEDT pools on 48-core nodes; 7c99c7c Reduce SQLite writer contention during AEDT ramp
- Diff total (`main...branch`):  65 files changed, 36680 insertions(+), 1136 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `fix/aedt-lock-hold-watchdog-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **118 commits**.
- Intent (inferred from top subjects): d7eb5e0 Harden pooled AEDT automation lock holds; 5e9d7ac Add verified dead AEDT session reaper; 0c568dd Avoid backup worker churn between intervals; cf7de6b Keep backup cadence scans off scheduler ticks
- Diff total (`main...branch`):  60 files changed, 32903 insertions(+), 1152 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **1**; vs 260725 line: **1**.
- Top unique vs integration-260717: d7eb5e0 Harden pooled AEDT automation lock holds
- Top unique vs 260725 line: d7eb5e0 Harden pooled AEDT automation lock holds
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Spot-check interpretation: d7eb5e0 is not patch-identical, but the candidate heads contain the later cross-GPFS enforcement 9ca42f6. Treat d7eb5e0 as the older lock-hardening implementation.
- Verdict: **SUPERSEDED - semantic successor 9ca42f6 is already in the 260725 line.**

## `fix/aedt-lease-cpu-accounting-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **136 commits**.
- Intent (inferred from top subjects): 2108a74 Stabilize pooled AEDT pre-admission and scaling; a45a1ad Keep AEDT pool allocations off GPU nodes; 47ac015 Allow AEDT pools on 48-core nodes; 22acf16 Reduce SQLite writer contention during AEDT ramp
- Diff total (`main...branch`):  65 files changed, 39408 insertions(+), 1137 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `fix/aedt-draining-allocation-idle-exclusion-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **122 commits**.
- Intent (inferred from top subjects): 0c36618 Replace idle AEDT sessions on draining allocations; 9150e7f Reconcile exact-pin and barrier route authorities; 76c6e98 Gate AEDT postprocess on sealed cohort completion; 3dc3624 Fail exact AEDT cohorts when target sessions die
- Diff total (`main...branch`):  60 files changed, 33798 insertions(+), 1152 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `codex/aedt-integration-260717`

- Last commit date: **2026-07-17**; ahead of `main`: **158 commits**.
- Intent (inferred from top subjects): 09ce0d9 Cap AEDT pool allocations to exact session demand; 19f855a Retire excess pending AEDT pool allocations; 3de4379 Cap standalone AEDT workers per project; 0ca941f Clarify allocation versus Slurm node state
- Diff total (`main...branch`):  66 files changed, 44984 insertions(+), 1370 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `fix/aedt-exact-pool-rollout-260717`

- Last commit date: **2026-07-17**; ahead of `main`: **158 commits**.
- Intent (inferred from top subjects): cea0052 Reserve independent standalone AEDT lanes; e16b788 Cap colocated FEA allocations by physical node resources; 3de4379 Cap standalone AEDT workers per project; 0ca941f Clarify allocation versus Slurm node state
- Diff total (`main...branch`):  66 files changed, 45297 insertions(+), 1339 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **2**; vs 260725 line: **0**.
- Top unique vs integration-260717: cea0052 Reserve independent standalone AEDT lanes; e16b788 Cap colocated FEA allocations by physical node resources
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in the 260725 line.**

## `fix/aedt-quota-burst-admission-260717`

- Last commit date: **2026-07-17**; ahead of `main`: **152 commits**.
- Intent (inferred from top subjects): 4043e61 Cache advisory AEDT storage ledgers; ccd9f71 Guard AEDT quota bursts with shadow reservations; f4bc16d Exclude nonactive AEDT sessions from capacity; fe94476 Preserve pending AEDT pool allocations
- Diff total (`main...branch`):  65 files changed, 43022 insertions(+), 1298 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **1**; vs 260725 line: **1**.
- Top unique vs integration-260717: ccd9f71 Guard AEDT quota bursts with shadow reservations
- Top unique vs 260725 line: ccd9f71 Guard AEDT quota bursts with shadow reservations
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Spot-check interpretation: ccd9f71 is not patch-identical to candidate history, but newer a564a1f has the same quota-shadow-reservation intent in both candidate heads.
- Verdict: **SUPERSEDED - semantic successor a564a1f is already in the 260725 line.**

## `fix/standalone-aedt-project-cap-260717`

- Last commit date: **2026-07-17**; ahead of `main`: **155 commits**.
- Intent (inferred from top subjects): 2e01c08 Cap standalone AEDT workers per project; b925a98 Protect pooled AEDT hosts from pressure requeue; 4092c78 Cache advisory AEDT storage ledgers; a564a1f Guard AEDT quota bursts with shadow reservations
- Diff total (`main...branch`):  66 files changed, 44623 insertions(+), 1365 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `fix/node-global-shadow-cap-260717`

- Last commit date: **2026-07-17**; ahead of `main`: **157 commits**.
- Intent (inferred from top subjects): e16b788 Cap colocated FEA allocations by physical node resources; 3de4379 Cap standalone AEDT workers per project; 0ca941f Clarify allocation versus Slurm node state; b925a98 Protect pooled AEDT hosts from pressure requeue
- Diff total (`main...branch`):  66 files changed, 45026 insertions(+), 1413 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **1**; vs 260725 line: **0**.
- Top unique vs integration-260717: e16b788 Cap colocated FEA allocations by physical node resources
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in the 260725 line.**

## `nec/integration/aedt-same-node-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **136 commits**.
- Intent (inferred from top subjects): 592494b Allow pooled AEDT cohorts to fill for two hours; d7f4e1b Keep AEDT pool allocations off GPU nodes; e8f14c5 Allow AEDT pools on 48-core nodes; 7c99c7c Reduce SQLite writer contention during AEDT ramp
- Diff total (`main...branch`):  65 files changed, 36680 insertions(+), 1136 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `origin/release/aedt-same-node-q23-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **137 commits**.
- Intent (inferred from top subjects): 3febcfa Stabilize pooled AEDT pre-admission and scaling; 592494b Allow pooled AEDT cohorts to fill for two hours; d7f4e1b Keep AEDT pool allocations off GPU nodes; e8f14c5 Allow AEDT pools on 48-core nodes
- Diff total (`main...branch`):  65 files changed, 39463 insertions(+), 1137 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `integration-source/aedt-shape`

- Last commit date: **2026-07-16**; ahead of `main`: **146 commits**.
- Intent (inferred from top subjects): bdd6f79 Honor exact AEDT client capacity reservations; 3e7a697 Size AEDT allocations by complete session footprints; 2df0dc6 Keep AEDT reconcile callbacks outside SQLite writer; e2adeef Guard destructive AEDT adapter withdrawal
- Diff total (`main...branch`):  65 files changed, 41312 insertions(+), 1269 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/API.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/campaign_mutation_lock.py, slurm_scheduler/slurm.py, templates/project_detail.html, tests/test_aedt_pool.py, tests/test_campaign_demand.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `release/aedt-busy-probe-skip-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **113 commits**.
- Intent (inferred from top subjects): bb4c01c Skip native AEDT probes while projects are live; 288753f Keep AEDT lease heartbeat alive through outages; bb0e5f1 Require protocol v2 for mixed AEDT canaries; 9bb3119 Harden mixed canary batch and idle host recovery
- Diff total (`main...branch`):  60 files changed, 30900 insertions(+), 1036 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `release/aedt-dead-session-reap-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **117 commits**.
- Intent (inferred from top subjects): 5e9d7ac Add verified dead AEDT session reaper; 0c568dd Avoid backup worker churn between intervals; cf7de6b Keep backup cadence scans off scheduler ticks; b58f7c8 Run scheduler database backups asynchronously
- Diff total (`main...branch`):  60 files changed, 31989 insertions(+), 1152 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `origin/release/aedt-dso-fix-260715`

- Last commit date: **2026-07-15**; ahead of `main`: **98 commits**.
- Intent (inferred from top subjects): 467b16b Load complete PyAEDT DSO templates; 2426721 Fix AEDT session launch ownership retries; ff84a75 Make pooled attach client self-contained; 3702677 Harden pooled AEDT session lifecycle
- Diff total (`main...branch`):  58 files changed, 27452 insertions(+), 1057 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `release/aedt-inventory-fix-260715`

- Last commit date: **2026-07-15**; ahead of `main`: **99 commits**.
- Intent (inferred from top subjects): bb4a8a7 Add paginated compact task inventory; 467b16b Load complete PyAEDT DSO templates; 2426721 Fix AEDT session launch ownership retries; ff84a75 Make pooled attach client self-contained
- Diff total (`main...branch`):  58 files changed, 27570 insertions(+), 1036 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `release/aedt-keepalive-retry-260716`

- Last commit date: **2026-07-16**; ahead of `main`: **112 commits**.
- Intent (inferred from top subjects): 288753f Keep AEDT lease heartbeat alive through outages; bb0e5f1 Require protocol v2 for mixed AEDT canaries; 9bb3119 Harden mixed canary batch and idle host recovery; f902ab7 Add operator-scoped mixed AEDT canary admission
- Diff total (`main...branch`):  60 files changed, 30711 insertions(+), 1036 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `origin/release/aedt-launch-fix-260715`

- Last commit date: **2026-07-15**; ahead of `main`: **97 commits**.
- Intent (inferred from top subjects): 2426721 Fix AEDT session launch ownership retries; ff84a75 Make pooled attach client self-contained; 3702677 Harden pooled AEDT session lifecycle; 3766b77 test: recovery semantics - live quarantine blocks, terminal history does not
- Diff total (`main...branch`):  58 files changed, 27279 insertions(+), 1057 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## `release/aedt-triple-lock-260715`

- Last commit date: **2026-07-16**; ahead of `main`: **111 commits**.
- Intent (inferred from top subjects): bb0e5f1 Require protocol v2 for mixed AEDT canaries; 9bb3119 Harden mixed canary batch and idle host recovery; f902ab7 Add operator-scoped mixed AEDT canary admission; ac3689c Bound AEDT native proxy liveness probe
- Diff total (`main...branch`):  60 files changed, 30668 insertions(+), 1036 deletions(-)
- Notable files: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/aedt_pool_client_guide_ko.md, docs/aedt_pool_runbook.md, docs/central_pool_pilot_260714.md, docs/incident_task_capacity_project_admission_260713.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to1_result_30089.md, docs/mft_aedt_attach_1to2.md, docs/mft_aedt_attach_1to2_result_30445.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py
- Patch-unique vs integration-260717: **0**; vs 260725 line: **0**.
- **Fully contained in integration-260717** (no non-merge patch-unique commits).
- **Fully contained in 260725 line** (no non-merge patch-unique commits).
- **MFT-CAMPAIGN EXCLUSION FLAG:** touches contract/campaign paths or matching diff text: docs/aedt_backend_selector.md, docs/aedt_pool.md, docs/mft_aedt_attach_1to1.md, docs/mft_aedt_attach_1to2.md, scripts/aedt_pool_1to1_pilot.py, scripts/aedt_pool_1to2_pilot.py, slurm_scheduler/aedt_attach_client.py, slurm_scheduler/app.py, slurm_scheduler/slurm.py, tests/test_aedt_pool.py, tests/test_core.py. Exclude these portions from consolidation.
- Verdict: **SUPERSEDED -fully contained (patch-equivalent) in integration-260717.**

## Recommended base head + ordered cherry-pick list (from my half)

**Recommended base: `fix/attached-task-cpu-contract-260725`.** It is the newer scheduler/FEA line and, for every branch in this half, contains either the exact patch, a later semantic successor, or (for the one unmatched canary handoff) all runtime work worth retaining. It also already includes the two commits that are patch-unique relative to integration-260717 but not relative to 260725: `e16b788` (physical-node FEA cap) and `cea0052` (independent standalone AEDT lanes).

**Ordered cherry-pick list: none from this half.**

Do not cherry-pick the apparent raw unique tips `f3efd3e`, `d7eb5e0`, or `ccd9f71`; use their newer 260725-line successors `8f5e7ac`, `9ca42f6`, and `a564a1f`. Do not replay the large alternate exact-pin squash `efb507d` or its stacked `720a3da`; the 260725 line has later exact-session/deadline work (`31711d5`, `e5d10b6`, plus subsequent routing/storage fixes). Skip documentation-only `b5f8e72`.

**Mandatory exclusion boundary:** the surveyed ancestry contains substantial MFT-campaign material. During consolidation, exclude changes specific to `mft_pipeline_status.py`, `campaign_mutation_lock.py`, `/api/mft-pipeline`, campaign-demand, simulation-policy, and the `MFT_AEDT_*` environment contract. The per-branch flags above identify where path/text screening detected this material; inherited MFT flags do not imply that each branch's top AEDT fix itself is campaign-specific.

---

# Part B branch survey: AEDT pool, scaling, license, web, and FEA

Survey date: 2026-07-28. Repository: `Y:\git\slurm_scheduler`.

This survey used read-only Git operations only and did not switch branches. All requested optional refs exist. The local `release/async-db-backup-260716` exists and matches its remote; all three `origin/deploy/q24-validated-async-package-*` refs exist; both `live/node-canary-260714` and `review/native-suspect-heartbeat-timeout-260716` exist.

Notation:

- **I** = `codex/aedt-integration-260717`
- **N** = `fix/attached-task-cpu-contract-260725`
- “raw unique” is exactly `git rev-list --count HEAD..BRANCH`; it is an ancestry count, not a patch-equivalence count.
- Patch containment was checked with branch-side `git log --left-right --cherry-mark/--cherry-pick BRANCH...HEAD`.
- The displayed diff totals are the final line of `git diff --stat main...BRANCH`; each detail also records the other two `tail -3` paths where there was a diff.

## Executive conclusions

1. Prefer **N, `fix/attached-task-cpu-contract-260725` at `1559a7c`**, as the consolidation base source. I and N share merge base `3de4379`. I then has 2 commits and N has 55, but I's two tip patches were replayed in N:
   - I `19f855a Retire excess pending AEDT pool allocations` = N `a7c5b1b`
   - I `09ce0d9 Cap AEDT pool allocations to exact session demand` = N `4c07abc`
   N therefore patch-contains I's complete AEDT feature depth and adds 53 patch-unique scheduler/FEA commits.
2. The generic unique additions missing from N are:
   - `4facdfe` — retain storage-safe strict FEA demand pools
   - `61b95c7` — survive a 15-minute AEDT control-plane outage
   - `6c3df6d` — prevent task-refresh starvation
   - `64d0f05` — track the real Windows worker process
   - `d30acf0` — avoid Windows Proactor listener loss
3. Do not also take `35f5fdb`; it is patch-identical to `64d0f05`. Do not take `23f1a05` on N; N already has patch-equivalent `0db7156`.
4. N is not clean of the prohibited MFT campaign work. Choosing it as the base requires deleting/reverting the MFT-only chain and surgically removing the `MFT_AEDT_*` compatibility contract from mixed generic pool commits. Skipping the named MFT branches alone is insufficient.
5. `control_plane_relay.py` is generic AEDT infrastructure, not MFT-coupled: its allowlist is only `/api/aedt-pool/` and `/healthz`, and it contains no MFT route or identifier. Keep the relay commits.

## Mandatory MFT-campaign exclusion map

Exclude whole commits/components where the change is entirely campaign-specific:

- `eb4668b`, patch-equivalent in N as `b4baaa9`: new `mft_pipeline_status.py`, `/api/mft-pipeline`, DB/API/dashboard integration, and `tests/test_mft_pipeline_status.py`.
- The later N MFT-status/UI-only chain: `aeafd97`, `857c36e`, `dbf938f`, `5864ab6`, `4cc0f70`, `4f657dd`, `4c1722b`, `4a33a83`, `adcb833`, `c7bde0c`, `bf638ab`, `88b59b8`, and `24b21e5`.
- `dc8e079 Add durable campaign demand controls`: `campaign_mutation_lock.py`, campaign-demand and simulation-policy routes, MFT project DB/config/UI/tests, and `MFT_ACTIVE_CONCURRENCY_CEILING`.
- `f4070c4 Raise MFT rolling concurrency ceiling to 500` (patch-equivalent to branch tip `ce7fa68`).
- MFT-only docs, campaign cards/routes, and concrete MFT project defaults.

Mixed generic commits need surgical filtering rather than wholesale removal:

- Strip all `MFT_AEDT_*` aliases/contracts from `docs/aedt_backend_selector.md`, `docs/aedt_pool.md`, `docs/mft_aedt_attach_*`, `scripts/aedt_pool_1to{1,2}_pilot.py`, `slurm_scheduler/aedt_attach_client.py`, `slurm_scheduler/app.py`, `slurm_scheduler/slurm.py`, and related tests.
- Relevant mixed history includes `8fe04d8`, `cc5a8c1`, `bd3cc0d`, `f3a65ee`, `bdbc6cc`, `9f7b005`, `3702677`, `2108a74`, `4df497a`, `31711d5`, `3d2d193`, and `d6080e1`.
- Retain the generic lane-cap framework from `cea0052`, but remove concrete `MFT_1MW_2026v1`, `mft_data`, `mft_nsga_fea_validation`, `mft-camp-*`, and `mft-nsgafea-*` definitions/examples.

The five recommended cherry-picks were checked directly; none contains `MFT_AEDT_*`, `MFT_ACTIVE_CONCURRENCY_CEILING`, `mft_pipeline_status`, `campaign_mutation_lock`, `/api/mft-pipeline`, `campaign-demand`, or `simulation-policy`.

## Does the 260725 line contain the AEDT pool modules?

Yes. Both `fix/attached-task-cpu-contract-260725` and `fix/strict-demand-attach-260725` contain:

- `slurm_scheduler/aedt_pool.py`
- `slurm_scheduler/aedt_session_host.py`
- `slurm_scheduler/aedt_attach_client.py`
- `slurm_scheduler/aedt_pool_api.py`

The two 260725 sibling heads are identical for all four files. The requested I-to-N comparison is:

```text
slurm_scheduler/aedt_pool.py | 2080 +++++++++++++++++++++++++++++++++++++++---
1 file changed, 1945 insertions(+), 135 deletions(-)
```

Thus `aedt_session_host.py`, `aedt_attach_client.py`, and `aedt_pool_api.py` are byte-identical between I and N; only `aedt_pool.py` differs. N's `aedt_pool.py` adds storage-aware batch/flexible cohort routing, storage-safe account preadmission, quota-growth and quota-churn guards, permitted exact-reservation retention, draining-capacity and zero-owner-drain repair, per-account storage-pressure drain limits, stale hard-pending allocation replanning, and strict-node contract support. Commits touching it after I's merge base are `a7c5b1b`, `4c07abc`, `70e4918`, `4920cf1`, `d6dce1e`, `fe9839f`, `3d2d193`, `e5d10b6`, `0305665`, `6eafa06`, `d6080e1`, `bc794a0`, and `751aadd`.

## Summary matrix

| # | Branch | Last date | Ahead | Raw unique vs I / N | Result |
|---:|---|---:|---:|---:|---|
| 1 | `experiment/central-pool-260714` | 2026-07-14 | 56 | 0 / 0 | SUPERSEDED |
| 2 | `experiment/central-pool-pilot-260714` | 2026-07-14 | 63 | 0 / 0 | SUPERSEDED |
| 3 | `experiment/pool-ratio3-260714` | 2026-07-14 | 66 | 0 / 0 | SUPERSEDED |
| 4 | `experiment/pool-reserve-exempt-260714` | 2026-07-14 | 63 | 0 / 0 | SUPERSEDED |
| 5 | `experiment/pool-start-race-260714` | 2026-07-14 | 74 | 0 / 0 | SUPERSEDED |
| 6 | `experiment/license-scalein-260714` | 2026-07-14 | 62 | 0 / 0 | SUPERSEDED |
| 7 | `experiment/remove-node-local-260714` | 2026-07-14 | 74 | 0 / 0 | SUPERSEDED |
| 8 | `codex/open-ended-500-260716` | 2026-07-16 | 130 | 1 / 1, patch-equivalent | SUPERSEDED / EXCLUDE |
| 9 | `codex/q22-total-demand-260716` | 2026-07-16 | 126 | 1 / 1, patch-equivalent | SUPERSEDED / EXCLUDE |
| 10 | `codex/native-pipeline-barrier-260716` | 2026-07-16 | 118 | 1 / 1, production-equivalent successor | SUPERSEDED |
| 11 | `codex/exact-pin-native-barrier-integration-260716` | 2026-07-16 | 121 | 0 / 0 | SUPERSEDED |
| 12 | `codex/mixed-task-nonce-260716` | 2026-07-16 | 111 | 0 / 0 | SUPERSEDED |
| 13 | `codex/ui-knob-260715` | 2026-07-15 | 86 | 0 / 0 | SUPERSEDED |
| 14 | `fix/web-supervisor-recovery-260716` | 2026-07-16 | 137 | 6 / 6; 1 patch-unique | UNIQUE-KEEP, deduplicate |
| 15 | `fix/web-ui-load-guard-260716` | 2026-07-16 | 124 | 0 / 0 | SUPERSEDED |
| 16 | `fix/client-outage-resilience-260716` | 2026-07-16 | 130 | 1 / 1 | UNIQUE-KEEP |
| 17 | `fix/8002-listener-recovery-a58-260722` | 2026-07-22 | 205 | 49 / 2 | UNIQUE-KEEP |
| 18 | `fix/8002-transient-recovery-260722` | 2026-07-19 | 140 | 9 / 9; 2 / 1 patch-unique | UNIQUE-KEEP |
| 19 | `feature/mft-pipeline-visibility-260717` | 2026-07-17 | 159 | 1 / 3; 1 / 0 patch-unique | SUPERSEDED / EXCLUDE |
| 20 | `fix/mft-validation-lane-cap-260717` | 2026-07-17 | 158 | 2 / 0 | SUPERSEDED on N |
| 21 | `fix/attached-task-cpu-contract-260725` | 2026-07-25 | 211 | 55 / 0 | UNIQUE-KEEP base |
| 22 | `fix/strict-demand-attach-260725` | 2026-07-25 | 211 | 55 / 1 | UNIQUE-KEEP `4facdfe` |
| 23 | `fix/task-capacity-project-admission-260713` | 2026-07-13 | 25 | 1 / 1, patch-equivalent | SUPERSEDED |
| 24 | `feature/project-env` | 2026-07-09 | 0 | 0 / 0 | SUPERSEDED |
| 25a | `integration-source/cohort-wait` | 2026-07-17 | 147 | 1 / 1, patch-equivalent | SUPERSEDED |
| 25b | `integration-source/nonactive-capacity` | 2026-07-17 | 147 | 1 / 1, patch-equivalent | SUPERSEDED |
| 25c | `integration-source/nonowning-client` | 2026-07-17 | 147 | 1 / 1, patch-equivalent | SUPERSEDED |
| 25d | `integration-source/terminal-cleanup` | 2026-07-17 | 147 | 1 / 1, patch-equivalent | SUPERSEDED |
| 26a | `origin/deploy/q24-validated-async-package-28fde9f` | 2026-07-16 | 138 | 0 / 0 | SUPERSEDED snapshot |
| 26b | `origin/deploy/q24-validated-async-package-260b273` | 2026-07-16 | 139 | 0 / 0 | SUPERSEDED snapshot |
| 26c | `origin/deploy/q24-validated-async-package-4df497a` | 2026-07-16 | 140 | 0 / 0 | SUPERSEDED snapshot |
| 27 | `release/async-db-backup-260716` | 2026-07-16 | 116 | 0 / 0 | SUPERSEDED |
| 28a | `live/node-canary-260714` | 2026-07-15 | 96 | 0 / 0 | SUPERSEDED |
| 28b | `review/native-suspect-heartbeat-timeout-260716` | 2026-07-16 | 109 | 1 / 1, semantically superseded | SUPERSEDED |

## Branch-by-branch findings

### 1. `experiment/central-pool-260714`

- **Tip/date/ahead:** `9bf7944`, 2026-07-14; 56 commits ahead of `main`.
- **Intent/log evidence:** establish the central AEDT pool and control-plane relay; the top log also shows node-local session exposure, slow-tick freshness fixes, configurable task refresh/concurrency, warm-spare, and license-refresh work.
- **Files:** stat tail: `tests/test_project_env.py`, `tests/test_web_supervisor.py`, **57 files changed, 16,810 insertions, 979 deletions**. Notable: `aedt_pool.py`, `scheduler.py`, `control_plane_relay.py`, `aedt_pool_api.py`, `aedt_session_host.py`, pool/relay tests, and `scripts/aedt_pool_1to2_pilot.py`.
- **Unique test/state:** `I..branch=0`; `N..branch=0`; no branch-side cherry subjects. **Fully contained in integration-260717** and **fully contained in the 260725 line**.
- **Verdict:** **SUPERSEDED — cherry-pick none.**
- **MFT flag:** inherited MFT attach pilots/docs and `MFT_AEDT_*` aliases must be excluded. The relay itself is generic and should remain.

### 2. `experiment/central-pool-pilot-260714`

- **Tip/date/ahead:** `891b9cc`, 2026-07-14; ahead 63.
- **Intent/log evidence:** add a disposable central-pool end-to-end pilot after merging the relay into the live canary and adding baseline-first FEA assignment (`891b9cc`, `21d9e9b`, `2bdfd6f`, `95cb5ff`).
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **61 files, +18,208/-1,035**. Notable: core pool/relay/API/host modules, `scheduler.py`, `scripts/aedt_pool_central_pilot.py`, pilot docs, and pool/core tests.
- **Unique test/state:** 0 vs I and 0 vs N; **fully contained in both heads**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** inherited `MFT_AEDT_*` pilot/task contract only; exclude it. No pipeline-status or campaign-demand delta.

### 3. `experiment/pool-ratio3-260714`

- **Tip/date/ahead:** `0e69d08`, 2026-07-14; ahead 66.
- **Intent/log evidence:** operator-approved three projects per AEDT session, combining the central-pool pilot and license-reserve exemption.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **62 files, +18,418/-1,025**. Notable: `aedt_pool.py`, `scheduler.py`, pool/API/host/relay modules, pool UI/tests, and license-reserve tests.
- **Unique test/state:** 0 / 0; **fully contained in I and N**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** inherited MFT attach docs/pilots/env aliases only; exclude those, retain the generic ratio logic already in both heads.

### 4. `experiment/pool-reserve-exempt-260714`

- **Tip/date/ahead:** `737b7b0`, 2026-07-14; ahead 63.
- **Intent/log evidence:** allow configured projects to bypass the normal license reserve, atop baseline-first FEA and central-pool work.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **60 files, +18,115/-1,025**. Notable: `scheduler.py`, `config.py`, `app.py`, `config/app.example.yaml`, and `tests/test_license_reserve_exempt.py`, plus inherited pool modules.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** only inherited pilot/env content; the reserve-exemption delta is generic.

### 5. `experiment/pool-start-race-260714`

- **Tip/date/ahead:** `83f77ee`, 2026-07-14; ahead 74.
- **Intent/log evidence:** harden AEDT pool startup races after removing node-local operation and normalizing FEA/AEDT counters.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **58 files, +18,477/-1,030**. Notable: `aedt_pool.py`, API/host modules, `scheduler.py`, dashboard, `tests/test_aedt_pool.py`, and task-count tests.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** inherited MFT aliases/pilots only; startup-race production work is generic and contained.

### 6. `experiment/license-scalein-260714`

- **Tip/date/ahead:** `21d9e9b`, 2026-07-14; ahead 62.
- **Intent/log evidence:** despite the branch name, the tip is baseline-first FEA assignment with infrastructure CPUs excluded from the solver budget (`2bdfd6f`), plus license-snapshot freshness.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **59 files, +17,959/-1,035**. Notable: `scheduler.py`, config/app, `tests/test_fea_baseline_assignment.py`, core tests, and inherited AEDT modules.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** inherited aliases/pilots only; the FEA budget logic is generic.

### 7. `experiment/remove-node-local-260714`

- **Tip/date/ahead:** `5d48d77`, 2026-07-14; ahead 74.
- **Intent/log evidence:** eliminate the node-local AEDT model in favor of one central pool, simplify counters, and split live session state from history in the UI.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **58 files, +18,173/-1,030**. Notable: `aedt_pool.py`, pool/dashboard templates, `app.py`, `db.py`, and pool/task-count tests; `cb66ab3` deletes obsolete node-local code.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** inherited `MFT_AEDT_*` pieces must be stripped; central-pool unification is generic.

### 8. `codex/open-ended-500-260716`

- **Tip/date/ahead:** `ce7fa68`, 2026-07-16; ahead 130.
- **Intent/log evidence:** raise the MFT rolling concurrency ceiling to 500, on durable campaign-demand controls and the AEDT reliability/exact-pin/native-barrier line.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +35,862/-1,108**. Notable: pool/host/client/scheduler modules, `app.py`, `campaign_mutation_lock.py`, project UI, and campaign tests.
- **Unique test/state:** raw 1 vs I and 1 vs N: `ce7fa68 Raise MFT rolling concurrency ceiling to 500`; cherry marks it equal to `f4070c4` in both. **Fully contained patch-equivalently in I and N.**
- **Verdict:** **SUPERSEDED / EXCLUDE — none.**
- **MFT flag:** the unique delta is entirely prohibited. Both candidate heads already contain the equivalent, so remove `f4070c4` from consolidation.

### 9. `codex/q22-total-demand-260716`

- **Tip/date/ahead:** `fdcadd8`, 2026-07-16; ahead 126.
- **Intent/log evidence:** durable revisioned MFT campaign total-demand controls and a host-wide feeder mutation lock.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +35,545/-1,108**. Notable: `campaign_mutation_lock.py`, `app.py`, `db.py`, `config.py`, project UI, `tests/test_campaign_demand.py`, plus inherited AEDT modules.
- **Unique test/state:** raw 1 / 1; `fdcadd8 Add durable campaign demand controls` is patch-equivalent to `dc8e079` in both. **Fully contained patch-equivalently in I and N.**
- **Verdict:** **SUPERSEDED / EXCLUDE — none.**
- **MFT flag:** exclude the entire delta: mutation lock, campaign-demand, simulation-policy, active-ceiling contract, DB/UI/config/tests.

### 10. `codex/native-pipeline-barrier-260716`

- **Tip/date/ahead:** `353fff2`, 2026-07-16; ahead 118.
- **Intent/log evidence:** gate AEDT postprocess on sealed cohort completion; inherited work includes dead-session reaping, async backup, outage handling, mixed canaries, liveness, logs, and workspace cleanup.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **60 files, +32,489/-1,152**. Notable: `aedt_pool.py`, `aedt_attach_client.py`, `aedt_pool_api.py`, host/scheduler/app, and pool tests.
- **Unique test/state:** raw 1 / 1 and cherry reports `353fff2`. However, successor `76c6e98` in both heads has exactly the same normalized production deltas in all three production files; only an obsolete route-count test assertion differs. **Has a raw/patch-id-unique commit, but its production work is fully contained via `76c6e98`.**
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** the barrier itself is generic. Strip inherited `MFT_AEDT_*` and simulation-policy contracts; keep the generic relay.

### 11. `codex/exact-pin-native-barrier-integration-260716`

- **Tip/date/ahead:** `9150e7f`, 2026-07-16; ahead 121.
- **Intent/log evidence:** reconcile exact-session pinning/reservation with the native pipeline barrier, dead-target cohort failure, and verified-dead reaping.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **60 files, +33,668/-1,152**. Notable: `tests/test_aedt_pool.py`, `aedt_pool.py`, core tests, host/client/API, scheduler/app, and relay.
- **Unique test/state:** 0 / 0; **fully contained in integration-260717** and **fully contained in the 260725 line**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** inherited `MFT_AEDT_*` and simulation-policy slices must be stripped; no pipeline-status/campaign-demand tip.

### 12. `codex/mixed-task-nonce-260716`

- **Tip/date/ahead:** `bb0e5f1`, 2026-07-16; ahead 111.
- **Intent/log evidence:** require protocol v2 for mixed AEDT canaries, harden batch/idle recovery, scope canary admission, bound native probes, isolate logs, and serialize automation.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **60 files, +30,668/-1,036**. Notable: pool/host/client/scheduler/relay modules and pool/core tests.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** strip inherited MFT task/env and simulation-policy contracts; no pipeline/campaign-demand content at this tip.

### 13. `codex/ui-knob-260715`

- **Tip/date/ahead:** `775f980`, 2026-07-15; ahead 86.
- **Intent/log evidence:** expose an AEDT concurrent-simulations operator knob, following pool timestamp, nonblocking API, lease/heartbeat, relay-capacity, and outage-resilience work.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **58 files, +20,725/-1,030**. Notable: core/pool tests, `aedt_pool.py`, scheduler, relay, host, dashboard/app/client.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** inherited `MFT_AEDT_*` aliases only; this tip predates campaign-demand/pipeline status.

### 14. `fix/web-supervisor-recovery-260716`

- **Tip/date/ahead:** `35f5fdb`, 2026-07-16; ahead 137.
- **Intent/log evidence:** make the Windows supervisor launch/track the real base-Python Uvicorn worker rather than the venv redirector and remove the delay after a watchdog-confirmed restart.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +36,722/-1,138**. Unique files: `scripts/start_web.cmd`, `slurm_scheduler/web_supervisor.py`, incident note, and supervisor tests; inherited churn is mostly pool/core.
- **Unique test/state:** raw 6 vs each head, but cherry leaves only `35f5fdb fix(web): track real Windows worker process`; the other five are patch-equivalent. **Has a unique commit vs I and N.**
- **Verdict:** **UNIQUE-KEEP `35f5fdb`, or equivalent `64d0f05`, but not both.** The final list uses `64d0f05` because `d30acf0` is based on it.
- **MFT flag:** branch ancestry contains campaign material, but the unique web patch has no prohibited component.

### 15. `fix/web-ui-load-guard-260716`

- **Tip/date/ahead:** `9562c6f`, 2026-07-16; ahead 124.
- **Intent/log evidence:** stage dashboard pagination without transient errors, keep the UI responsive under task-list bursts, and replace idle sessions on draining allocations.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **62 files, +34,489/-1,102**. Notable: pool/core tests, `aedt_pool.py`, host/client, scheduler, app/db/dashboard, and web-read-guard tests.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** strip inherited MFT env/simulation-policy slices; no campaign-demand or pipeline-status delta.

### 16. `fix/client-outage-resilience-260716`

- **Tip/date/ahead:** `61b95c7`, 2026-07-16; ahead 130.
- **Intent/log evidence:** keep AEDT clients/hosts alive through a 15-minute relay outage by increasing client/host outage budgets from 360 to 1,200 seconds and pool lease/heartbeat defaults from 600 to 1,800 seconds.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +36,208/-1,108**. Unique delta: `aedt_attach_client.py`, `aedt_pool.py`, `aedt_session_host.py`, and pool tests (+365/-15).
- **Unique test/state:** raw 1 / 1; branch-side cherry subject is exactly `61b95c7 Keep AEDT pool alive through 15-minute relay outages`. **Has a unique commit vs I and N.**
- **Verdict:** **UNIQUE-KEEP `61b95c71320e4b1e00d6a5fdf04781da81a5523c`.**
- **MFT flag:** inherited campaign content is excluded; `61b95c7` itself is clean.

### 17. `fix/8002-listener-recovery-a58-260722`

- **Tip/date/ahead:** `d30acf0`, 2026-07-22; ahead 205.
- **Intent/log evidence:** prevent Windows Proactor/IOCP `AcceptEx` listener loss by serving Uvicorn on `SelectorEventLoop`, layered on the real-worker supervisor fix. The top log also carries later FEA admission, pagination, scheduler-read, pool-capacity, and MFT UI history.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **69 files, +63,682/-3,815**. Notable: core/pool tests, `aedt_pool.py`, `scheduler.py`, `mft_pipeline_status.py`, DB/app/dashboard, and web-supervisor files.
- **Unique test/state:** vs I raw 49, 47 patch-unique; top subjects include `d30acf0`, `64d0f05`, `a58b518`, `cb8696d`, `7bfe94f`, and `66013eb`. Vs N raw 2 and cherry leaves exactly `64d0f05 fix(web): track real Windows worker process` and `d30acf0 Avoid Windows Proactor listener loss`. **Has two unique commits vs N.**
- **Verdict:** **UNIQUE-KEEP, in order: `64d0f0599d76c9aa27bf464ec496058675bfcec5`, then `d30acf083c9d376e49df15bcef29c0388430b9f5`.**
- **MFT flag:** the branch contains every prohibited campaign category and the later pipeline-status chain; exclude them all. Neither web keeper touches them.

### 18. `fix/8002-transient-recovery-260722`

- **Tip/date/ahead:** `6c3df6d`, 2026-07-19; ahead 140.
- **Intent/log evidence:** prevent bounded task-refresh scans from starving FEA tasks, enforce timeout cancellation independently of remote probing, and limit FEA pressure reclaim per node.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **66 files, +40,406/-1,148**. Notable: pool/core tests, `aedt_pool.py`, `scheduler.py`, host/client/db/app, and new `tests/test_task_refresh_fairness.py`.
- **Unique test/state:** vs I raw 9; cherry leaves `6c3df6d Prevent task refresh starvation` and `23f1a05 Limit FEA memory-pressure reclaim per node`. Vs N raw 9; cherry leaves only `6c3df6d`, because `23f1a05` = N `0db7156`. **Has one unique commit vs N.**
- **Verdict:** **UNIQUE-KEEP `6c3df6d4e67e7256c7977294b460b20c65157644` on N.** If I were used, also take `23f1a05`.
- **MFT flag:** inherited campaign material must be excluded; `6c3df6d` is clean.

### 19. `feature/mft-pipeline-visibility-260717`

- **Tip/date/ahead:** `eb4668b`, 2026-07-17; ahead 159.
- **Intent/log evidence:** expose continuous MFT pipeline status; inherited top log includes exact pool-demand caps and pending-allocation retirement.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **68 files, +46,189/-1,370**. Overall notable: pool modules plus `app.py`, `db.py`, new `mft_pipeline_status.py`, dashboard, and tests. The tip itself changes only seven MFT endpoint/status/UI files (+1,206/-1).
- **Unique test/state:** vs I raw 1 and genuinely unique `eb4668b`. Vs N raw 3 but all cherry-equal: `eb4668b`=`b4baaa9`, `09ce0d9`=`4c07abc`, `19f855a`=`a7c5b1b`. **Has a unique MFT commit vs I; fully contained patch-equivalently in N.**
- **Verdict:** **SUPERSEDED / EXCLUDE — no generic keeper.**
- **MFT flag:** exclude the entire unique tip. Its `web_read_guard.py` edit only supports `/api/mft-pipeline`; it is not an independent generic web fix.

### 20. `fix/mft-validation-lane-cap-260717`

- **Tip/date/ahead:** `cea0052`, 2026-07-17; ahead 158.
- **Intent/log evidence:** `e16b788` caps colocated FEA allocations by physical node CPU/RAM; `cea0052` reserves independent standalone AEDT lanes.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **66 files, +45,297/-1,339**. Branch-specific notable: `scheduler.py`, `config.py`, `db.py`, `app.py`, sample config, and core tests.
- **Unique test/state:** vs I raw 2 with both subjects above; vs N raw 0. **Has two unique generic commits vs I; fully contained in the 260725 line.**
- **Verdict:** **SUPERSEDED on N — none.** On I, keep `e16b788` then generic parts of `cea0052`.
- **MFT flag:** it does not contain `mft_pipeline_status.py`, but inherits campaign-demand/ceiling/simulation-policy/env contracts. Strip concrete MFT lane names/defaults from `cea0052` while retaining the generic mechanism.

### 21. `fix/attached-task-cpu-contract-260725`

- **Tip/date/ahead:** `1559a7c`, 2026-07-25; ahead 211.
- **Intent/log evidence:** newest scheduler/FEA line: preserve claimed Priority FEA pools, recheck storage at final claim, cap strict same-node work by owned CPUs, gate pressure reclaim, add per-task strict placement, protect sealed artifacts, fix FEA starvation, pagination, and scheduler-read hot paths.
- **Files:** stat tail: `tests/test_web_supervisor.py`, `tests/test_workspace_prune_protection.py`, **72 files, +67,333/-3,420**. Notable: `scheduler.py`, `aedt_pool.py`, `db.py`, app/models/slurm/retention, generic web/templates/tests, plus prohibited MFT status/UI files.
- **Unique test/state:** vs I raw 55, 53 patch-unique; top subjects are `1559a7c`, `41b3b93`, `2b637ad`, `e542c8a`, `22f6fb9`, `751aadd`, `0800a8d`, `190f10d`, `a58b518`, `cb8696d`, `7bfe94f`, `66013eb`, `bf59208`, `71ae4e9`, and `24b21e5`. Vs itself 0. **Has unique commits vs I; is the complete N line.**
- **Verdict:** **UNIQUE-KEEP as preferred base source at `1559a7c9d236a9efa323cf7a25a3194cd58b23f4`; no self-cherry-pick.**
- **MFT flag:** contains the full prohibited pipeline/status chain, campaign lock/demand, simulation-policy, ceiling, and MFT env aliases. Apply the global exclusion map. Relay code is generic.

### 22. `fix/strict-demand-attach-260725`

- **Tip/date/ahead:** `4facdfe`, 2026-07-25; ahead 211.
- **Intent/log evidence:** retain one structurally fitting, storage-safe strict-node FEA demand pool while transient pressure blocks attachment; the final memory/load/storage gates still run.
- **Files:** stat tail: `tests/test_web_supervisor.py`, `tests/test_workspace_prune_protection.py`, **72 files, +67,587/-3,768**. Tip delta is only `scheduler.py` and `tests/test_core.py` (+204).
- **Unique test/state:** vs I raw 55, 53 patch-unique. Vs N raw 1 with `4facdfe fix: retain storage-safe strict FEA demand pools`. N separately has sibling `1559a7c`; the two are complementary. **Has one unique commit vs N.**
- **Verdict:** **UNIQUE-KEEP `4facdfe36f74ff0679a477f6c25e1dddf26ce791`.**
- **MFT flag:** inherited base contamination must be removed; `4facdfe` itself has no prohibited token/path.

### 23. `fix/task-capacity-project-admission-260713`

- **Tip/date/ahead:** `597af30`, 2026-07-13; ahead 25.
- **Intent/log evidence:** pass project identity through capacity admission and surface project-aware license/admission diagnostics; inherited work includes exclusive allocation and FEA utilization fixes.
- **Files:** stat tail: `templates/task_detail.html`, `tests/test_core.py`, **19 files, +4,083/-294**. Notable: `scheduler.py`, `slurm.py`, `app.py`, node/dashboard templates, config, incident doc, and core tests.
- **Unique test/state:** raw 1 / 1, but `597af30` is cherry-equal to `42ddca2` in both. **Fully contained patch-equivalently in I and N.**
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** no flagged campaign component.

### 24. `feature/project-env`

- **Tip/date/ahead:** `7ba1c48`, 2026-07-09; ahead 0.
- **Intent/log evidence:** branch head says “Add Project Environments: git-repo project bundles deployed per account,” but `git log main..branch` and `git diff main...branch` are empty.
- **Files:** no triple-dot diff; no stat tail.
- **Unique test/state:** 0 / 0; **already contained in `main`, I, and N**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** none.

### 25a. `integration-source/cohort-wait`

- **Tip/date/ahead:** `df35b26`, 2026-07-17; ahead 147.
- **Intent/log evidence:** keep exact AEDT cohorts/clients alive until the fill deadline.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +42,044/-1,269**. Tip delta: `aedt_attach_client.py`, `aedt_pool.py`, and pool tests (+778/-46).
- **Unique test/state:** raw 1 / 1, but `df35b26`=`31711d5`. **Fully contained patch-equivalently in both.**
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** side commit is generic; strip inherited campaign/env contracts.

### 25b. `integration-source/nonactive-capacity`

- **Tip/date/ahead:** `af5bbd5`, 2026-07-17; ahead 147.
- **Intent/log evidence:** exclude nonactive AEDT sessions from usable capacity.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +41,394/-1,269**. Tip delta: `aedt_pool.py` and pool tests (+104/-22).
- **Unique test/state:** raw 1 / 1, but `af5bbd5`=`f4bc16d`. **Fully contained patch-equivalently in both.**
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** side commit generic; inherited MFT content excluded.

### 25c. `integration-source/nonowning-client`

- **Tip/date/ahead:** `6b07c07`, 2026-07-17; ahead 147.
- **Intent/log evidence:** make pooled AEDT clients non-owning so a project client does not close the shared Desktop.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +41,594/-1,269**. Tip delta: attach client and pool tests (+303/-21).
- **Unique test/state:** raw 1 / 1, but `6b07c07`=`248476d`. **Fully contained patch-equivalently in both.**
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** generic side commit; strip inherited aliases.

### 25d. `integration-source/terminal-cleanup`

- **Tip/date/ahead:** `3544a0c`, 2026-07-17; ahead 147.
- **Intent/log evidence:** clean exact terminal AEDT workspaces safely.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **66 files, +42,448/-1,269**. Tip delta: pool/DB/scheduler and new terminal-cleanup tests (+1,136).
- **Unique test/state:** raw 1 / 1, but `3544a0c`=`04eb515`. **Fully contained patch-equivalently in both.**
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** generic side commit; inherited campaign content excluded.

### 26a. `origin/deploy/q24-validated-async-package-28fde9f`

- **Tip/date/ahead:** `28fde9f`, 2026-07-16; ahead 138.
- **Intent/pin:** deploy snapshot pinning `28fde9f Preserve serial AEDT reservation cohorts`.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +40,205/-1,130**. Overall notable: pool/client/host/scheduler/DB/relay and pool/core tests; the pinned commit itself changes pool code/tests (+120/-3).
- **Unique test/state:** 0 / 0; **exact ancestor of both**.
- **Verdict:** **SUPERSEDED deploy snapshot — none.**
- **MFT flag:** snapshot inherits campaign/env material; no deploy-only patch to retain.

### 26b. `origin/deploy/q24-validated-async-package-260b273`

- **Tip/date/ahead:** `260b273`, 2026-07-16; ahead 139.
- **Intent/pin:** the next validated package point, adding `Expose AEDT native solve mode`.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +40,224/-1,130**. Same overall notable files; pinned delta is `aedt_pool.py` and tests (+19).
- **Unique test/state:** 0 / 0; **exact ancestor of both**.
- **Verdict:** **SUPERSEDED deploy snapshot — none.**
- **MFT flag:** inherited contamination only.

### 26c. `origin/deploy/q24-validated-async-package-4df497a`

- **Tip/date/ahead:** `4df497a`, 2026-07-16; ahead 140.
- **Intent/pin:** the next validated package point, adding a two-hour pooled AEDT cohort fill window.
- **Files:** stat tail: `tests/test_web_read_guard.py`, `tests/test_web_supervisor.py`, **65 files, +40,279/-1,130**. Pinned delta is attach client and pool tests (+59/-4).
- **Unique test/state:** 0 / 0; **exact ancestor of both**.
- **Verdict:** **SUPERSEDED deploy snapshot — none.**
- **MFT flag:** strip inherited campaign/env aliases.

The snapshot chain is `28fde9f -> 260b273 -> 4df497a`; the refs add no packaging-only commits and simply pin those exact history points.

### 27. `release/async-db-backup-260716`

- **Tip/date/ahead:** `0c568dd`, 2026-07-16; ahead 116. Local and remote refs are identical.
- **Intent/log evidence:** `b58f7c8` runs scheduler DB backups asynchronously, `cf7de6b` removes cadence scans from scheduler ticks, and `0c568dd` avoids backup worker churn.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **60 files, +31,422/-1,152**. Tip work concentrates in scheduler/core tests; overall notable files include inherited AEDT pool/host/client/relay.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** strip inherited MFT env/simulation-policy slices; backup work is generic.

### 28a. `live/node-canary-260714`

- **Tip/date/ahead:** `ff84a75`, 2026-07-15; ahead 96.
- **Intent/log evidence:** validated node-canary history culminating in a self-contained pooled attach client, hardened session lifecycle, recycle-drain recovery, allocation rotation, vanished-job convergence, and operator pool controls.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **58 files, +27,060/-1,057**. Notable: all four AEDT modules, scheduler/app/DB/relay/web supervisor, templates, and pool/core tests.
- **Unique test/state:** 0 / 0; **fully contained in both**.
- **Verdict:** **SUPERSEDED — none.**
- **MFT flag:** strip inherited env/simulation-policy contract; keep generic canary/pool work already present.

### 28b. `review/native-suspect-heartbeat-timeout-260716`

- **Tip/date/ahead:** `71e53ea`, 2026-07-16; ahead 109.
- **Intent/log evidence:** refresh an unhealthy session heartbeat after a suspect native probe only when an unexpired accepted client owns native work, allowing an idle wedged Desktop to expire.
- **Files:** stat tail: `tests/test_task_count_history.py`, `tests/test_web_supervisor.py`, **60 files, +29,777/-1,036**. Tip delta: `aedt_pool.py` and pool tests (+185/-3).
- **Unique test/state:** raw 1 / 1 and cherry reports `71e53ea`. Nevertheless, both candidates already have the enhanced semantic implementation through `9bb3119` and `dc30e73`: their current `report_session_fault()` contains the same `live_native_owner` query and conditional heartbeat refresh, plus safer conditional drain handling and prompt idle-unhealthy recycling. **Raw unique, semantically superseded in both.**
- **Verdict:** **SUPERSEDED — do not cherry-pick `71e53ea`.**
- **MFT flag:** tip is generic; inherited MFT aliases/routes must be stripped.

## Recommended base head + ordered cherry-pick list (from my half)

Use `fix/attached-task-cpu-contract-260725` at `1559a7c9d236a9efa323cf7a25a3194cd58b23f4` as the base source, then apply the mandatory MFT exclusion map above. It patch-contains I's two post-merge-base AEDT commits and carries the newest generic scheduler/FEA work.

Ordered additions:

1. `4facdfe36f74ff0679a477f6c25e1dddf26ce791` — retain storage-safe strict FEA demand pools; complementary sibling of the base tip.
2. `61b95c71320e4b1e00d6a5fdf04781da81a5523c` — 15-minute AEDT control-plane outage resilience.
3. `6c3df6d4e67e7256c7977294b460b20c65157644` — task-refresh fairness and independent bounded timeout cancellation.
4. `64d0f0599d76c9aa27bf464ec496058675bfcec5` — track the real Windows worker process.
5. `d30acf083c9d376e49df15bcef29c0388430b9f5` — run Windows Uvicorn accepts on `SelectorEventLoop` to avoid Proactor listener loss.

Do not also cherry-pick `35f5fdb` (duplicate of item 4), `23f1a05` (already patch-equivalent as N `0db7156`), or any MFT campaign/status commit.
