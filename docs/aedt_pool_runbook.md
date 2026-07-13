# AEDT Pool Pilot and Rollback Runbook

> **Do not run this pilot against the active MFT campaign allocations.**
> Use a dedicated account/allocation and leave the feature disabled until the
> final evidence record reports `passed`.

## Source and deployment safety

- Build, test, commit, and push this experiment from a dedicated clean clone,
  never from the dirty live scheduler tree.
- Deploy only by checking out an identified GitHub branch and commit SHA onto
  local or cluster-local storage, then verify the checked-out SHA before any
  service action.
- The live scheduler has not been deployed or restarted for this experiment.
  Keep `enabled=false` and `adapter_ready=false` until the complete live A/B
  validation contract passes.

The first integration step is the exclusive 1:1 procedure in
[`mft_aedt_attach_1to1.md`](mft_aedt_attach_1to1.md).  It must use
`priority=10000`, `project=MFT_1MW_2026v1`, and exact full Git SHAs.  It leaves
the production pool disabled and does not satisfy the later 1:2 A/B gate.
After that pass, use [`mft_aedt_attach_1to2.md`](mft_aedt_attach_1to2.md) for
the isolated two-project normal and pre-solve-abort cases. The pilot remains
one scheduler task and does not write a passing live validation automatically.

## 1. Preflight

1. 현재 production scheduler DB를 online backup한다.
2. scheduler와 MFT repo의 dirty worktree/diff를 기록한다.
3. production task/allocation/account 목록을 저장한다.
4. 별도 계정 또는 명시적으로 격리된 `AEDT pool` allocation을 선정한다.
5. Desktop/HFSS/Maxwell/Icepak license feature를 `lmstat`로 5~10초 간격 수집할 준비를 한다.
6. baseline과 pooled case는 같은 geometry, setup, mesh, core count, output contract를 사용한다.

이 단계에서는 다음만 확인한다.

```bash
curl -sS "$SCHEDULER_URL/api/aedt-pool"
curl -sS -X POST "$SCHEDULER_URL/api/aedt-pool/reconcile?dry_run=true"
```

`enabled=false`, `adapter_ready=false` 또는 `validation_passed=false` 중 하나라도 false gate이면
Slurm node와 AEDT가 생성되지 않아야 한다.

### Pilot queue priority

검증/A-B task는 production refill보다 높은 명시적 `priority=10000`으로 제출한다. 이 필드는
기존 `tasks.priority INTEGER`와 `POST /api/tasks`가 이미 지원한다. 값이 클수록 queued attach
순서가 빠르고 같은 값은 FIFO다. API에 별도 policy min/max는 없으며 SQLite signed integer에
저장되지만, pilot은 일관되게 10000만 사용한다.

priority는 실행 중인 task를 선점하거나 취소하지 않는다. 이미 running인 300개 MFT task를
건드리지 않고, 새 capacity가 생겼을 때 queued pilot이 일반 refill보다 먼저 attach되게 할
뿐이다. session-host 내부 task는 control plane이 더 높은 100000을 사용한다.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks" \
  -H 'Content-Type: application/json' \
  --data '{
    "name":"aedt-pool-validation-a",
    "remote_cwd":"/isolated/aedt-pilot",
    "command":"python run_pilot.py --case A",
    "scheduling_profile":"fea_bursty",
    "priority":10000,
    "cpus":4,
    "memory_mb":65536
  }'
```

## 2. Node-side adapter configuration

`config/app.yaml`의 opt-in block 예시:

```yaml
aedt_pool:
  session_host_enabled: false  # pilot 준비가 끝난 뒤에만 true
  scheduler_url: "http://scheduler-host:8000"
  host_remote_cwd: "/shared/slurm_scheduler"
  host_python: "/shared/conda/envs/pyaedt2026v1/bin/python"
  host_env_setup: |
    source /shared/conda/etc/profile.d/conda.sh
    conda activate pyaedt2026v1
  host_bootstrap_token_file: "/shared/secrets/aedt_pool_bootstrap"
  host_task_memory_mb: 4096
```

Scheduler service 환경에는 같은 secret을 넣는다.

```bash
export SLURM_AEDT_POOL_BOOTSTRAP_TOKEN='<random secret>'
```

token file은 compute 계정만 읽도록 권한을 제한한다. 설정만 작성하고 production scheduler를
임의 재시작하지 않는다. 현재 캠페인 owner가 maintenance window를 승인한 뒤 재시작한다.

## 3. Mandatory live A/B

현재 MFT production runner는 항상 새 Desktop을 열고 finally에서 Desktop/descendant를 정리한다.
따라서 A/B 첫 단계는 disposable pilot wrapper에서 `aedt_attach_client`를 직접 사용한다.
MFT runner의 pooled branch는 A/B가 통과한 뒤에만 켠다. 상세 cut point는
`docs/aedt_pool.md`의 integration audit를 참고한다.

### Baseline

- AEDT 2개를 각각 새 Desktop으로 연다.
- project A와 B를 한 개씩 실행한다.
- 각 단계 runtime, peak CPU/RAM, exit, output hash/metrics를 기록한다.
- Desktop 및 solver feature checkout을 연속 수집한다.

### Treatment

- AEDT 1개를 session host가 연다.
- project A와 B가 각각 독립 lease/slot으로 attach한다.
- 같은 runtime/resource/output/license 정보를 기록한다.
- client가 Desktop을 닫지 않고 host만 lifecycle을 소유하는지 확인한다.

Acceptance:

- baseline `2 desktops / 2 projects`, treatment `1 desktop / 2 projects`
- Desktop license delta `<= -1`
- 두 project 모두 성공
- output parity 통과
- treatment runtime / baseline runtime `<= 1.20`
- solver features가 기대한 개수로 checkout되고 서로의 design/project를 덮어쓰지 않음

## 4. Required failure injection

다음은 별도 disposable output 경로에서 수행한다.

1. project A pre-solve error: A만 close/release되고 B가 끝나는지 확인한다.
2. project A 정상 cancel: B가 끝나는지 확인한다.
3. project A solver timeout:
   - session이 즉시 draining/quarantine인지 확인
   - 새 project C가 같은 AEDT에 배정되지 않는지 확인
   - grace 동안 global `StopSimulations`가 호출되지 않는지 확인
   - B가 성공 완료하는지 확인
   - PID/gRPC 생존이 아니라 B terminal output, data row, field solution을 각각 확인
   - fault owner A를 local project-close ACK로 released 처리하지 않는지 확인
   - B 종료 후 Desktop과 전용 allocation이 강제로 drain/recycle되는지 확인
   - A lease가 새 session 대상으로 requeue되는지 확인
   - recycle 전 faulted Desktop에는 project C가 절대 admit되지 않는지 확인
   - recycle 뒤 죽은 A의 solver/Desktop license checkout이 실제 반환되는지 확인
4. AEDT/gRPC process death: A/B lease가 모두 `queued`로 돌아가 새 session에서 재시도되는지 확인한다.
5. node/session-host task death: allocation claim, stale heartbeat, requeue와 cleanup을 확인한다.

특정 solver PID만 종료하는 실험은 `scripts/aedt_pool_fault_injection.py`의 dry-run을 먼저 보고,
명시적 execute flag와 PID identity 조건을 모두 준 disposable pilot에서만 한다. 이것이 성공해도
production cancel 구현으로 자동 승격하지 않는다. 공개 AEDT stop API는 여전히 session-wide로
취급한다.

## 5. Record validation

예시 payload:

```json
{
  "baseline_desktops": 2,
  "pooled_desktops": 1,
  "baseline_projects": 2,
  "pooled_projects": 2,
  "runtime_ratio": 1.04,
  "desktop_license_delta": -1,
  "output_parity_passed": true,
  "cancellation_isolation_passed": true,
  "crash_recovery_passed": true,
  "timeout_fault_injection_passed": true,
  "sibling_completion_passed": true,
  "sibling_terminal_output_passed": true,
  "sibling_data_rows_passed": true,
  "sibling_field_solution_passed": true,
  "fault_checkout_released_after_recycle_passed": true,
  "faulted_desktop_not_reused_passed": true,
  "baseline_artifact": "/evidence/baseline.json",
  "pooled_artifact": "/evidence/pooled.json",
  "license_artifact": "/evidence/lmstat.jsonl"
}
```

```bash
curl -sS -X POST "$SCHEDULER_URL/api/aedt-pool/validations" \
  -H 'Content-Type: application/json' \
  --data @validation.json
```

응답이 `status=passed`가 아니면 활성화하지 않는다.

## 6. Gradual enablement

250/500으로 바로 시작하지 않는다. 같은 코드와 gate에서 아래 단계로 올린다.

1. max AEDT 1 / projects 2
2. max AEDT 2 / projects 4
3. max AEDT 5 / projects 10
4. max AEDT 10 / projects 20
5. 각 단계의 license/runtime/failure/requeue가 안정적일 때만 250 / 500 ceiling으로 변경

UI에는 AEDT ceiling만 입력한다. project target은 자동으로 2배가 된다.

```bash
curl -sS -X PATCH "$SCHEDULER_URL/api/aedt-pool/config" \
  -H 'Content-Type: application/json' \
  --data '{"max_aedt_sessions":250,"target_project_concurrency":500,"projects_per_aedt":2}'

curl -sS -X POST "$SCHEDULER_URL/api/aedt-pool/enable" \
  -H 'Content-Type: application/json' \
  --data '{"enabled":true}'
```

## 7. Monitoring

항상 함께 본다.

- hard-counted AEDT / ceiling
- starting/ready/busy/draining/unhealthy 분포
- live/queued/releasing leases
- requested/dedicated nodes
- host task heartbeat와 Slurm live step
- Desktop/HFSS/Maxwell/Icepak feature usage
- project별 matrix/loss/Icepak runtime와 output parity
- timeout quarantine과 sibling outcome

`unhealthy`는 license를 쓴다고 가정한다. 실제 process가 없다는 확인 없이 row를 지우거나 cap을
회수하지 않는다.

## 8. Rollback

1. 먼저 pooled backend를 disable한다.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/aedt-pool/enable" \
  -H 'Content-Type: application/json' \
  --data '{"enabled":false}'
```

2. 새 lease 유입이 멈췄는지 확인한다.
3. healthy sibling grace를 존중하며 session을 drain한다.
4. host ACK로 AEDT 종료를 확인한다. 확인되지 않은 session은 `unhealthy`로 유지한다.
5. counted session과 live host task가 모두 0인 전용 allocation만 닫는다.
6. MFT 제출 backend를 기존 standalone 경로로 유지/복귀한다.
7. pooled task 결과는 validation revision으로 격리하고, 검증 전 production training data에 섞지 않는다.

Rollback은 기존 standalone task나 현재 300개 캠페인을 취소할 권한을 의미하지 않는다.
