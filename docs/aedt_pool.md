# Experimental AEDT Session Pool

> **Status (2026-07-13): experimental, disabled, not production-approved.**
> The requested `250 AEDT / 500 projects` topology is staged as a default
> ceiling and derived capacity only.  It does not open a Slurm allocation or
> AEDT process until the host adapter is configured and the mandatory live A/B
> validation passes.

> **Source-of-truth note:** build and test this experiment in a dedicated clean
> clone, never in the dirty live scheduler tree.  Deployment must use an
> identified GitHub branch and exact commit SHA on cluster-local storage.

> **Current integration gate:** exclusive 1:1 passed in task 30089. Shared 1:2
> remains an isolated pilot described in
> [MFT shared AEDT 1:2 pilot](mft_aedt_attach_1to2.md). It does not authorize
> the 250/500 target until its real evidence passes and is reviewed.

## 목적과 범위

기존 backend는 프로젝트 한 개가 AEDT Desktop 한 개를 열고 소유한다. 새 pooled backend는
Desktop 라이선스가 먼저 포화되는 환경에서 AEDT 하나에 최대 두 프로젝트를 붙이는 opt-in
경로다. 기존 standalone task와 현재 캠페인은 그대로 유지한다.

WEB UI에서 사용자는 AEDT session 상한, 전체 project 병렬 상한,
`projects_per_aedt`를 각각 설정한다. project 상한은
`max_aedt_sessions * projects_per_aedt`를 넘을 수 없으며 slot 수 변경은 pool이
disabled이고 완전히 drain된 경우에만 허용된다.

```text
max_aedt_sessions = 250
projects_per_aedt = 2 (validation contract)
target_project_concurrency = 500
```

250은 미리 열어 두는 수가 아니라 hard ceiling이다. 다음 상태는 모두 ceiling을 소비한다.

- `starting`
- `ready`
- `busy`
- `draining`
- `unhealthy`

따라서 heartbeat가 끊긴 Desktop을 단순히 숫자에서 빼고 새 Desktop을 여는 license overshoot가
발생하지 않는다. host가 실제 종료를 확인해야 `closed` 또는 `failed`로 빠진다.

## 구조

```text
WEB UI: max AEDT ceiling
  -> AedtPoolRuntime: demand/reconcile and dedicated-node requests
    -> Slurm dedicated allocation (reason starts with "AEDT pool")
      -> aedt_session_host: exactly one AEDT owner and lifecycle authority
        -> up to two aedt_attach_client project leases
```

구현은 다음처럼 분리된다.

| 구성요소 | 파일 | 소유권 |
|---|---|---|
| control plane | `slurm_scheduler/aedt_pool.py` | session/lease 상태, cap, scaling, recovery |
| HTTP/UI | `slurm_scheduler/aedt_pool_api.py`, `templates/aedt_pool.html` | operator limit, host/client protocol |
| session host | `slurm_scheduler/aedt_session_host.py` | AEDT PID, gRPC endpoint, project close, Desktop recycle |
| attach client | `slurm_scheduler/aedt_attach_client.py` | 자기 project lease와 heartbeat만 소유 |
| allocation safety | `db.py`, `scheduler.py` | counted session이 있는 allocation의 일반 scale-in 차단 |

프로젝트 client는 shared Desktop을 종료하면 안 된다. `Desktop(..., close_on_exit=False)`로
attach하고 `release_desktop(close_desktop=True)`를 호출하지 않는다. 종료는 lease release를
요청하고 session host가 해당 프로젝트를 닫은 뒤 확인하는 2단계 방식이다.

## 동적 배치

1. 프로젝트가 lease를 요청하면 먼저 `queued`가 된다.
2. 기존 `ready/busy` AEDT에 같은 node/allocation 조건을 만족하는 빈 slot이 있으면 붙인다.
3. slot이 없으면 현재 demand에서 필요한 AEDT 수를 계산한다.
4. 전용 allocation에 공간이 있으면 `starting` session claim을 만든다.
5. 전용 allocation이 부족하면 일반 scheduler의 full-node shape로 node를 요청한다.
6. node가 `warm/active`가 되면 session-host task를 그 allocation에 정확히 reserve하고 `srun`으로
   실행한다.
7. demand가 줄거나 cap을 내리면 새 lease를 막고 idle session부터 `draining`한다.
8. 모든 session과 host task가 끝난 빈 전용 allocation만 닫는다.

Validation/A-B task는 API의 기존 `priority` 필드에 10000을 명시한다. priority는 queued attach
ordering에만 관여하며 running production task를 선점하거나 취소하지 않는다.

전용 allocation만 사용한다. `drain_reason`이 `AEDT pool`로 시작하지 않는 기존 FEA/MFT
allocation에는 session을 배치하지 않는다.

node shape는 `requested_cpus=8` 같은 micro-allocation을 만들지 않고 scheduler의 기존
`allocation_cpus`(현재 정책상 보통 full node)를 요청한다. 기본 CPU factor는 `1.0`이다.
프로젝트가 4 CPU, AEDT당 프로젝트가 2개라면 64 CPU node의 baseline은 8 AEDT/16 projects다.
factor는 최대 `2.0`으로 제한되며, 2x 사용은 별도 실측 validation 이후에만 허용해야 한다.
실제 project task는 기존 `fea_bursty` CPU/load/memory admission을 계속 통과해야 한다.

## 상태와 recovery

### Session

```text
starting -> ready -> busy -> ready
                    |        |
                    +-> draining -> closed
                    +-> unhealthy -> failed/closed
```

- `starting`: cap을 먼저 예약한다. host start ACK timeout도 자동으로 cap에서 제거하지 않고
  `unhealthy`로 보낸다.
- `ready`: lease가 0개다.
- `busy`: lease가 1~2개다.
- `draining`: 새 lease를 받지 않는다. cap 하향, idle scale-in, timeout quarantine에 사용한다.
- `unhealthy`: heartbeat가 끊겼지만 Desktop 종료가 확인되지 않은 fail-safe 상태다.

개별 project/session 실패는 해당 lease/session/allocation만 quarantine, drain, requeue한다. 최근
성공률이나 개별 실패 때문에 전역 pool을 자동 disable하는 정책은 없다. 전역 disable은 operator의
명시적 API/UI 행위이며, 최초 enable proof gate와는 별개다.

### Lease

```text
queued -> leased -> active -> releasing -> released
   |                   |          |
 expired          quarantine    failed
```

- queued client도 heartbeat해야 한다. node queue가 길어져도 정상 request가 180초 뒤 사라지지
  않도록 `aedt_attach_client.wait_until_leased()`가 주기적으로 갱신한다.
- release는 client 요청과 host close ACK의 2단계다. `releasing` slot은 재사용하지 않는다.
- AEDT/gRPC가 죽으면 같은 session의 모든 sibling lease를 slot에서 분리해 `queued`로 되돌린다.
  각 attempt는 독립 output workspace/commit 규칙을 사용해야 중복 결과를 피할 수 있다.

## `StopSimulations`의 session-wide 제약

PyAEDT 0.22의 공개 `stop_simulations` 경로는 `oDesktop.StopSimulations(...)`를 사용한다.
이는 특정 프로젝트나 solver만 중지하는 안전한 API로 간주할 수 없고, 같은 AEDT의 sibling
solve도 영향을 받을 수 있다.

그래서 production 정책은 다음과 같다.

1. pre-solve/script error: 해당 프로젝트만 close/release한다.
2. solver timeout: session을 즉시 `draining` + quarantine하여 새 lease를 차단한다.
3. 건강한 sibling에는 기본 15분 grace를 준다.
4. sibling이 끝나거나 grace가 만료된 뒤에만 host가 global stop을 사용할 수 있다.
5. fault owner A는 project-close ACK로 released 처리하지 않는다. PID가 죽어도 solver checkout과
   `AreThereSimulationsRunning=true`가 남을 수 있으므로 `releasing`에 둔다.
6. 이후 Desktop 전체와 전용 allocation을 drain/recycle하고 A lease를 `queued`로 되돌린다.
7. quarantine된 Desktop은 어떤 경우에도 새 lease에 재사용하지 않는다. AEDT death면 sibling
   lease도 전부 requeue한다.

2026-07-13 isolated pilot `732549`에서 한 Desktop의 A/B 동시 solver 중 A PID만 SIGTERM한 뒤
B PID와 gRPC가 생존하는 것은 확인했지만, A의 license checkout과 Desktop running flag가
잔존했다. 최종 감사에서 B도 `Completed N/A`, data row 0, field solution 없음으로 확인됐다.
따라서 이 pilot은 **FAIL**이며 정확한 PID kill이나 gRPC 생존만으로 project-local cancellation이
증명된 것이 아니다. terminal output, data row, field solution, recycle 후 checkout 반환을 모두
별도 evidence로 통과하기 전에는 adapter/pool을 활성화하지 않는다. 현재
`adapter_ready=0`, `enabled=0`이며 250/500 적용은 금지다.

이 사례를 false-positive reopen/liveness probe로 회귀 테스트한다. process/port가 다시 보이거나
gRPC 호출이 성공해도 terminal artifact 세 항목 중 하나라도 없으면 validation status는 `failed`다.

프로젝트별 solver PID를 정확히 골라 죽이는 방법은 production 가정이 아니다. 별도 명시적
fault-injection 실험에서만 시험하며, sibling completion evidence가 없으면 pooled backend는
활성화되지 않는다. blast radius는 `projects_per_aedt=2`로 고정한다.

## 데이터베이스

optional schema는 서비스 시작 때 additive migration으로 생성된다.

- `aedt_sessions`: allocation/node, endpoint/PID, slot 수, state, heartbeat, quarantine/drain 정보
- `aedt_project_leases`: request/task, placement constraint, session/slot, client heartbeat와 expiry
- `aedt_pool_validations`: license/runtime/parity/fault/cancel/crash A/B evidence
- `scheduler_settings`: enabled, adapter-ready, max sessions와 내부 policy 값

live slot에는 partial unique index `(session_id, slot_index)`가 있어 concurrent reconcile에서도
두 프로젝트가 같은 slot을 얻지 못한다.

## API와 UI

Operator:

- `GET /aedt-pool`
- `GET /api/aedt-pool`
- `PATCH /api/aedt-pool/config` — session/project/slot 상한을 durable하게 저장
- `POST /api/aedt-pool/reconcile?dry_run=true`
- `POST /api/aedt-pool/enable`
- `POST /api/aedt-pool/validations`

Project lease:

- `POST /api/aedt-pool/leases`
- `GET /api/aedt-pool/leases/{id}`
- `POST /api/aedt-pool/leases/{id}/heartbeat`
- `PATCH /api/aedt-pool/leases/{id}/project-name`
- `POST /api/aedt-pool/leases/{id}/release`
- `POST /api/aedt-pool/leases/{id}/fault`

Session host:

- `POST /api/aedt-pool/hosts/claim-start`
- `POST /api/aedt-pool/sessions/{id}/register`
- `POST /api/aedt-pool/sessions/{id}/heartbeat`
- `GET /api/aedt-pool/sessions/{id}/commands`
- `POST /api/aedt-pool/sessions/{id}/leases/{lease_id}/release-complete`
- `POST /api/aedt-pool/sessions/{id}/closed`

bootstrap/host/client token은 각각 다른 header를 사용한다. bootstrap secret은 command line이나
DB에 평문으로 넣지 않고 scheduler 환경 변수와 compute node의 권한 제한 파일로 제공한다.

## Enablement gate

아래가 모두 true가 아니면 `enabled=true` 요청은 HTTP 409로 거부된다.

- session-host adapter config와 bootstrap secret이 준비됨
- baseline: 2 AEDT / 2 projects
- treatment: 1 AEDT / 2 projects
- treatment의 Desktop checkout이 최소 1 감소
- pooled runtime이 baseline의 1.20배 이하
- output parity 통과
- solver/license isolation 관찰 통과
- 정상 project cancellation isolation 통과
- AEDT crash recovery/requeue 통과
- solver timeout fault injection 통과
- timeout sibling completion 통과
- sibling terminal output 생성 통과
- sibling data row 생성 통과
- sibling field solution 존재 통과
- recycle 뒤 faulted solver checkout 반환 통과
- faulted/quarantined Desktop 무재사용 통과
- baseline/pooled/lmstat artifact 경로 기록

자세한 절차와 rollback은 [AEDT pool runbook](aedt_pool_runbook.md)을 따른다.

## MFT / pyaedt_library integration audit

현재 `MFT_1MW_2026/run_simulation_260706.py`의 production 경로는 pooled mode와 의도적으로
연결하지 않았다.

- `_create_simulation_session()`은 `pyDesktop(new_desktop=True, close_on_exit=True)`를 호출한다.
- `run_one_loop()` finally는 `release_desktop(close_projects=True, close_on_exit=True)`를 호출한다.
- 같은 finally의 descendant cleanup은 해당 Python이 만든 AEDT/solver tree를 정리한다.

이 세 동작은 shared Desktop에서는 sibling을 종료할 수 있으므로 그대로 재사용할 수 없다.
반면 `pyaedt_library/src/pyaedt_module/core/pydesktop.py`는 이미 `machine`, `port`,
`new_desktop=False`, `close_on_exit=False` 인자를 전달할 수 있고, `pydesign.py`도 주입된
Desktop의 PID/port/machine을 solver class에 넘긴다. 따라서 pilot integration cut은 다음이다.

1. `MFT_AEDT_BACKEND=pooled`일 때만 `acquire_project_lease()`를 호출한다.
2. `lease.connect_desktop(desktop_factory=pyDesktop)`로 기존 wrapper를 remote port에 붙인다.
3. 실제 `sim.PROJECT_NAME` 생성 후 `lease.bind_project_name()`을 호출한다.
4. pooled finally에서는 Desktop release/kill/descendant tree cleanup을 하지 않는다.
5. `sim.close_project()` 완료 후 `lease.release()`만 호출하고 host ACK를 기다린다.
6. solver timeout은 client `report_fault("solver_timeout")`으로 quarantine을 요청한다.

이 branch를 production script에 넣는 작업은 isolated 1:2 pilot이 통과한 뒤 별도 revision/feature
flag로 진행한다. 현재 300개 standalone 캠페인에는 import나 behavior change가 없다.
