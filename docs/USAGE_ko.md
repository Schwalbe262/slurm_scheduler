# Slurm Scheduler 사용·운영 가이드

이 서비스는 Slurm allocation을 묶어 유지하고 그 안에 여러 작업을 step으로 붙입니다.
프로젝트의 시뮬레이션 로직과 결과 데이터는 스케줄러가 수정하지 않습니다.

## 시작 전 확인

1. `config/app.example.yaml`, `config/accounts.example.yaml`을 참고해 Git에서 제외된
   `config/app.yaml`, `config/accounts.yaml`을 준비합니다. 계정 SSH 인증, 원격 작업
   디렉터리, Slurm partition, CPU·GPU 할당량을 실제 환경에 맞게 설정합니다.
2. 해당 클러스터를 관리 중인 스케줄러가 이미 실행 중이면 **두 번째 인스턴스를
   시작하지 않습니다**. 같은 계정·allocation을 두 프로세스가 동시에 관리할 수
   있습니다. 현재 서비스의 `/api/health`를 먼저 확인합니다.
3. 새 설치에서만 `SLURM_SCHEDULER_CONFIG`를 설정하고 `python -m slurm_scheduler`로
   시작합니다. 운영 교체는 DB 백업, 설정 및 스키마 호환성 확인, 한 세대씩 교체,
   롤백 경로 확보 후 수행합니다. UI에는 자체 로그인 기능이 없으므로 접근을
   사설망이나 인증된 프록시로 제한합니다.

실행 중인 스케줄러의 상태를 별도 포트에서 보기만 하려면 **운영 DB의 일관된
로컬 복사본**을 만들고 `observer_mode: true`를 설정합니다. 이 모드는 DB를
읽기 전용으로 열고 스케줄러 루프를 시작하지 않으며, 변경 HTTP 요청을 403으로
막습니다. 복사본을 새로 만들기 전까지 화면은 마지막 스냅샷 상태를 보여줍니다.
운영 중인 DB를 직접 가리키지 않습니다. 설정 예시는 [CONFIG.md](CONFIG.md)에
있습니다.

## 작업 제출

대시보드의 작업 생성 폼에서 원격 디렉터리와 명령을 직접 입력하거나, 등록된
프로젝트와 entrypoint를 선택합니다. 프로젝트를 고르면 저장된 원격 경로와
환경 설정이 적용됩니다. `dedupe_key`를 사용하면 같은 요청의 중복 제출을
막을 수 있습니다. 명령과 자원 요구량은 제출 전 `/api/task-capacity`의 dry-run
결과와 비교하세요.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"name":"example-run-001","remote_cwd":"/work/example", \
       "command":"python run.py --case 001","cpus":4,"memory_mb":8192, \
       "scheduling_profile":"standard","dedupe_key":"example-run-001"}'
```

GPU 작업에는 `gpus`, `gpu_model`, `partition`을 추가합니다. `fea_bursty`는
CPU·메모리 사용이 변동하는 작업이 정책을 이해하고 명시적으로 선택할 때만
사용합니다. 일정한 자원 격리가 필요하면 `standard`를 사용합니다.

## 진행 상황과 문제 확인

- 대시보드와 `/api/health`에서 스케줄러 상태를 확인합니다.
- `/api/inventory/freshness`의 `stale`이 참이면 노드 용량 판단에 쓰인
  inventory 또는 `pestat` 스냅샷이 오래됐거나 없습니다. 새 작업을 대량으로
  제출하기 전 수집 상태를 확인합니다.
- `/api/tasks/{id}`에서 작업 상태와 로그 위치를 확인합니다. `queued`는
  적합한 allocation을 기다리는 상태이고, `attaching`은 Slurm step을 시작하는
  중입니다. `running` 이후 종료 코드 0이면 `completed`입니다.
- 취소가 필요하면 **이번 운영자가 제출해 ID를 기록한 작업만** 취소합니다.
  다른 프로젝트의 task·Slurm job·allocation은 건드리지 않습니다.

## AEDT 한 Desktop에 여러 프로젝트 실행

AEDT pool 모듈은 기본적으로 꺼져 있습니다. 일반 작업은 standalone으로
실행됩니다. 공유 Desktop은 프로젝트별 lease와 고립된 작업 디렉터리를 쓰며,
프로젝트 하나의 종료가 다른 프로젝트나 Desktop 전체를 종료해서는 안 됩니다.
`projects_per_aedt=N`을 올리려면 **그 N에 대한** 실제 AEDT 출력·데이터 행·
field solution, 장애 격리, 소요 시간, 라이선스 checkout 비교를 먼저 통과해야
합니다. 로컬 loopback 통과만으로 운영 풀을 켜지 않습니다. 절차는
[AEDT 검증·롤백 가이드](aedt_pool_runbook.md)에 있습니다.
실제 라이선스와 출력 비교에 사용할 독립 모델 절차는
[범용 AEDT 1:N 파일럿](aedt_pool_live_pilot.md)에 있습니다.

## 변경 배포와 복구

변경 전에는 실행 중인 서비스의 commit, 설정, DB 백업, 현재 task·allocation
상태를 기록합니다. 새 코드를 복사 DB와 설정에서 먼저 검증합니다. 스키마나
기능이 다르면 운영 서비스를 유지하고 호환성 수정 후 다시 검증합니다. 배포
중에는 기존 작업을 일괄 취소하지 않습니다. 문제가 생기면 새 제출과 새
pool admission을 중지하고, 기존 작업을 drain한 뒤 백업과 이전 commit으로
돌립니다. 자세한 설정 키는 [설정 문서](CONFIG.md), API는 [API 문서](API.md)를
참고하세요.
