# 중앙 AEDT 풀 사용 안내 (타 프로젝트용)

스케줄러가 공유 AEDT Desktop 풀을 직접 관리합니다. 프로젝트는 자기 AEDT를 여는 대신
풀에서 **lease**를 받아 기존 Desktop에 **attach**해서 씁니다. AEDT 1개당 최대 3개
프로젝트가 동시에 붙고, Desktop 라이선스는 세션(풀)이 소유하므로 클라이언트는
electronics_desktop 좌석을 추가로 소모하지 않습니다 (admission에서 자동 면제).

- 풀 API (클러스터 노드에서 접근): `http://172.16.10.37:18790` (로그인 노드 릴레이)
- 풀 현황 UI: 스케줄러 웹 `/aedt-pool`
- 세션 상한 250 / 동시 프로젝트 750 / AEDT당 3 (2026-07-14 기준)

## 준비물 (6개 공용 계정에 이미 배포됨)

| 항목 | 경로 (각 계정 홈 기준) |
|---|---|
| 클라이언트 라이브러리 | `~/slurm_scheduler/aedt_pool_pkg` (slurm_scheduler 체크아웃, `aedt_attach_client` 포함) |
| 부트스트랩 토큰 | `~/slurm_scheduler/aedt_pool_bootstrap` (600, lease 발급 인증용) |

## 태스크 제출

`POST /api/tasks`(또는 프로젝트 entrypoint 제출)에 `"aedt_backend": "pooled"`를 넣으면
풀이 operational일 때만 admit됩니다. 클라이언트 프로세스는 얇은 드라이버(솔브는 풀
노드의 Desktop 프로세스에서 실행)이므로 **cpus 1~2, memory 4~6GB**면 충분합니다.

## 러너 코드 통합 (핵심 5줄)

```python
import sys, os
sys.path.insert(0, os.path.expanduser("~/slurm_scheduler/aedt_pool_pkg"))
from slurm_scheduler.aedt_attach_client import acquire_project_lease

lease = acquire_project_lease(
    "http://172.16.10.37:18790",            # 풀 API (릴레이)
    "my-project-run-001",                    # 이 실행의 프로젝트 이름 (고유하게)
    bootstrap_token_file=os.path.expanduser("~/slurm_scheduler/aedt_pool_bootstrap"),
    task_id=int(os.environ.get("SLURM_SCHEDULER_TASK_ID", "0") or 0),
)
lease.wait_until_leased(timeout_seconds=1800)   # 빈 슬롯 배정 대기 (풀이 세션 자동 증설)
lease.start_heartbeat()
desktop = lease.connect_desktop()               # 공유 Desktop에 gRPC attach

# --- 여기서 pyaedt로 평소처럼 작업 (프로젝트 생성/솔브/추출) ---
# 프로젝트를 만들면 lease.bind_project_name(실제_프로젝트명) 호출 권장

lease.release(wait_seconds=300)                 # 종료: 반드시 release (close ACK까지 확인됨)
```

실패 시: `lease.report_fault("script_error", failure_message=...)` 호출 후 종료.

## 절대 규칙 (어기면 다른 프로젝트 2개가 같이 죽습니다)

1. **Desktop을 절대 닫지 마세요** — `Desktop(close_on_exit=True)` 금지,
   `release_desktop(close_desktop=True)` 금지, `odesktop.QuitApplication()` 금지.
   Desktop 종료 권한은 풀 세션 host에만 있습니다.
2. 자기 프로젝트만 만지세요 — 같은 Desktop에 다른 프로젝트 2개가 열려 있을 수 있습니다.
3. 종료 시 반드시 `lease.release()` — 안 하면 heartbeat 만료까지 슬롯이 잠깁니다.
4. 솔브 코어 수 계약: 풀 노드는 프로젝트당 4코어 기준으로 사이징되어 있습니다.
   솔버 설정에서 4코어를 초과하지 마세요 (초과가 필요하면 풀 운영자와 조정).

## 참고 구현

- 실전 어댑터 패턴: MFT의 `module/aedt_pool_adapter.py` (env 기반 opt-in, 표준/풀 모드 겸용)
- 최소 E2E 예제: `scripts/aedt_pool_central_pilot.py` (lease→attach→작업→release+증거 JSON)
- 클라이언트 라이브러리 전체: `slurm_scheduler/aedt_attach_client.py`
