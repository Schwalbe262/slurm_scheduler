# 개선: MFT 디커플링 — 범용 Slurm 스케줄러 복원 (2026-07-28)

## 무엇이 문제였나 (before)

MFT 최적설계 캠페인(`Y:\git\MFT_1MW_2026`)을 개발하면서 스케줄러에 캠페인 전용 레이어가
섞여 들어갔다. 통합 base(`fix/attached-task-cpu-contract-260725`) 기준으로:

- 전용 모듈: `mft_pipeline_status.py`(NSGA-II lane/surrogate 상태 리더),
  `campaign_mutation_lock.py`(호스트 전역 캠페인 뮤테이션 락)
- 전용 API: `/api/mft-pipeline/status`, `/api/projects/{name}/campaign-demand`,
  `/api/projects/{name}/simulation-policy(+/validation)`
- 전용 UI: dashboard "MFT 연속 설계 파이프라인" 패널(NSGA lane 테이블, 15초 폴링),
  project_detail "Simulation campaign controls"(campaign budget, 500-active controller)
- 전용 계약: `MFT_AEDT_*` 환경변수 pooled task contract, `MFT_ACTIVE_CONCURRENCY_CEILING=500`,
  `MAX_CAMPAIGN_TOTAL_SIMULATIONS`
- 하드코딩: `config.py` 기본값에 `MFT_1MW_2026v1`/`PYAEDT_MOTOR_IPMSM_V2` 라이선스 비용 맵,
  `electronics_desktop` 예약 32, `/gpfs/tmp_cpu2/mft_pool` 경로,
  dashboard placeholder에 실제 MFT repo/entrypoint

## 무엇을 바꿨나 (after)

- 위 모듈/API/UI/테스트(`test_mft_pipeline_status.py`, `test_campaign_demand.py`) 및
  `scripts/repair_mft_cleanup_globs.py` 삭제 — 27 files, +510/−8,800.
- pooled task contract는 삭제가 아니라 **범용화**: `MFT_AEDT_*` → `SLURM_AEDT_*`
  (`SESSION_PROFILE`, `WORKLOAD_FAMILY`, `ISOLATION_POLICY`). 메커니즘 유지, 프로젝트 중립.
- `TERMINAL_AEDT_WORKSPACE_ROOT` 하드코딩 → config 노브
  `aedt_pool.terminal_workspace_root`(기본 `/gpfs/tmp_cpu2/aedt_pool`).
- 라이선스 admission 기본값을 빈 맵으로 — 동작은 오직 `config/app.yaml`에서.
- placeholder 중립화(`my_project`, `run.py`, `org/repo` 예시).
- 유지(keeper): 범용 AEDT 세션 풀/attach 전체, `control_plane_relay.py`, `fea_bursty`,
  GPU 스케줄링, Conda Env Sync, Capabilities, Projects deploy, Token Usage,
  config-gated 라이선스 모니터.

## 증거 (evidence)

- `compileall` clean; `pytest tests/ -q`: **880 passed / 3 failed / 2 skipped** —
  실패 3건은 base부터 존재한 pilot loopback 드리프트(Phase 4 대상,
  `docs/consolidation_index_20260728.md` 참조)로 이번 변경과 무관.
- grep 게이트: `slurm_scheduler/`, `templates/`, `config/app.example.yaml`에서
  `mft|nsga|campaign|MFT_AEDT_|PYAEDT_MOTOR|IPMSM` **0건**.

## 남은 리스크

- 캠페인 데모드/시뮬레이션 정책 API를 소비하던 MFT 클라이언트는 이 브랜치 배포 시 해당
  엔드포인트가 사라진다 — MFT 쪽 파이프라인은 자체 repo(캠페인 스크립트)에서 운영해야 한다.
- `SLURM_AEDT_*` 계약으로 이름이 바뀌었으므로 pooled 클라이언트 env 주입 코드는 새 이름을
  써야 한다(라이브 1:2 pilot 갱신 시 함께 반영).
