# 개선: AEDT 브랜치 완전 통합 + 옵션 모듈화 (2026-07-28~29)

`docs/consolidation_index_20260728.md`의 Phase 1/3/4a를 하나의 개선 기록으로 정리한다.
MFT 디커플링은 별도 문서(`docs/improvement_mft_decoupling_20260728.md`) 참조.

## Before

- "1 AEDT → N projects attach" 기능이 ~40개 브랜치(worktree로 `Y:\git\slurm_scheduler_*`,
  `C:\Users\peets\codex_worktrees\*`에 산개)에 흩어져 병렬 라인으로 갈라진 채 한 번도 통합되지
  않았다. 최신 두 라인(07-17 `codex/aedt-integration-260717` vs 07-25
  `fix/attached-task-cpu-contract-260725`)은 서로 조상 관계가 아니었다.
- `app.py`가 AEDT 풀 서비스/router/스레드를 **무조건** 생성해, 순수 스케줄러로 쓸 때도
  aedt DB 테이블이 만들어지고 pool 스레드가 떴다.
- pilot 하니스(loopback 테스트 + 라이브 스크립트)가 진화한 컨트롤 플레인(fail-closed 런타임
  attestation, protocol_version=2 lease 게이트)과 어긋나 4개 테스트가 결정적으로 실패했다.

## After

- **단일 통합 브랜치** `integration/aedt-consolidated-20260728`:
  base = `fix/attached-task-cpu-contract-260725`@`1559a7c`(모든 AEDT 작업의 patch 상위집합,
  `aedt_pool.py` +1,945/-135 우세) + 고유 keeper 5건 cherry-pick
  (`61b95c7` client-outage, `6c3df6d` refresh 기아 방지(비동기 취소 설계로 합성),
  `64d0f05`+`d30acf0` 8002 리스너 복구, `4facdfe` strict FEA demand).
  서베이 근거: `docs/aedt_branch_survey_20260728.md`.
- **옵션 모듈화**: `aedt_pool.module_enabled`(기본 false). off면 서비스/런타임/relay/router/
  테이블 미생성, `/aedt-pool`·`/api/aedt-pool` 404, pooled 제출 422 명시 거부. on이면 기존
  triple-gate(enabled/adapter_ready/validation_passed) 유지. 회귀:
  `tests/test_aedt_pool_module_flag.py`. **라이브 실증**: 2026-07-29 프라이머리 배포 기동 시
  두 라우트 404 확인.
- **pilot 하니스 v2 정합**: fakes에 attestation(GetVersion)/liveness/v2 lease 필드 보강,
  스크립트 v2 갱신. 프로덕션 모듈 무변경. 전체 스위트 885 passed / 2 skipped.
- **배포 복구**: 프라이머리(`C:\Users\peets\NEC\slurm_scheduler`, 07-13 crash loop 후 다운)를
  `5d9c2e7`로 갱신·재기동. 기존 작업 WIP 커밋(`7ba1935`)·DB 백업 보전. 크래시 원인은 코드가
  아니라 orphan worker의 8000 포트 점유 + 외부 kill이었음을 로그로 규명.

## Evidence

- 전체 pytest: **885 passed / 2 skipped / 실패 0** (`5d9c2e7`).
- 게이팅: flag off에서 aedt 라우트 404 (라이브 배포에서 확인).
- 실전 코어 경로: 재기동한 스케줄러가 6계정 SSH 헬스, warm pool 유지, priority-10000
  attached task(1:2 pilot, task 30475) 제출→allocation 8478/n108 attach를 정상 수행.
- 라이브 1:2 gate: task 30475의 `pilot_evidence.json`이 유일 판정 근거 —
  결과는 `docs/mft_aedt_attach_1to2_result_20260729.md`에 기록.

## 남은 리스크

- 구 브랜치들(worktree 포함)은 아직 정리 전 — 통합 브랜치 검증 완료 후 삭제/아카이브 권장.
- `apply_project_to_payload`가 project env_setup을 클라이언트 제공 env_setup 앞에 무조건
  prepend하여 중복 블록이 생김(30445/30475에서 관찰) — 무해하지만 idempotency 개선 여지.
- Y: RaiDrive 마운트는 부하 시 플래핑(2026-07-28 사고) — 빌드/테스트/Git/서비스는 C: 로컬에서,
  Y:는 참조 전용 (`docs/y_snapshot_sha256_20260713.md` 원칙 재확인).
