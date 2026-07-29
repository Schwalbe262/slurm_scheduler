# 2026-07-28 순수 스케줄러 복원 + AEDT 통합 — 인덱스

이 문서는 2026-07-28 시작된 "MFT 디커플링 + AEDT 브랜치 통합 + 1:2 attach 검증" 작업의
살아있는 인덱스다. 각 시도/개선 문서가 생기면 여기에 한 줄씩 추가한다.

## 목표

1. MFT 전용 코드를 제거해 범용 Slurm 스케줄러 복원 (AEDT 풀은 flag-gated 옵션 모듈로).
2. ~40개 브랜치에 흩어진 AEDT 1:N attach 작업을 하나의 통합 브랜치로 완전 통합
   (MFT 캠페인 전용 요소는 배제).
3. 1 AEDT : 2 project attach의 동작 검증 — 모의 → 라이브 pilot (`pilot_evidence.json`).

## 기준점 (Phase 0, 2026-07-28)

- 기준 브랜치: `experiment/aedt-attach-1to2-20260713` @ `620a301`
- 백업 태그: `backup/pre-consolidation-20260728`
- 베이스라인 테스트: `tests.test_core` 342 OK,
  `tests/test_aedt_pool*` 37 passed (pytest), `compileall` clean.
- 통합 브랜치(예정): `integration/aedt-consolidated-20260728` (Phase 1 서베이로 base 확정 후)

## Phase 1 결과 (2026-07-28)

- 서베이: `docs/aedt_branch_survey_20260728.md` — base = `fix/attached-task-cpu-contract-260725`
  @ `1559a7c`, cherry-pick 5건(`61b95c7`,`6c3df6d`,`64d0f05`,`d30acf0`,`4facdfe`).
- 통합 브랜치 재구성 완료. 전체 스위트 924 passed / 4 failed →
  `test_task_refresh_fairness` 1건은 base의 비동기 취소 설계에 맞춰 테스트 갱신으로 해결.
- **알려진 실패(Phase 4 대상, base 자체 결함):** pilot loopback 3건 —
  `test_aedt_pool_1to1_pilot::test_loopback_pilot_performs_exclusive_attach_and_close_ack`,
  `test_aedt_pool_1to2_pilot::test_shared_loopback_closes_aborted_project_without_stopping_sibling`,
  `test_aedt_pool_1to2_pilot::test_shared_loopback_timeout_quarantines_then_recycles_after_sibling`.
  증상: `state.session["endpoint"]`가 빈 문자열(세션 호스트 endpoint 등록 실패). 순수
  `1559a7c`에서도 동일 재현 — 260725 라인의 aedt_pool.py 진화가 pilot 프로토콜과 어긋남.

## Phase 2 결과 (2026-07-28)

- MFT 캠페인 레이어 전체 제거 — `docs/improvement_mft_decoupling_20260728.md` 참조.
  커밋 `ab23569`. 880 passed, MFT 토큰 grep 0건.

## Phase 3 결과 (2026-07-28~29)

- `aedt_pool.module_enabled` feature flag(기본 false) 도입: flag off면 AEDT 풀
  서비스/런타임/relay/router가 아예 생성되지 않고(`app.py` None-게이트), aedt DB 테이블도
  만들어지지 않으며, pooled task 제출은 422 "AEDT pooled backend module is disabled"로
  명시 거부. flag on이면 기존 triple-gate(enabled/adapter_ready/validation_passed) 그대로.
- 회귀 테스트: `tests/test_aedt_pool_module_flag.py` (전부 통과).
- 주의: Y: RaiDrive 마운트 사고(2026-07-28 밤)로 작업이 중단됐다가
  `2e30e8d`(salvage 커밋)로 보전됨. 이후 정식 작업본을 `C:\Users\peets\work\slurm_scheduler`
  클론으로 전환(Y:는 참조용) — `docs/y_snapshot_sha256_20260713.md`의 교훈 재확인.

## Phase 4a 결과 (2026-07-29)

- pilot loopback 실패 4건(위 3건 + salvage 후 `rejects_exclusive_or_third_lease`)의 근본
  원인: **전부 harness 드리프트, 프로덕션 결함 아님** — base 라인이 fail-closed 런타임
  attestation(`_attest_runtime_profile`, Desktop `GetVersion` 요구)과 protocol_version=2
  lease 게이트를 도입했는데 pilot 스크립트/테스트가 구식이었음 (원인 커밋 `3702677`,
  `ef81118` — 통합 이전부터 존재).
- 수리: pilot 스크립트는 v2 프로토콜로 갱신(salvage분), 테스트 fakes에 attestation/liveness/
  v2 lease 필드 보강. 프로덕션 모듈 무변경. 집중 스위트 303 passed.

## 시도 (attempts)

- 2026-07-28 Phase 1: 40개 브랜치 서베이 → base 확정 (`docs/aedt_branch_survey_20260728.md`)
- 2026-07-28 Phase 2: MFT 디커플링 (`docs/improvement_mft_decoupling_20260728.md`)
- 2026-07-28~29 Phase 3/4a: 모듈 게이팅 + pilot v2 수리 (Y: 사고로 salvage `2e30e8d` 경유)

## Phase 4b (라이브 1:2 pilot, 2026-07-29)

- 프라이머리 배포(`C:\Users\peets\NEC\slurm_scheduler`, 07-13부터 다운)를 `5d9c2e7`로
  갱신·재기동 (기존 작업 WIP `7ba1935` + DB 백업 보전). 기동 시 module flag off →
  `/aedt-pool` 404 게이팅 실증.
- pilot task **30475** 제출 (`mft-aedt-1to2-pilot-20260729-013829`): 30445 payload 미러 +
  scheduler SHA `5d9c2e7` 핀. MFT `fd3b02c2`, library `e6b9b9d2` (30445와 동일 조합).
  r1jae262 / allocation 8478 / n108 / Slurm 848062에서 실행.
- 판정 근거는 `pilot_evidence.json` 단독 — 결과는
  `docs/mft_aedt_attach_1to2_result_20260729.md`에 기록 예정.
- 발견: form `/tasks` 엔드포인트가 `project`/`timeout_seconds`/`dedupe_key`를 무시(JSON
  `/api/tasks`는 정상) — 개선 후보. `apply_project_to_payload`의 env_setup 중복 prepend도
  개선 후보.

## 개선 (improvements)

- MFT 디커플링: `docs/improvement_mft_decoupling_20260728.md` (+ insight.md 항목)
- AEDT 통합 + 옵션 모듈화: `docs/improvement_aedt_consolidation_20260729.md`
