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

## 시도 (attempts)

- (추가 예정)

## 개선 (improvements)

- (추가 예정)
