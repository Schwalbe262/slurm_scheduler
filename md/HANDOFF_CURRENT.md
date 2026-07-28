# Current Handoff

## Current status

- 2026-07-28: 순수 스케줄러 복원 + AEDT 통합 작업 시작 (`docs/consolidation_index_20260728.md`).
- 기준: `experiment/aedt-attach-1to2-20260713` @ `620a301`, 백업 태그 `backup/pre-consolidation-20260728`.
- 베이스라인: `tests.test_core` 342 OK, `tests/test_aedt_pool*` 37 passed, compileall clean.

## Current objective

- Phase 1: AEDT 브랜치 전수 서베이 → base head + cherry-pick 리스트 확정 (`docs/aedt_branch_survey_20260728.md`).
- Phase 2: MFT 디커플링 (config.py/scheduler.py/app.example.yaml/dashboard.html의 MFT 하드코딩 제거).

## Active branch / part

- Branch: `experiment/aedt-attach-1to2-20260713` → `integration/aedt-consolidated-20260728` (예정)
- Part: consolidation Phase 0-1

## Token/context policy

- Start from this file. Do not read `note.md`/`insight.md` in full. Targeted `rg` only.
- Update this file in 10 lines or fewer at closeout.

## Risks and gotchas

- MFT 캠페인 전용 요소(`mft_pipeline_status`, `campaign_mutation_lock`, `/api/mft-pipeline`,
  `MFT_AEDT_*` contract)는 07-17 브랜치 통합 시 배제할 것.
- AEDT 풀은 `enabled=0`/`adapter_ready=0` 유지; 라이브 1:2 gate 통과 전 활성화 금지.
- 다른 worktree가 체크아웃한 브랜치는 checkout 불가 — merge/cherry-pick은 통합 브랜치에서.
