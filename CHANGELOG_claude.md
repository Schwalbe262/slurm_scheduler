
## 2026-07-07 (MFT 캠페인)
- GET /api/tasks 에 limit(기본 200, 최대 10000) / name_prefix 쿼리 파라미터 추가
  - 이유: 400+ 태스크 캠페인 집계·결과 회수 시 200개 페이지 제한으로 누락 발생
  - 주의: 실행 중인 서비스에는 재시작 후 반영됨 (캠페인 웨이브 사이 안전 시점에 재시작 권장)

## 2026-07-09 (Claude)
- docs/env_profile_MFT_1MW_2026v1.md: MFT 캠페인용 env_profile 추가 요청 문서 (accounts.yaml 반영 + 재시작 필요)

## 2026-07-09 (Claude) - 추가
- docs/bug_packed_cancel_leaks_children.md: 태스크 취소가 AEDT 자식 트리를 회수하지 못하는 버그 리포트 (노드당 세션 44-86개 잔존 실측)

## 2026-07-09 (Claude) - 추가 2
- docs/design_projects_runs_items.md: Project/Run/Work-Item 3층 구조 설계 제안 (RL 대응 포함)

## 2026-07-09 (Claude) - 추가 3
- slurm.py shell_path: $HOME/ 프리픽스 경로가 통째로 quote되어 project run의 cd가 실패하던 버그 수정 (재시작 필요)

## 2026-07-10 (Codex) - MFT shared cleanup repair
- `scripts/repair_mft_cleanup_globs.py`: scheduler tick 사이의 휴지 구간에서 온라인 DB 백업과
  무결성 검사를 수행한 뒤, MFT active task/project의 공유 `simulation` cleanup glob을
  `*.aedtresults`로 원자 교체하는 guarded maintenance 도구 추가.
- 2026-07-10 05:48 KST 적용: active destructive rows 325 -> 0. 백업은
  `data/backups/manual-pre-cleanup-glob-20260710-054839.db`.

## 2026-07-10 (Codex) - MFT jji0930 capability repair
- Git에서 제외되는 운영 `config/accounts.yaml`의 `jji0930` 계정에 기존
  `pyaedt2026v1` 환경과 일치하는 `conda:pyaedt2026v1` capability를 추가.
- 설정 파싱과 scheduler 테스트 255개를 통과한 뒤 native worker만 재시작했다.
  기존 비-b630 태스크 16개와 allocation 4개는 보존됐고, placement dry-run에서
  `jji0930`이 eligible로 확인되어 대기 중이던 태스크 6개가 새 64 CPU pool에 연결됐다.
