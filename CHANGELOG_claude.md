
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
