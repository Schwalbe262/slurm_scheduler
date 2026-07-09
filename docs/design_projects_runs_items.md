# 설계 제안: Project / Run / Work-Item 3층 구조

MFT 캠페인 운영(2026-07)에서 손으로 구축·검증한 패턴의 정식화 제안.
현재의 env_profile + 태스크 단발 제출 구조를 확장해, 반복 구동형 프로젝트와
강화학습(RL)류 워크로드까지 하나의 모델로 커버한다.

## 1층: Project (환경 = 코드 + 실행환경 정의)

DB 엔티티 + UI (테이블은 요약만, 상세 페이지에서 조회/수정):

| 필드 | 예 (MFT_1MW_2026v1) |
|---|---|
| name | MFT_1MW_2026v1 |
| git_url / branch | github.com/Schwalbe262/MFT_1MW_2026 / main |
| conda_env | pyaedt2026v1 |
| entrypoints | run_simulation_260706.py (복수 등록 가능) |
| prelude | module load ansys-electronics/v252; export FLEXLM_TIMEOUT=... |
| staging_path | ~/slurm_scheduler/MFT_1MW_2026v1 (계정마다 자동 클론) |
| update_policy | 태스크 시작 시 git pull, **git hash를 태스크 메타에 기록** (데이터 계보) |
| cleanup_policy | simulation/ 아래 mtime 6h+ 자동 스윕 |

## 2층: Run (실행 버튼)

- Run = project + entrypoint + args + 병렬수 N + 자원(cpus/mem) -> 태스크 N개 생성
- UI: 프로젝트 상세에서 "N개 병렬 실행" 버튼, 실행 중 카운트 실시간 표시, 정지 버튼
- **결과 일괄 수집**: 계정별로 흩어진 output(CSV/parquet 파트)을 서버가 모아
  병합 다운로드 (프로젝트에 output glob 패턴 등록, 예: simulation_results_*.csv, results_parts/*.parquet)

## 3층: Work-Item 큐 (RL/능동학습 대응의 핵심)

단순 병렬(N개 동일 실행)과 RL의 차이는 "다음 일감을 누가 정하는가"뿐이다.
프로젝트별 일감 큐를 추가하면 둘 다 같은 구조로 처리된다:

```
POST /projects/{id}/items        {"params": {...}, "priority": 5}    # 트레이너가 일감 투입
GET  /projects/{id}/items/claim  (워커가 원자적으로 집어감)
POST /projects/{id}/results      {"item_id": ..., "data": {...}}     # 결과 보고
GET  /projects/{id}/results?since=...                                # 트레이너가 수거
```

- **워커 모드 태스크**: 장수명 태스크가 loop { claim -> 실행 -> 결과 POST }.
  AEDT 같은 무거운 툴을 켠 채 유지 -> 일감당 기동비용(데스크톱 1-3분 + 라이선스
  체크아웃 폭풍) 제거. 짧은 평가를 수천 번 하는 RL에서 필수.
- 단순 캠페인 = "일감을 워커가 스스로 생성(랜덤 샘플)"하는 특수 케이스
- RL/능동학습 = 외부 트레이너가 설정이 다른 일감을 계속 투입하는 케이스
  (MFT AL 루프에서 --params JSON 인라인 제출로 이미 실전 검증된 패턴)

## 운영에서 얻은 필수 요구사항 (2026-07 실측)

1. **취소 = 프로세스 트리 전체 회수** (docs/bug_packed_cancel_leaks_children.md)
   - 미회수 시 노드당 AEDT 44-86세션 잔존, load 231%, 라이선스 수백 시트 점유 실측
2. **노드 CPU 캡 기본 활성** (fea_node_requested_cpu_factor=1.0) - 동적 패킹 폭주 방지
3. **고유 실행 ID 주입 유지** (SIMULATION_ID) - 공유 폴더 동시 실행의 전제
4. 결과 스트리밍 관례: 워커 stdout에 `RESULT_JSON {...}` 한 줄 = 일감 1개 결과
   (태스크 완주를 기다리지 않는 회수 - 3층 큐 도입 전의 브리지로도 유효)
