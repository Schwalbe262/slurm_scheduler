# 버그 리포트: packed 태스크 취소 시 자식 프로세스 트리 잔존

## 증상 (2026-07-09 실측, MFT 캠페인)

- 태스크 취소(POST /tasks/{id}/cancel) 후 상태는 cancelled로 바뀌지만,
  해당 태스크가 띄운 **ansysedt/3dedy 프로세스 트리가 노드에 살아남음**
- 잔존 프로세스의 부모는 init(1)이 아니라 **러너 프로세스** — 즉 러너가
  태스크의 python만 종료하고 손자(AEDT 데스크톱/솔버)는 회수하지 않음
- 결과: 노드당 ansysedt 44~86개 누적, load 90~108 (64코어 노드),
  owned FEA CPU 231% — 신규 solve 전면 저속화 + 라이선스 시트 수백 개 점유

## 재현

1. AEDT를 자식으로 띄우는 태스크(pyaedt) 다수를 fea_bursty로 실행
2. 실행 중 태스크를 cancel
3. 노드에서 `pgrep ansysedt` → 세션 잔존, `ps -o ppid=` → 러너 pid

## 제안

- 태스크 취소/종료 시 **프로세스 그룹/세션 단위 kill** (setsid 후 killpg,
  또는 cgroup 사용) — python만이 아니라 트리 전체 회수
- 러너가 주기적으로 "내 자식 중 활성 태스크에 속하지 않는 프로세스" 스윕

## 임시 우회 (클라이언트 측, 적용됨)

- MFT_1MW_2026 리포지토리 23b6002: 샘플 종료마다 psutil로 자식 AEDT 강제 회수
- 정리 태스크 purge v6: 조상 체인에 python이 없는 ansysedt/3dedy만 kill
