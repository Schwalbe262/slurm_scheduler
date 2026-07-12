# 버그: 태스크 0개인 active 할당이 유휴 회수에서 누락

## 증상 (2026-07-12 실측)
- allocation 6757 (harry261, n113): **63시간** 동안 태스크 0개로 held, 자동 회수 안 됨
- 수동 close는 정상 동작 (POST /api/allocations/6757/close -> closed, closed_task_ids=[])

## 원인 (코드)
- `scale_in_idle_allocations()`가 **state == WARM 만** 스캔 (scheduler.py ~4479)
- 태스크가 모두 빠진 뒤 active -> warm 전환이 누락되는 경로가 있으면
  그 할당은 영원히 회수 대상에서 제외됨

## 제안
- scale-in 대상에 "active && active_task 0 && last_active > idle_seconds" 추가
  (close_allocation의 active-task 가드가 이미 있으므로 안전), 또는
- 태스크 카운트가 0이 되는 시점에 active -> warm 전환 보장

## 참고
- 다른 active 할당들은 close 시 409 (active tasks 가드)로 잘 보호됨 - 가드 자체는 건강
