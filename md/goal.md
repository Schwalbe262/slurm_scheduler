\# Project Goal



\## Mission



\- <최종 목표>

\- <사용자/운영자에게 중요한 결과>

\- <자동화/에이전트/시스템이 맡을 역할>

\- <사람이 개입해야 하는 경계>

\- <장기적으로 도달할 상태>



\## Current Sprint



\- <현재 1차 목표>

\- <이번 sprint의 성공 조건>

\- <지금 피해야 할 실패 패턴>

\- <검증 방법>

\- <다음 단계 후보>



\## System Roles



\### Strategic layer



\- Decides goals, priorities, bottleneck diagnosis, and safe candidate directions.

\- May use LLM/planner/rules/human review depending on the project.

\- Must not directly mutate high-risk state unless the deterministic layer validates it.



\### Deterministic execution layer



\- Performs concrete actions.

\- Owns validation, rollback, tests, command execution, and state mutation.

\- Must be logged and test-covered.



\### Monitoring/UI layer



\- Shows current status, blockers, metrics, token usage, recent loops, and confirmed insights.

\- Supports manual inspection and safe operator intervention.



\### Codex role



\- Adds missing deterministic functions, tests, docs, CLI hooks, and UI support.

\- Does not replace the project’s normal runtime agent once that agent can perform the loop.

\- Keeps changes small, validated, and recorded.



\## Success Criteria



\- <정확성 기준>

\- <성능/효율 기준>

\- <안전/보안 기준>

\- <관측 가능성 기준>

\- <유지보수성 기준>

\- <사용자 경험 기준>



\## Quality Criteria



\- <프로젝트별 좋은 결과의 기준>

\- <throughput, cost, latency, robustness, safety, compactness, scalability 등>



\## Learning / Improvement Roadmap



\- Preserve useful traces and decisions.

\- Record all meaningful loops in `note.md`.

\- Promote only confirmed reusable improvements to `insight.md`.

\- Compare before/after behavior with evidence.

\- Use accumulated traces for prompt tuning, evals, fine-tuning, regression tests, or operator training as appropriate.



\## Current Status



\- <현재 상태 요약>

\- <최근 검증>

\- <현재 blocker>

\- <다음 작업>



\## Later Milestones



\- <중기 목표>

\- <장기 목표>

