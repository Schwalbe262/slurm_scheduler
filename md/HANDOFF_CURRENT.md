\# Current Handoff



\## Current status



\- <현재 상태 3-7줄>



\## Current objective



\- <지금 part의 목표>



\## Active branch / part



\- Branch:

\- Part:



\## Important files



\- `<file>`: <왜 중요한지>

\- `<file>`: <왜 중요한지>



\## Last validation



\- <마지막 테스트/검증>

\- <결과>

\- <로그 경로, 필요 시>



\## Current blocker



\- <현재 막힌 점>

\- <원인 추정>

\- <확인된 증거>



\## Next steps



1\. <구체적 다음 작업>

2\. <구체적 다음 작업>

3\. <구체적 다음 작업>

4\. <선택적>

5\. <선택적>



\## Token/context policy



\- Start from this file.

\- Do not read `note.md` or `insight.md` in full.

\- Search archive docs only with targeted `rg`.

\- Do not paste full logs, JSON/JSONL, test output, or git diff.

\- Update this file in 10 lines or fewer at closeout.



\## Archive/search policy



\- `note.md`: chronological loop archive.

\- `insight.md`: confirmed reusable improvements.

\- Old handoffs/logs/traces: search-only.



\## Recent changes



\- <최대 10개 bullet>



\## Risks and gotchas

## 2026-07-12 exclusive allocation fix
- `exclusive_node` allocation scripts now emit `#SBATCH --exclusive`.
- Task/allocation exclusivity must match exactly; exclusive shapes reject mixed/busy nodes on every CPU partition.
- Focused regressions passed 6/6; the full `tests.test_core` suite passed 331/331.
- Changes are intentionally uncommitted, undeployed, and have not touched live tasks.



\- <주의점>

\- <주의점>

