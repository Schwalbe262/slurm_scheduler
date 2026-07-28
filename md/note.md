\# Project Loop Notes



Every meaningful execution, validation, diagnosis, planning, UI, strategy, or agent loop is recorded here in chronological order.



This file is archive/search-only for new Codex sessions.

Do not read this file in full.

Use `HANDOFF\_CURRENT.md` for current state and use targeted search for old evidence.



\## Entry template



```text

\## YYYY-MM-DD HH:mm:ss +09:00 - Loop N



\- Part:

\- Goal:

\- Hypothesis:

\- Actions:

\- Candidates:

\- Metrics:

\- Result:

\- Failure reason:

\- Next action:

\- Token usage:



```text

note.md rules:

\- Record every meaningful loop.

\- Keep entries concise.

\- Reference raw logs by path; do not paste raw logs.

\- Record failures and partial progress here.

\- Record diagnostic-only work here.

\- Do not require future sessions to read the whole file.

\- At closeout, append one concise entry only.

- 2026-07-28 21:45:14 +09:00 | MFT campaign removal | Removed pipeline/campaign APIs, persistence, UI, defaults, contracts, and repair artifacts; neutralized keeper AEDT pool labels; added configurable generic terminal workspace root. Validation: compileall clean, pytest 880 passed/3 known pilot failures/2 skipped, required grep and diff check clean. Token usage unavailable. Next: orchestrator review.

