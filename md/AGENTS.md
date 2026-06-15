\## Codex context budget and project memory policy



Start each new Codex thread from `HANDOFF\_CURRENT.md`.



Hard input rules:

\- Never read `note.md` in full.

\- Never read `insight.md` in full.

\- Never paste full test/build logs.

\- Never paste full `git diff`.

\- Never paste full JSON/JSONL logs.

\- Never continue a long previous Codex thread when `HANDOFF\_CURRENT.md` can resume the work.



Default startup context:

1\. `HANDOFF\_CURRENT.md`

2\. `AGENTS.md` or equivalent repo instructions

3\. `goal.md`, only enough to understand mission/current sprint

4\. minimal project metadata needed for commands and entrypoints



Treat long journals, old handoffs, logs, JSONL traces, generated reports, `note.md`, and `insight.md` as archive/search-only history.



Use targeted search instead:

\- `rg -n "specific term" note.md`

\- `rg -n "specific term" insight.md`

\- `rg -n "^#|^##|Current|Next|Validation|Failure|Result" <long-md-file>`

\- `tail -n 80 <log>`

\- `git diff --stat`

\- `git diff -- <specific-file>`



For correctness:

\- inspect exact source ranges before editing;

\- inspect exact diff hunks before review;

\- do not rely on lossy summaries for source code, diffs, migrations, schemas, auth/security, infra, or data-loss-sensitive changes.



Project memory:

\- `goal.md` is the mission, sprint, success criteria, roadmap, and project-specific quality standard.

\- `note.md` is the chronological execution journal.

\- `insight.md` is only for confirmed reusable improvements.

\- `HANDOFF\_CURRENT.md` is the short current-state handoff for new threads.



Loop journal rule:

Append to `note.md` for each meaningful execution loop:

\- timestamp;

\- part;

\- goal;

\- hypothesis;

\- actions;

\- candidates/options;

\- metrics;

\- result;

\- failure reason;

\- next action;

\- token usage if available.



Insight rule:

Append to `insight.md` only when a reusable improvement is confirmed:

\- source loop;

\- improvement;

\- before;

\- after;

\- evidence;

\- remaining risk.



Do not add routine loop completions, speculative ideas, diagnostic-only runs, or ordinary failures to `insight.md`.



At the end of each small part:

\- run the smallest relevant validation first;

\- inspect `git diff --stat`;

\- inspect only exact changed hunks needed for review;

\- update `HANDOFF\_CURRENT.md` in 10 lines or fewer;

\- append one concise event to `note.md`;

\- append to `insight.md` only if there is a confirmed reusable improvement;

\- record current Codex thread token usage if the project supports it.

