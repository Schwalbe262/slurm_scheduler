# Scheduler tick latency candidate (2026-07-18)

## Scope and safety

- Exact live source inspected: `71ae4e99020be44d0ad44309cd846511fb8ebf69`.
- Exact live deployment directory inspected read-only:
  `C:\Users\peets\slurm_scheduler_runtime\deployments\71ae4e99020b`.
- Live scheduler HTTP port `8002`, process, database, tasks, allocations, and
  campaign demand were not mutated or restarted.
- The candidate changes only repeated metadata reads inside the scheduler
  tick. Capacity, fit, priority, storage, license, project caps, attach caps,
  allocation submission, and the 500-task campaign policy are unchanged.
- Dashboard name filtering remains a SQL `WHERE` predicate before
  `LIMIT/OFFSET`; a regression test covers matches hidden behind more than one
  unfiltered page.

## Before evidence

The last 30 complete live ticks sampled from ticks 229 through 258 had:

| Stage | Minimum | Median | p95 | Maximum |
|---|---:|---:|---:|---:|
| whole tick | 68.363 s | 109.091 s | 188.813 s | 209.188 s |
| `assign_ready_fea` | 25.382 s | 51.480 s | 92.507 s | 110.529 s |
| `maintain_allocation_pool` | 18.257 s | 29.970 s | 59.311 s | 66.538 s |
| `refresh_tasks` | 7.821 s | 9.704 s | 16.840 s | 19.127 s |

Read-only HTTP observations during the incident were 162 ms for
`/api/health`, 48 ms for `/api/tasks/summary`, 53 ms for a 100-row compact
queued-task read, and 2.928 s for the full dashboard. These are observations,
not candidate post-deployment measurements.

## Root cause

The fit planner evaluates each queued task against each live allocation.
`account_supports()` reopened the network-backed SQLite database and selected
the same account environment overlays for every task/allocation pair. The FEA
assignment loop also rebuilt the same global node worker count for each
capacity miss. Neither repeated read changed scheduling semantics within a
single pass.

On the 12:50 live backup, a 20-task by roughly 43-allocation reservation-plan
profile took 30.504 seconds. Of that, 760 calls to
`list_account_env_overlays()` consumed 28.578 seconds. This is local database
I/O on the scheduler's serial critical path, not required SSH or Slurm I/O.

## Candidate

1. `_tick_client_cache()` now owns a scheduler-thread-local account overlay
   capability/profile snapshot. Each eligible account is read at most once in
   a tick. Web and control threads do not inherit it and continue to read the
   current database state.
2. FEA node worker counts are reused only by the scheduler tick thread. The
   cache is invalidated after a successful attach, task refresh, rebalance, or
   pressure requeue, and is also keyed by tick sequence.

The change does not skip a task or allocation, bound the number scanned, alter
ordering, or defer remote side effects. It removes duplicate reads while every
existing admission decision still runs.

## Benchmarks

All benchmarks used an isolated diagnostic copy or read-only backup; no live
row was changed.

| Exact workload | Live source | Candidate | Reduction |
|---|---:|---:|---:|
| 20-task reservation plan on network-backed backup | 30.504 s | 2.041 s | 93.3% |
| complete 137-task candidate reservation plan | not rerun to completion | 5.093 s | candidate absolute result |
| 20-task FEA assignment on identical local DB clones | 3.216 s | 1.109 s | 65.5% |

For normal ticks without allocation submission or periodic orphan cleanup,
`maintain_allocation_pool` is predicted to fall into roughly the 5-12 second
range. `assign_ready_fea` should lose its repeated overlay and no-capacity
worker-map costs. Remote `sbatch`, account failures, task probes, orphan
process sweeps, cleanup, and backup remain separately bounded sources of tick
latency; this candidate does not claim to remove them.

## Deployment prerequisites

1. Build an immutable deployment from the reviewed candidate commit and
   record source/deployment hashes.
2. Back up the live database and record the current launcher, PID tree,
   campaign counts, task status counts, and exact rollback deployment
   `71ae4e99020b`.
3. Run the full unit suite and lint/compile checks from the immutable build.
4. Restart only the scheduler web/watchdog child. Do not cancel, resubmit, or
   rewrite any task/allocation/campaign row.
5. Confirm filter-before-pagination on the deployed dashboard/API.
6. Observe at least five ordinary ticks and one periodic-maintenance tick.
   Compare stage timing and lightweight API latency while the 500-task target,
   task priorities, active counts, allocation counts, and submission rate stay
   unchanged.
7. Roll back to the recorded `71ae4e99020b` deployment if health fails, task
   counts change unexpectedly, or any fit/capacity regression appears.

## Validation status

- Focused timing/concurrency/filter tests: 7 passed.
- Assignment, allocation-pool, reservation-plan, filter, and cache regression
  selection: 17 passed.
- Python compile check: passed.
- Full suite: 855 passed, 1 skipped, 84 subtests passed, with 3 failures.
  The same 3 loopback pilot tests fail unchanged on exact live source
  `71ae4e99020b` because their `FakeDesktop` lacks the pre-existing required
  `GetVersion` runtime-attestation API. They are not touched by this candidate
  and were reproduced separately against the live deployment source.
- Full suite excluding only those two known-broken pilot files: 849 passed,
  1 skipped, 84 subtests passed.
- Ruff is not installed in the live scheduler virtual environment; no lint
  result is claimed. `git diff --check` passes.
