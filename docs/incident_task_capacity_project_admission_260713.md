# `/api/task-capacity` project admission incident (2026-07-13)

## Symptom

Project-owned priority tasks ran normally, but the MFT refill controller saw
`queue_state=blocked` even with ready CPU/memory capacity. The diagnostic was:

```text
license admission: unconfigured license admission profile for FEA project <empty>
```

The controller sent `project=MFT_1MW_2026v1`, but the live FastAPI route did
not declare a `project` query parameter. FastAPI therefore discarded it and
constructed a projectless synthetic capacity task. The admission layer was
correctly fail-closed; the route had lost the identity before that layer.

## Fix and invariant

`GET /api/task-capacity` now declares `project` and copies the stripped value
into the same synthetic task passed to both `task_fit_capacity` and
`task_queue_diagnostics`. No project, task, allocation, cap, priority, or
license configuration is mutated.

Unknown/projectless FEA probes remain fail-closed. A configured project may be
admitted only through its existing
`license_monitor.admission.persistent_cost_by_project` profile.

## Deployment and rollback

Before deployment:

1. run the targeted route/OpenAPI test and the license-admission suite;
2. record the pre-deploy service SHA/PIDs and current task/allocation/project
   counts;
3. preserve the current runtime tree as a read-only rollback reference.

Deploy only the reviewed commit, restart the watchdog child without cancelling
tasks or allocations, and verify:

- `/healthz` is 200;
- OpenAPI lists `project` for `/api/task-capacity`;
- an empty project remains blocked;
- `project=MFT_1MW_2026v1` no longer reports `<empty>` and uses the configured
  admission profile;
- project cap retains its configured value and existing task/allocation identities are unchanged.

Rollback restores the recorded pre-deploy source commit/tree and restarts only
the web child. It must not cancel Slurm jobs, tasks, or allocations.
