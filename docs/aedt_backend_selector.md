# AEDT backend selector

`scheduling_profile` controls Slurm CPU and memory placement.  It does not
select how Electronics Desktop is owned.  Tasks and project campaigns have a
separate `aedt_backend` field:

- `standalone` (default): the runner owns one AEDT process, preserving all
  existing task and campaign behavior.
- `pooled`: the runner attaches to an admitted AEDT session pool.

Missing values and migrated rows become `standalone`.  Unknown values are
rejected with HTTP 422.  A task value overrides its project's default; when it
is omitted, a project task inherits `project.aedt_backend`.

```json
POST /api/tasks
{
  "name": "mft-canary",
  "project": "MFT_1MW_2026v1",
  "remote_cwd": "~/case",
  "command": "python run_simulation_260706.py --fixed --thermal",
  "scheduling_profile": "fea_bursty",
  "aedt_backend": "pooled"
}
```

The task script exports `MFT_AEDT_BACKEND=standalone|pooled`.  The selector
never exports pilot or canary acknowledgement flags.  A canary submitter must
provide the exact separately approved runner acknowledgement in `env_setup`.

Pooled tasks are accepted into the durable queue but fail closed at admission
until the AEDT pool reports `operational=true`.  Queue diagnostics report the
backend gate directly, and blocked pooled tasks do not open ordinary demand
allocations.  Standalone tasks do not consult this gate.

For pooled tasks, task-level persistent license admission removes only the
`electronics_desktop` cost because that checkout belongs to the counted pool
session.  Any other configured persistent feature costs remain enforced.

