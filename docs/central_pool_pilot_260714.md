# Central AEDT Pool Pilot (2026-07-14)

This disposable task exercises the live control plane through the login-node
relay and attaches to a warm-spare AEDT session. The bootstrap token file must
be readable on the compute node; do not put the token itself in the task
command.

## Submit as a scheduler task

Use an account with the `conda:pyaedt2026v1` capability and profile. PyAEDT
still needs the AEDT installation environment for attach-only clients, so keep
the same module/fallback line used by session hosts.

```bash
export SCHEDULER_URL=http://scheduler-host:8000
export PILOT_REMOTE_CWD=/shared/slurm_scheduler_central_pilot_260714
export PILOT_TOKEN_FILE=/shared/secrets/aedt_pool_bootstrap
export POOL_RELAY_URL=http://172.16.10.37:18790

curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=central-aedt-pool-pilot \
  -F remote_cwd="$PILOT_REMOTE_CWD" \
  -F command="python scripts/aedt_pool_central_pilot.py --scheduler-url $POOL_RELAY_URL --token-file $PILOT_TOKEN_FILE --output-json central_pool_pilot_evidence.json" \
  -F required_capability=conda:pyaedt2026v1 \
  -F env_profile=pyaedt2026v1 \
  -F 'env_setup=module load ansys-electronics/v252 || export ANSYSEM_ROOT252=/opt/ohpc/pub/Electronics/v252/Linux64' \
  -F aedt_backend=pooled \
  -F scheduling_profile=fea_bursty \
  -F priority=10000 \
  -F cpus=4 \
  -F memory_mb=8192 \
  -F gpus=0
```

The task's default project name includes a timestamp. Give `--project-name`
only when a stable evidence name is useful. The JSON and saved `.aedt` project
are written in the task working directory in the example above.

## Acceptance checklist

- The JSON reports `ok: true`, a lease ID, and phase timings.
- The lease is granted, and `endpoint` / `session_node_name` identify the same
  compute node that ran the task.
- `saved_project_path` exists and contains the named Maxwell 3D design with
  `CentralPilotBox`.
- `final_lease_state` is `released`, which is the project-close ACK; the client
  did not close the shared Desktop.
- After the close ACK, the matching session is `ready` again in
  `GET /api/aedt-pool`.
