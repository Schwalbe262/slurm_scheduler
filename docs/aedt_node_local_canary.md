# Scheduler-managed node-local AEDT canary

Use this bounded fallback only when a compute node cannot route to the live
scheduler HTTP service. It does not enable the central AEDT pool.

One high-priority scheduler task runs
`scripts/aedt_pool_node_canary_host.py`. That task owns a loopback-only control
plane and exactly one AEDT Desktop. It writes a discovery JSON file under
`/tmp` after Desktop registration. `--expected-projects N` defines the bounded
number of co-located MFT tasks. They read `scheduler_url` from that file and
use the explicit MFT shared-canary acknowledgement. The current live canary
must stay at `N=2`; larger N requires separate license, isolation, memory, and
solver validation first.

The scheduler admits `aedt_backend=pooled` while the central pool is disabled
only when a task uses the exact `aedt_node_canary_client` entrypoint,
references an active `aedt_node_canary_host` through `same_node_as_task_id`,
and carries matching bundle identity and expected-project count fields in
`payload_json`. This temporary exception is hard-capped at the currently
validated N=2.

Submit the host itself with `requested_allocation_id` set to the database ID
of an existing live allocation and leave `same_node_as_task_id` unset (or
zero). This is an exact allocation pin: it neither falls back to another
allocation on the same node nor opens a replacement allocation. The host's
own task claim keeps that allocation active, so completion of an unrelated
sibling does not terminate it. For example, the host task portion of a
`POST /api/tasks` request is:

```json
{
  "entrypoint": "aedt_node_canary_host",
  "requested_allocation_id": 1234,
  "same_node_as_task_id": 0,
  "timeout_seconds": 0
}
```

After the host reaches `running`, submit each client with
`same_node_as_task_id` (or its `same_node_as` alias) set to the host task ID.
Do not combine `requested_allocation_id` and `same_node_as_task_id` on the
same task; the API rejects that ambiguous request.

The host exits only after all N project-local close ACKs and Desktop process
shutdown. Its final `NODE_CANARY_EVIDENCE` record is passing only when both
leases were released without a fault. The discovery file is removed on exit.

Rollback is scoped to this one Desktop: create the advertised rollback file
or terminate the host task. The host marks both project leases releasing,
closes their projects, and drains its owned Desktop. Never use `scancel` on an
existing standalone production task as part of this procedure.
