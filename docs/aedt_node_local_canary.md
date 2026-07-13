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

The host exits only after all N project-local close ACKs and Desktop process
shutdown. Its final `NODE_CANARY_EVIDENCE` record is passing only when both
leases were released without a fault. The discovery file is removed on exit.

Rollback is scoped to this one Desktop: create the advertised rollback file
or terminate the host task. The host marks both project leases releasing,
closes their projects, and drains its owned Desktop. Never use `scancel` on an
existing standalone production task as part of this procedure.
