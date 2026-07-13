# MFT shared AEDT 1:2 isolated pilot

> Status: the corrected isolated normal/pre-solve-abort pilot passed in task
> 30445 on 2026-07-13. Production pool remains `enabled=false` and
> `adapter_ready=false`: this pass is not the complete enablement contract.
> See [the task 30445 result](mft_aedt_attach_1to2_result_30445.md).

This is the next gate after task 30089 proved the exclusive 1 AEDT:1 MFT
project lifecycle. The entrypoint is `scripts/aedt_pool_1to2_pilot.py`. It runs
inside one priority-10000 scheduler task, opens one disposable Desktop at a
time, and attaches two independent Matrix-only MFT runners to that Desktop.
It does not use the live pool database or change live pool settings.

The task clones exact full Git SHAs for scheduler, MFT, and `pyaedt_library`.
The scheduler task consumes one project-cap slot even though its internal
orchestrator starts two MFT project clients.

## Cases

`normal`:

- one `electronics_desktop` checkout for the exact Desktop PID;
- two non-exclusive leases in distinct slots;
- two distinct MFT projects and terminal `RESULT_JSON` records;
- both records have `result_valid_em=1`, one Matrix solve/query, and positive
  positive `Llt`;
- peak two owned `elec_solve_maxwell` checkout rows;
- independent project-close ACKs, Desktop close ACK, process exit, license
  return, and workspace cleanup.

`abort`:

- client A creates its project, writes a pre-solve marker, and intentionally
  waits before any solver starts;
- the orchestrator terminates only client A and reports a `pre_solve` fault;
- the session host closes only A's project and does not call global
  `StopSimulations` or recycle the Desktop while B is live;
- B must still produce the same valid terminal Matrix result contract;
- after B's independent release, both close ACKs and Desktop/license cleanup
  must complete.

This fault is intentionally different from the failed 732549/732554 approach.
Killing one solver PID mid-solve left a solver checkout and Desktop running
state behind; PID/gRPC survival did not produce a sibling data row or field
solution. Direct solver-PID termination is therefore not a production cancel
mechanism. A true solver timeout remains session quarantine + sibling grace +
whole-Desktop recycle. The local abort case proves only the safe project-local
pre-solve boundary.

The next isolated gate can be selected with `--cases timeout`. It waits until
two owned Maxwell solver checkouts are visible, terminates only disposable
client A (not an AEDT/solver PID), reports a solve-timeout quarantine, rejects
project C, and requires B's terminal row and solution query before the host is
allowed to globally stop and recycle the faulted Desktop. This validates the
control-plane blast radius; it still does not turn direct solver-PID
termination into a supported cancellation method.

## Command

```bash
python scripts/aedt_pool_1to2_pilot.py \
  --mft-repo-url https://github.com/Schwalbe262/MFT_1MW_2026.git \
  --mft-revision <full-40-char-sha> \
  --library-repo-url <library-git-url> \
  --library-revision <full-40-char-sha> \
  --output-dir <new-disposable-path> \
  --lmutil <lmutil-path> \
  --license-server <port@server> \
  --solver-license-feature elec_solve_maxwell
```

For the timeout-only follow-up, append `--cases timeout`. Keep it in one
priority-10000 disposable scheduler task. It must not cancel or replace any
production task.

`pilot_evidence.json` is the only pass/fail authority. Task exit 0 without
`passed=true`, two valid normal results, one valid abort sibling result, both
project-close ACKs per case, and license cleanup is not a pass.

## WEB/operator limits

`/aedt-pool` exposes three durable settings:

- maximum AEDT sessions: 0..550;
- total concurrent projects: 0..1100 and no more than sessions × slots;
- projects per AEDT: 1..2.

The values may be saved while disabled. Changing projects per AEDT requires a
disabled, fully drained pool. The page also shows current sessions, leases,
usage, latest validation, adapter gate, and operational state. Enable remains
fail-closed unless both the durable validation and host-adapter gates pass.
Saving limits alone never opens a node or Desktop.

## Remaining production gates after task 30445

- real active-solve timeout quarantine, sibling completion, Desktop recycle,
  and faulted solver/license return;
- normal cancel isolation (separate from a pre-solve script/client abort);
- AEDT/gRPC death and session-host/allocation death requeue recovery;
- exact 2-Desktop baseline versus 1-Desktop treatment output parity, runtime
  ratio `<=1.20`, and Desktop license delta `<=-1`;
- live pool database/session-host adapter configuration and an explicit
  adapter readiness review;
- an opt-in production MFT backend revision. The current MFT switch is still
  named `MFT_AEDT_SHARED_1TO2_PILOT` and the active campaign does not set it.
