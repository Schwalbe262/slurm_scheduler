# MFT AEDT exclusive 1:1 pilot result — task 30089

Date: 2026-07-13 KST

Verdict: **PASS for exclusive 1:1 lifecycle only**

This result does not authorize two projects per Desktop, production pool
enablement, or the 250-AEDT/500-project target.

## Submitted identity

- scheduler revision: `099719ba75f9dd61f96d86abd81179dc7de5f6e5`
- MFT revision: `2f1eed694392520cc4c91c435815e650613d439b`
- pyaedt_library revision: `e6b9b9d20a832ff5c3f7ca97218737a0b8650781`
- scheduler task: `30089`
- Slurm job/allocation: `732393` / `8358`
- account/node: `harry261` / `n116`
- priority/project: `10000` / `MFT_1MW_2026v1`
- production tasks cancelled: `0`

The task checked out all three full SHAs in detached mode before starting AEDT.

## Terminal solver artifact

- exactly one `RESULT_JSON`
- `result_valid_em=1`
- `aedt_backend=pooled`
- `aedt_lease_id=1`
- `aedt_exclusive_session=1`
- `matrix_solve_attempts=1`
- `matrix_solution_queries=1`
- Matrix solve time: `106.7882890701 s`
- project: `simulation_732393_812320`
- runner exit: `0`

## Lifecycle and cleanup evidence

- attach endpoint: `nib116.hpc:53751`
- Desktop PID: `811459`
- exclusive lease created and project name bound
- release requested after terminal artifact
- project-close ACK: `true`
- lease final state: `released`
- Desktop-close ACK: `true`
- session final state: `closed`
- session host exit: `0`
- Desktop PID absent after close
- exact PID `electronics_desktop` checkout observed by `lmstat`
- exact PID checkout absent after close (license returned)
- disposable AEDT project workspace absent after host ACK
- `pilot_evidence.json`: `passed=true`, `failures=[]`

Event order (UTC):

1. `22:54:22` host claimed
2. `22:54:32` host registered
3. `22:54:34` exclusive lease created and project bound
4. `22:56:40` release requested
5. `22:56:42` project-close ACK
6. `22:56:53` Desktop-close ACK
7. `22:56:54` evidence completed

Evidence directory:

```text
/gpfs/home1/harry261/slurm_scheduler/runs/
  mft_aedt_1to1_pilot_20260713-075327/evidence/
```

The host log emitted a PyAEDT `TypeError` while querying the active-session
registry during shutdown, followed by `Desktop has been released and closed`.
The PID-exit check, host ACK, exit code, and exact-PID license-return check all
passed, so this message did not fail the 1:1 lifecycle gate.  It remains a log
signal to watch in any later pilot.

## Activation decision

- live production pool: remains disabled
- `adapter_ready`: remains false
- MFT production backend: remains standalone
- next allowed scope: reviewed disabled-by-default scheduler deployment only
- still prohibited: 1:2, 250/500, or any production task replacement based on
  this result alone
