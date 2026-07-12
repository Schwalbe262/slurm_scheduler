from __future__ import annotations

import argparse
import json
import os
import signal
from pathlib import Path


def read_identity(pid: int) -> dict:
    proc = Path("/proc") / str(pid)
    stat_text = (proc / "stat").read_text(encoding="utf-8")
    after_comm = stat_text.rsplit(")", 1)[1].strip().split()
    # /proc/<pid>/stat field 22; after the comm field, index 19 is starttime.
    start_ticks = int(after_comm[19])
    cmdline = (proc / "cmdline").read_bytes().replace(b"\0", b" ").decode(
        "utf-8", errors="replace"
    ).strip()
    environ = {}
    for item in (proc / "environ").read_bytes().split(b"\0"):
        key, separator, value = item.partition(b"=")
        if separator:
            environ[key.decode("utf-8", errors="replace")] = value.decode(
                "utf-8", errors="replace"
            )
    return {
        "pid": pid,
        "start_ticks": start_ticks,
        "cmdline": cmdline,
        "slurm_sched_task_id": environ.get("SLURM_SCHED_TASK_ID", ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Exact-PID AEDT solver fault injection (disposable pilot only)"
    )
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--expected-start-ticks", type=int, required=True)
    parser.add_argument("--expected-cmdline-contains", required=True)
    parser.add_argument("--expected-task-id", default="")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--i-understand-this-is-not-a-production-cancel-path",
        action="store_true",
    )
    args = parser.parse_args()
    identity = read_identity(args.pid)
    errors = []
    if identity["start_ticks"] != args.expected_start_ticks:
        errors.append("PID start_ticks changed; possible PID reuse")
    if args.expected_cmdline_contains not in identity["cmdline"]:
        errors.append("command line identity mismatch")
    if args.expected_task_id and identity["slurm_sched_task_id"] != args.expected_task_id:
        errors.append("SLURM_SCHED_TASK_ID mismatch")
    result = {
        "experimental": True,
        "production_cancel_supported": False,
        "identity": identity,
        "execute_requested": bool(args.execute),
        "errors": errors,
        "signal_sent": False,
    }
    if args.execute:
        if not args.i_understand_this_is_not_a_production_cancel_path:
            errors.append("explicit experimental acknowledgement flag is required")
        if errors:
            print(json.dumps(result, indent=2, sort_keys=True))
            return 2
        # Exact PID only: no process group, pkill, parent AEDT PID, or public
        # session-wide StopSimulations fallback is permitted in this experiment.
        os.kill(args.pid, signal.SIGTERM)
        result["signal_sent"] = True
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
