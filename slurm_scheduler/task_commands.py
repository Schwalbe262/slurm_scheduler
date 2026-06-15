from __future__ import annotations

import shlex


ACCOUNT_WORKSPACE_PLACEHOLDER = "__SLURM_SCHEDULER_ACCOUNT_WORKSPACE__"
TASK_ID_PLACEHOLDER = "__SLURM_SCHEDULER_TASK_ID__"


def build_git_task_command(repo_url: str, git_ref: str, entrypoint: str, arguments: str = "") -> str:
    args = (arguments or "").strip()
    command = f"python {shlex.quote(entrypoint)}"
    if args:
        command = f"{command} {args}"
    return "\n".join(
        [
            f"git_root={ACCOUNT_WORKSPACE_PLACEHOLDER}/git_tasks",
            'mkdir -p "$git_root"',
            f"workdir=\"$git_root/task-{TASK_ID_PLACEHOLDER}\"",
            'rm -rf "$workdir"',
            'mkdir -p "$workdir"',
            f"git clone {shlex.quote(repo_url)} \"$workdir/repo\"",
            'cd "$workdir/repo"',
            f"git checkout {shlex.quote(git_ref)}",
            command,
        ]
    )
