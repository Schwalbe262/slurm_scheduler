from __future__ import annotations

import re
import shlex


ACCOUNT_WORKSPACE_PLACEHOLDER = "__SLURM_SCHEDULER_ACCOUNT_WORKSPACE__"
TASK_ID_PLACEHOLDER = "__SLURM_SCHEDULER_TASK_ID__"


def literal_shell_assignments(text: str, *, leading_only: bool) -> dict[str, str]:
    result: dict[str, str] = {}
    stop = False
    for raw_line in str(text or "").splitlines():
        if stop:
            break
        try:
            lexer = shlex.shlex(raw_line, posix=True, punctuation_chars=";")
            lexer.whitespace_split = True
            lexer.commenters = "#"
            tokens = list(lexer)
        except ValueError:
            if leading_only:
                break
            continue
        statements: list[list[str]] = [[]]
        for token in tokens:
            if token and set(token) == {";"}:
                statements.extend([] for _ in token)
            else:
                statements[-1].append(token)
        for statement in statements:
            if not statement:
                continue
            if statement[0] == "export":
                statement = statement[1:]
            assignments: dict[str, str] = {}
            for token in statement:
                if "=" not in token:
                    assignments = {}
                    break
                key, value = token.split("=", 1)
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                    assignments = {}
                    break
                assignments[key] = value
            if assignments:
                result.update(assignments)
                continue
            if leading_only:
                stop = True
                break
    return result


def literal_task_environment(task: dict) -> dict[str, str]:
    """Read literal assignments without executing or expanding user shell."""

    result = literal_shell_assignments(
        str(task.get("env_setup") or ""), leading_only=False
    )
    # submission_env is a safe leading assignment prefix in command.
    result.update(
        literal_shell_assignments(
            str(task.get("command") or ""), leading_only=True
        )
    )
    return result


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
