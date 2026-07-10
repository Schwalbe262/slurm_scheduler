from __future__ import annotations

import threading
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


def days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

from slurm_scheduler.config import AccountConfig, GitCredentialConfig, load_app_config
from slurm_scheduler.allocation_metrics import annotate_allocation_fea_pressure, annotate_allocation_node_metrics
from slurm_scheduler.conda_sync import conda_bootstrap, env_prefix_lookup_command
from slurm_scheduler.db import Database
from slurm_scheduler.git_auth import find_git_credential, git_task_payload
from slurm_scheduler.models import AccountSnapshot, AllocationStatus, JobCreate, JobStatus, SchedulingProfile, TaskCreate, TaskStatus
from slurm_scheduler.scheduler import Scheduler
from slurm_scheduler.inventory import parse_scontrol_nodes, parse_sinfo_nodes, partition_rank
from slurm_scheduler.pestat import parse_pestat, plan_dynamic_allocations
from slurm_scheduler.task_commands import ACCOUNT_WORKSPACE_PLACEHOLDER, TASK_ID_PLACEHOLDER, build_git_task_command
from slurm_scheduler.slurm import (
    JobStateInfo,
    RemoteCommandTimeout,
    RemoteExecutionError,
    SSHSession,
    TaskProbe,
    apply_env_profile,
    background_wrapper_command,
    build_allocation_script,
    build_sbatch_script,
    build_srun_attach_command,
    build_task_script,
    cancel_process_group_command,
    parse_du_gb,
    parse_sacct_states,
    parse_sbatch_job_id,
    parse_squeue_counts,
    parse_squeue_table,
    parse_task_probe_output,
    remote_text_command,
    remote_execution_path,
    resolve_task_placeholders,
)


class SlurmParsingTests(unittest.TestCase):
    def test_load_app_config_parses_git_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "app.yaml"
            path.write_text(
                "\n".join(
                    [
                        "git_credentials:",
                        "  - id: private-project",
                        "    url_patterns: ['*org/private-project*']",
                        "    clone_url: 'git@github.com:org/private-project.git'",
                        "    source_account: account_a",
                        "    source_private_key_path: '~/.ssh/private_project_deploy'",
                    ]
                ),
                encoding="utf-8",
            )
            config = load_app_config(path)
        self.assertEqual(config.git_credentials[0].id, "private-project")
        self.assertEqual(config.git_credentials[0].source_account, "account_a")

    def test_load_app_config_defaults_cleanup_finished_artifacts_to_three_days(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "app.yaml"
            path.write_text("", encoding="utf-8")
            config = load_app_config(path)
        self.assertEqual(config.cleanup_finished_task_ttl_seconds, 259200)
        self.assertEqual(config.cleanup_finished_job_ttl_seconds, 259200)

    def test_load_app_config_parses_gpu_prewarm_cpu_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "app.yaml"
            path.write_text(
                "\n".join(
                    [
                        "gpu_prewarm:",
                        "  gpus_per_allocation: 4",
                        "  min_gpus_per_allocation: 4",
                        "  cpus_per_allocation: 4",
                    ]
                ),
                encoding="utf-8",
            )
            config = load_app_config(path)
        self.assertEqual(config.gpu_prewarm_gpus_per_allocation, 4)
        self.assertEqual(config.gpu_prewarm_min_gpus_per_allocation, 4)
        self.assertEqual(config.gpu_prewarm_cpus_per_allocation, 4)

    def test_load_app_config_parses_cpu_partition_allocation_limits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "app.yaml"
            path.write_text(
                "\n".join(
                    [
                        "cpu_partition_allocation_limits:",
                        "  cpu2: 2",
                    ]
                ),
                encoding="utf-8",
            )
            config = load_app_config(path)
        self.assertEqual(config.cpu_partition_allocation_limits, {"cpu2": 2})

    def test_load_app_config_parses_fea_overload_scale_out_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "app.yaml"
            path.write_text(
                "\n".join(
                    [
                        "fea_bursty:",
                        "  overload_scale_out_load_factor: 2.25",
                        "  overload_scale_out_seconds: 420",
                    ]
                ),
                encoding="utf-8",
            )
            config = load_app_config(path)
        self.assertEqual(config.fea_overload_scale_out_load_factor, 2.25)
        self.assertEqual(config.fea_overload_scale_out_seconds, 420)

    def test_annotate_allocation_node_metrics_adds_pestat_usage(self) -> None:
        allocations = [{"id": 1, "node_name": "n001"}, {"id": 2, "node_name": "n002"}]
        annotated = annotate_allocation_node_metrics(
            allocations,
            [
                {
                    "hostname": "n001",
                    "state": "mix",
                    "cpu_used": 40,
                    "cpu_total": 64,
                    "cpu_load": 12.5,
                    "memory_mb": 102400,
                    "free_memory_mb": 65536,
                    "observed_at": "2026-06-18 12:00:00",
                }
            ],
        )
        self.assertEqual(annotated[0]["node_cpu_used"], 40)
        self.assertEqual(annotated[0]["node_cpu_total"], 64)
        self.assertEqual(annotated[0]["node_cpu_load"], 12.5)
        self.assertEqual(annotated[0]["node_cpu_load_percent"], 19.5)
        self.assertEqual(annotated[0]["node_memory_used_mb"], 36864)
        self.assertEqual(annotated[0]["node_memory_used_gb"], 36)
        self.assertEqual(annotated[0]["node_memory_total_gb"], 100)
        self.assertEqual(annotated[0]["node_memory_used_percent"], 36.0)
        self.assertIsNone(annotated[1]["node_cpu_load"])
        self.assertIsNone(annotated[1]["node_memory_used_mb"])

    def test_annotate_allocation_fea_pressure_adds_owned_requested_cpu(self) -> None:
        allocations = [{"id": 1, "node_name": "n001"}, {"id": 2, "node_name": "n002"}]
        annotated = annotate_allocation_fea_pressure(
            allocations,
            {1: {"workers": 32, "requested_cpus": 128, "owned_cpus": 64}},
        )
        self.assertEqual(annotated[0]["node_fea_requested_cpus"], 128)
        self.assertEqual(annotated[0]["node_fea_owned_cpus"], 64)
        self.assertEqual(annotated[0]["node_fea_cpu_percent"], 200.0)
        self.assertIsNone(annotated[1]["node_fea_requested_cpus"])

    def test_parse_squeue_counts(self) -> None:
        self.assertEqual(parse_squeue_counts("RUNNING\nPENDING\nR\nPD\nCOMPLETED\n"), (2, 2))

    def test_parse_sbatch_job_id(self) -> None:
        self.assertEqual(parse_sbatch_job_id("Submitted batch job 12345\n"), "12345")

    def test_parse_du_gb(self) -> None:
        self.assertAlmostEqual(parse_du_gb("1048576\t/path\n"), 1.0)

    def test_remote_text_command_limits_on_remote_side(self) -> None:
        self.assertEqual(
            remote_text_command("/remote/out.log", max_bytes=1024),
            "test -f /remote/out.log && tail -c 1024 -- /remote/out.log",
        )
        self.assertEqual(
            remote_text_command("/remote/out.log", tail_lines=100, max_bytes=4096),
            "test -f /remote/out.log && tail -n 100 -- /remote/out.log | tail -c 4096",
        )
        self.assertIn("'/remote/path with spaces/out.log'", remote_text_command("/remote/path with spaces/out.log", max_bytes=10))

    def test_build_sbatch_script(self) -> None:
        job = {
            "job_name": "sleep-test",
            "time_limit": "00:10:00",
            "cpus": 4,
            "memory": "1G",
            "partition": "",
            "gpus": 0,
            "entrypoint": "scripts/run.py",
            "arguments": "--x 1",
            "env_setup": "module load python",
        }
        script = build_sbatch_script(job, "/tmp/job-1")
        self.assertIn("#SBATCH --cpus-per-task=4", script)
        self.assertIn("module load python", script)
        self.assertIn("python scripts/run.py --x 1", script)
        self.assertLess(script.index("#SBATCH --job-name"), script.index("set -euo pipefail"))

    def test_build_sbatch_script_with_relative_remote_dir(self) -> None:
        job = {
            "job_name": "relative-test",
            "time_limit": "00:10:00",
            "cpus": 4,
            "memory": "1G",
            "partition": "",
            "gpus": 0,
            "entrypoint": "run.py",
            "arguments": "",
            "env_setup": "",
        }
        script = build_sbatch_script(job, "slurm_scheduler/job-1")
        self.assertIn("#SBATCH --output=slurm-%j.out", script)
        self.assertIn("cd repo", script)

    def test_build_sbatch_script_with_gpu_model(self) -> None:
        job = {
            "job_name": "gpu-test",
            "time_limit": "00:10:00",
            "cpus": 8,
            "memory": "16G",
            "partition": "gpu3",
            "node_name": "gpu-node",
            "gpus": 1,
            "gpu_model": "a6000ada",
            "exclusive_node": 1,
            "entrypoint": "run.py",
            "arguments": "",
            "env_setup": "",
        }
        script = build_sbatch_script(job, "/tmp/job-1")
        self.assertIn("#SBATCH --gres=gpu:a6000ada:1", script)
        self.assertIn("#SBATCH --nodelist=gpu-node", script)
        self.assertIn("#SBATCH --exclusive", script)

    def test_build_packed_script_uses_adaptive_manager(self) -> None:
        job = {
            "job_mode": "packed_srun",
            "job_name": "packed",
            "time_limit": "12:00:00",
            "cpus": 44,
            "memory": "128G",
            "partition": "cpu1",
            "gpus": 0,
            "entrypoint": "run_simulation.py",
            "arguments": "",
            "env_setup": "module load app",
            "remote_path": "~/project",
            "simulation_count": 16,
            "simulation_start": 1,
            "cpus_per_simulation": 4,
            "initial_workers": 11,
            "max_workers_per_job": 16,
            "mem_per_simulation_gb": 8,
            "load_target": 0.75,
            "ramp_interval_seconds": 900,
        }
        script = build_sbatch_script(job, "slurm_scheduler/job-1")
        self.assertIn("#SBATCH --ntasks=1", script)
        self.assertIn("#SBATCH --cpus-per-task=44", script)
        self.assertIn("initial_limit = 11", script)
        self.assertIn("max_limit = 16", script)
        self.assertIn("[adaptive] increased worker limit", script)
        self.assertIn("[adaptive] reduced worker limit", script)
        self.assertIn("cpu_headroom", script)
        self.assertIn("probe_interval = 60", script)
        self.assertLess(script.index("#SBATCH --job-name"), script.index("set -euo pipefail"))

    def test_build_packed_script_includes_requested_node(self) -> None:
        job = {
            "job_mode": "packed_srun",
            "job_name": "packed",
            "time_limit": "12:00:00",
            "cpus": 4,
            "memory": "8G",
            "partition": "cpu2",
            "gpus": 0,
            "entrypoint": "run.py",
            "arguments": "",
            "env_setup": "",
            "remote_path": "~/project",
            "simulation_count": 1,
            "simulation_start": 1,
            "cpus_per_simulation": 4,
            "initial_workers": 1,
            "max_workers_per_job": 1,
            "node_name": "n110",
        }
        script = build_sbatch_script(job, "slurm_scheduler/job-1")
        self.assertIn("#SBATCH --partition=cpu2", script)
        self.assertIn("#SBATCH --nodelist=n110", script)

    def test_allocation_script_is_single_node_pool_job(self) -> None:
        allocation = {
            "id": 12,
            "remote_dir": "/remote/allocation",
            "stdout_path": "/remote/allocation/allocation-%j.out",
            "stderr_path": "/remote/allocation/allocation-%j.err",
            "total_cpus": 32,
            "total_memory_mb": 65536,
            "partition": "cpu2",
            "node_name": "n100",
        }
        script = build_allocation_script(allocation, "48:00:00")
        self.assertIn("#SBATCH --job-name=pool", script)
        self.assertNotIn("pool-12", script)
        self.assertIn("#SBATCH --nodes=1", script)
        self.assertIn("#SBATCH --ntasks=1", script)
        self.assertIn("#SBATCH --cpus-per-task=32", script)
        self.assertIn("#SBATCH --nodelist=n100", script)
        self.assertIn("while true; do sleep 60", script)

    def test_allocation_script_can_request_gpu_model(self) -> None:
        allocation = {
            "id": 13,
            "remote_dir": "/remote/allocation",
            "stdout_path": "/remote/allocation/allocation-%j.out",
            "stderr_path": "/remote/allocation/allocation-%j.err",
            "total_cpus": 48,
            "total_memory_mb": 65536,
            "total_gpus": 1,
            "gpu_model": "a6000ada",
            "partition": "gpu3",
            "node_name": "gpu-node",
        }
        script = build_allocation_script(allocation, "48:00:00")
        self.assertIn("#SBATCH --gres=gpu:a6000ada:1", script)

    def test_allocation_script_can_request_multiple_partitions(self) -> None:
        allocation = {
            "id": 14,
            "remote_dir": "/remote/allocation",
            "stdout_path": "/remote/allocation/allocation-%j.out",
            "stderr_path": "/remote/allocation/allocation-%j.err",
            "total_cpus": 16,
            "total_memory_mb": 131072,
            "total_gpus": 4,
            "gpu_model": "a6000",
            "partition": "gpu4,gpu5",
            "node_name": "",
        }
        script = build_allocation_script(allocation, "48:00:00")
        self.assertIn("#SBATCH --partition=gpu4,gpu5", script)
        self.assertIn("#SBATCH --gres=gpu:a6000:4", script)

    def test_apply_env_profile_prepends_account_setup(self) -> None:
        account = AccountConfig(
            "a",
            "host",
            22,
            "a",
            "key",
            "/work",
            env_profiles={"pyaedt": "source ~/miniconda3/etc/profile.d/conda.sh\nconda activate pyaedt"},
        )
        payload = apply_env_profile({"env_profile": "pyaedt", "env_setup": "module load ansys"}, account)
        self.assertIn("conda activate pyaedt\nmodule load ansys", payload["env_setup"])

    def test_git_task_command_uses_account_workspace_placeholder(self) -> None:
        command = build_git_task_command("git@example.com:repo.git", "main", "run.py", "--x 1")
        self.assertIn(ACCOUNT_WORKSPACE_PLACEHOLDER, command)
        self.assertIn(TASK_ID_PLACEHOLDER, command)
        self.assertIn("git clone git@example.com:repo.git", command)
        self.assertIn("git checkout main", command)
        self.assertIn("python run.py --x 1", command)
        account = AccountConfig("a", "host", 22, "a", "key", "~/scheduler")
        task = resolve_task_placeholders({"id": 42, "remote_cwd": ACCOUNT_WORKSPACE_PLACEHOLDER, "command": command}, account)
        self.assertEqual(task["remote_cwd"], "~/scheduler")
        self.assertIn("$HOME/scheduler/git_tasks", task["command"])
        self.assertIn("task-42", task["command"])

    def test_git_credential_matches_alias_and_payload_exposes_only_id(self) -> None:
        credential = GitCredentialConfig(
            id="kakao-loco-bot",
            url_patterns=["*Schwalbe262/kakao-loco-bot*"],
            clone_url="git@github.com:Schwalbe262/kakao-loco-bot.git",
            source_account="r1jae262",
            source_private_key_path="~/.ssh/kakao_loco_bot_deploy",
        )
        repo_url = "git@github.com-kakao-loco-bot:Schwalbe262/kakao-loco-bot.git"
        self.assertEqual(find_git_credential([credential], repo_url).id, "kakao-loco-bot")
        payload = git_task_payload(repo_url, "main", "run.py", credential=credential)
        self.assertIn('"git_credential_id":"kakao-loco-bot"', payload)
        self.assertNotIn("kakao_loco_bot_deploy", payload)
        command = build_git_task_command(credential.clone_url, "main", "run.py")
        self.assertIn("git clone git@github.com:Schwalbe262/kakao-loco-bot.git", command)
        self.assertNotIn("github.com-kakao-loco-bot", command)

    def test_task_script_uses_remote_cwd_and_command(self) -> None:
        task = {
            "remote_cwd": "~/case",
            "env_setup": "module load ansys",
            "command": "ansys -b -i input.dat",
        }
        script = build_task_script(task)
        self.assertIn("cd $HOME/case", script)
        self.assertIn("module load ansys", script)
        self.assertIn("ansys -b -i input.dat", script)

    def test_task_script_exports_git_ssh_command(self) -> None:
        script = build_task_script(
            {
                "remote_cwd": "~/case",
                "git_ssh_command": "ssh -i ~/task/key -o IdentitiesOnly=yes",
                "command": "git clone git@github.com:org/private.git repo",
            }
        )
        self.assertIn("export GIT_TERMINAL_PROMPT=0", script)
        self.assertIn("export GIT_SSH_COMMAND=", script)
        self.assertIn("git clone git@github.com:org/private.git repo", script)

    def test_task_script_writes_payload_json(self) -> None:
        task = {
            "remote_cwd": "~/case",
            "env_setup": "",
            "command": "python run.py",
            "payload_json": '{"route":"ICN-SFO"}',
            "payload_path": "/remote/task-1/payload.json",
        }
        script = build_task_script(task)
        self.assertIn("SLURM_SCHEDULER_PAYLOAD_PATH=/remote/task-1/payload.json", script)
        self.assertIn("path.write_text", script)
        self.assertLess(script.index("path.write_text"), script.index("cd $HOME/case"))

    def test_srun_attach_command_targets_existing_allocation(self) -> None:
        task = {"cpus": 4, "memory_mb": 8192}
        allocation = {"slurm_job_id": "12345"}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("srun --jobid=12345", command)
        self.assertIn("--nodes=1", command)
        self.assertIn("--ntasks=1", command)
        self.assertIn("--cpus-per-task=4", command)
        self.assertIn("--mem=8192M", command)
        self.assertIn("--exclusive", command)

    def test_srun_attach_command_keeps_gpu_task_exclusive_when_cpu_is_tight(self) -> None:
        task = {"cpus": 4, "memory_mb": 8192, "gpus": 1}
        allocation = {"slurm_job_id": "12345", "free_cpus": 0, "gpu_model": "a6000"}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("--gres=gpu:a6000:1", command)
        self.assertIn("--exclusive", command)
        self.assertNotIn("--overlap", command)

    def test_srun_attach_command_overlaps_vllm_service_task(self) -> None:
        task = {
            "name": "factorio-vllm-service-qwen-p8000",
            "command": "SERVICE_DURATION_SECONDS=43200 python -m vllm.entrypoints.openai.api_server",
            "cpus": 1,
            "memory_mb": 32768,
            "gpus": 1,
            "gpu_model": "a6000",
        }
        allocation = {"slurm_job_id": "12345", "gpu_model": "a6000"}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("--gres=gpu:a6000:1", command)
        self.assertIn("--overlap", command)
        self.assertNotIn("--exclusive", command)

    def test_srun_attach_command_overlaps_same_node_cpu_client_when_capacity_is_tight(self) -> None:
        task = {"cpus": 1, "memory_mb": 32768, "gpus": 0, "same_node_as_task_id": 100}
        allocation = {"slurm_job_id": "12345", "free_cpus": 0, "free_memory_mb": 0}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("--overlap", command)
        self.assertNotIn("--exclusive", command)

    def test_srun_attach_command_overlaps_same_node_cpu_client_even_with_free_capacity(self) -> None:
        task = {"cpus": 1, "memory_mb": 1024, "gpus": 0, "same_node_as_task_id": 100}
        allocation = {"slurm_job_id": "12345", "free_cpus": 16, "free_memory_mb": 65536}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("--overlap", command)
        self.assertNotIn("--exclusive", command)

    def test_srun_attach_command_overlaps_fea_bursty_task(self) -> None:
        task = {"cpus": 4, "memory_mb": 8192, "scheduling_profile": SchedulingProfile.FEA_BURSTY.value}
        allocation = {"slurm_job_id": "12345", "free_cpus": 64}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("--cpus-per-task=4", command)
        self.assertIn("--mem=8192M", command)
        self.assertIn("--overlap", command)
        self.assertNotIn("--exclusive", command)

    def test_background_wrapper_uses_new_session_for_process_group_cancel(self) -> None:
        command = background_wrapper_command("srun --jobid=1 bash /remote/task.sh", "/remote/wrapper.log")
        self.assertIn("nohup setsid bash -lc", command)
        self.assertIn("& echo $!", command)

    def test_cancel_process_group_command_terminates_wrapper_group_and_fallback_children(self) -> None:
        command = cancel_process_group_command("1234", term_grace_seconds=0)
        self.assertIn('kill -TERM -- "-$pid"', command)
        self.assertIn('pkill -TERM -P "$pid"', command)
        self.assertIn('kill -KILL -- "-$pid"', command)
        self.assertIn('pkill -KILL -P "$pid"', command)

    def test_allocation_script_does_not_request_slurm_exclusive_for_scheduler_exclusive_pool(self) -> None:
        allocation = {
            "total_cpus": 12,
            "total_memory_mb": 98304,
            "partition": "cpu1",
            "node_name": "",
            "total_gpus": 0,
            "gpu_model": "",
            "exclusive_node": 1,
            "remote_dir": "/remote/allocation",
            "stdout_path": "/remote/allocation/out",
            "stderr_path": "/remote/allocation/err",
        }
        script = build_allocation_script(allocation, "48:00:00")
        self.assertIn("#SBATCH --cpus-per-task=12", script)
        self.assertNotIn("#SBATCH --exclusive", script)

    def test_remote_execution_path_promotes_relative_path_under_home(self) -> None:
        self.assertEqual(remote_execution_path("slurm_scheduler/task-1/task.sh"), "$HOME/slurm_scheduler/task-1/task.sh")
        self.assertEqual(remote_execution_path("~/slurm_scheduler/task-1/task.sh"), "$HOME/slurm_scheduler/task-1/task.sh")
        self.assertEqual(remote_execution_path("/tmp/task.sh"), "/tmp/task.sh")

    def test_srun_attach_command_can_request_gpu(self) -> None:
        task = {"cpus": 8, "memory_mb": 16384, "gpus": 1, "gpu_model": "a6000"}
        allocation = {"slurm_job_id": "12345", "gpu_model": "a6000"}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("--gres=gpu:a6000:1", command)

    def test_partition_rank_uses_cpu_and_gpu_profiles(self) -> None:
        nodes = parse_sinfo_nodes(
            "n040|cpu1|48|768000|(null)|idle\n"
            "n107|cpu2|256|1031519|(null)|mix\n"
            "n062|gpu3|56|1024000|gpu:a6000ada:4|mix\n"
            "n101|gpu5|64|1024000|gpu:a6000:4|mix\n"
        )
        rows = [node.__dict__ for node in nodes]
        self.assertEqual(partition_rank(rows, needs_gpu=False)[0]["partition"], "cpu2")
        self.assertEqual(partition_rank(rows, needs_gpu=True)[0]["partition"], "gpu3")

    def test_parse_scontrol_nodes_tracks_used_gpus(self) -> None:
        nodes = parse_scontrol_nodes(
            "NodeName=n062 Arch=x86_64 CPUTot=56 RealMemory=1024000 "
            "Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:3(IDX:0-2) "
            "State=MIXED Partitions=gpu3\n"
        )
        self.assertEqual(nodes[0].gpu_model, "a6000ada")
        self.assertEqual(nodes[0].gpu_count, 4)
        self.assertEqual(nodes[0].gpu_used_count, 3)

    def test_parse_scontrol_nodes_tracks_alloc_tres_gpus(self) -> None:
        nodes = parse_scontrol_nodes(
            "NodeName=n002 Arch=x86_64 CPUTot=48 RealMemory=768000 "
            "Gres=gpu:rtx3090:4 State=ALLOCATED Partitions=gpu1 "
            "CfgTRES=cpu=48,mem=750G,billing=48,gres/gpu=4,gres/gpu:rtx3090=4 "
            "AllocTRES=cpu=48,gres/gpu=2,gres/gpu:rtx3090=2\n"
        )
        self.assertEqual(nodes[0].gpu_model, "rtx3090")
        self.assertEqual(nodes[0].gpu_count, 4)
        self.assertEqual(nodes[0].gpu_used_count, 2)

    def test_parse_pestat_and_dynamic_plan(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "n001 cpu1 idle 0 48 0.25 768000 700000\n"
            "n002 cpu1 mix 40 48 42.0 768000 700000 1 user\n"
        )
        self.assertEqual(len(nodes), 2)
        plans = plan_dynamic_allocations(
            nodes,
            total_simulations=20,
            cpus_per_simulation=4,
            mem_per_simulation_gb=8,
            max_workers_per_allocation=32,
            max_allocations=2,
            partition="auto",
        )
        self.assertEqual(plans[0].node_name, "n001")
        self.assertEqual(plans[0].initial_workers, 11)
        self.assertEqual(plans[0].workers, 16)
        self.assertEqual(plans[0].total_cpus, 44)


class FakeClient:
    snapshots: dict[str, AccountSnapshot] = {}
    submitted: list[str] = []
    allocation_submits: list[str] = []
    allocation_states: dict[str, JobStatus] = {}
    task_states: dict[int, JobStatus] = {}
    attached_tasks: list[int] = []
    cancelled: list[str] = []
    cancelled_tasks: list[int] = []
    removed: list[str] = []

    def __init__(self, account: AccountConfig):
        self.account = account

    def snapshot(self, storage_used_gb: float | None = None) -> AccountSnapshot:
        return self.snapshots[self.account.name]

    def storage_used_gb(self) -> float | None:
        return None

    def submit(self, job: dict) -> dict[str, str]:
        self.submitted.append(self.account.name)
        return {
            "slurm_job_id": "777",
            "remote_job_dir": "/remote/job",
            "stdout_path": "/remote/job/slurm-777.out",
            "stderr_path": "/remote/job/slurm-777.err",
        }

    def submit_allocation(self, allocation: dict, time_limit: str) -> dict[str, str]:
        slurm_id = f"alloc-{allocation['id']}"
        self.allocation_submits.append(self.account.name)
        self.allocation_states[slurm_id] = JobStatus.RUNNING
        return {
            "slurm_job_id": slurm_id,
            "remote_dir": f"/remote/allocation-{allocation['id']}",
            "stdout_path": f"/remote/allocation-{allocation['id']}/out",
            "stderr_path": f"/remote/allocation-{allocation['id']}/err",
        }

    def attach_task(self, task: dict, allocation: dict) -> dict[str, str]:
        self.attached_tasks.append(int(task["id"]))
        self.task_states[task["id"]] = JobStatus.RUNNING
        return {
            "remote_dir": f"/remote/task-{task['id']}",
            "stdout_path": f"/remote/task-{task['id']}/stdout.log",
            "stderr_path": f"/remote/task-{task['id']}/stderr.log",
            "exit_code_path": f"/remote/task-{task['id']}/exit_code",
            "wrapper_pid": str(1000 + task["id"]),
        }

    def task_state(self, task: dict) -> JobStatus:
        return self.task_states.get(task["id"], JobStatus.RUNNING)

    def task_exit_code(self, task: dict) -> int | None:
        state = self.task_states.get(task["id"])
        if state == JobStatus.COMPLETED:
            return 0
        if state == JobStatus.FAILED:
            return 1
        return None

    def state(self, slurm_job_id: str) -> JobStatus:
        if slurm_job_id in self.allocation_states:
            return self.allocation_states[slurm_job_id]
        return JobStatus.COMPLETED

    def pending_reason(self, slurm_job_id: str) -> str:
        return self.pending_reasons.get(slurm_job_id, "")

    def allocation_node_name(self, slurm_job_id: str) -> str:
        return ""

    def cancel(self, slurm_job_id: str) -> None:
        self.cancelled.append(slurm_job_id)

    def cancel_task(self, task: dict, allocation_job_id: str = "") -> None:
        self.cancelled_tasks.append(int(task["id"]))

    def remove_tree(self, remote_path: str) -> None:
        self.removed.append(remote_path)

    def remove_trees(self, remote_paths: list[str]) -> None:
        self.removed.extend(remote_paths)

    def job_states(self, slurm_job_ids: list[str]) -> dict[str, JobStateInfo]:
        out = {}
        for slurm_job_id in slurm_job_ids:
            status = self.state(slurm_job_id)
            out[slurm_job_id] = JobStateInfo(
                status=status,
                pending_reason=self.pending_reasons.get(slurm_job_id, "") if status == JobStatus.SUBMITTED else "",
                node_name=self.allocation_node_name(slurm_job_id) if status == JobStatus.RUNNING else "",
            )
        return out

    def task_probes(self, tasks: list[dict]) -> dict[int, TaskProbe]:
        out = {}
        for task in tasks:
            status = self.task_state(task)
            exit_code = self.task_exit_code(task) if status in {JobStatus.COMPLETED, JobStatus.FAILED} else None
            out[int(task["id"])] = TaskProbe(status=status, exit_code=exit_code)
        return out


class BlockingAttachClient(FakeClient):
    attach_started = threading.Event()
    release_attach = threading.Event()

    def attach_task(self, task: dict, allocation: dict) -> dict[str, str]:
        self.attach_started.set()
        self.release_attach.wait(timeout=5)
        return super().attach_task(task, allocation)


class AttachFailureClient(FakeClient):
    def attach_task(self, task: dict, allocation: dict) -> dict[str, str]:
        raise RemoteExecutionError(
            "ssh exec failed",
            {
                "remote_dir": f"/remote/task-{task['id']}",
                "stdout_path": f"/remote/task-{task['id']}/stdout.log",
                "stderr_path": f"/remote/task-{task['id']}/stderr.log",
                "exit_code_path": f"/remote/task-{task['id']}/exit_code",
            },
        )


class SubmitFailureClient(FakeClient):
    def submit(self, job: dict) -> dict[str, str]:
        raise RemoteExecutionError(
            "git clone failed",
            {
                "remote_job_dir": f"/remote/job-{job['id']}",
                "stdout_path": f"/remote/job-{job['id']}/submit.stdout.log",
                "stderr_path": f"/remote/job-{job['id']}/submit.stderr.log",
            },
        )


class FailingCancelClient(FakeClient):
    def cancel(self, slurm_job_id: str) -> None:
        raise RuntimeError("scancel failed")


class PartialSnapshotFailureClient(FakeClient):
    def snapshot(self, storage_used_gb: float | None = None) -> AccountSnapshot:
        if self.account.name == "b":
            raise RuntimeError("squeue unavailable")
        return super().snapshot(storage_used_gb=storage_used_gb)


class SchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [
            AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10),
            AccountConfig("b", "host", 22, "b", "key", "/work", 4, 10, 10),
        ]
        FakeClient.submitted = []
        FakeClient.allocation_submits = []
        FakeClient.allocation_states = {}
        FakeClient.pending_reasons = {}
        FakeClient.task_states = {}
        FakeClient.attached_tasks = []
        FakeClient.cancelled = []
        FakeClient.cancelled_tasks = []
        FakeClient.removed = []
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=3, pending=0, max_running=4, max_pending=10, max_total=10),
            "b": AccountSnapshot("b", running=1, pending=1, max_running=4, max_pending=10, max_total=10),
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def create_fea_allocation(
        self,
        node_name: str = "n001",
        total_cpus: int = 64,
        state: str = AllocationStatus.ACTIVE.value,
    ) -> int:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name=node_name,
            total_cpus=total_cpus,
            total_memory_mb=262144,
        )
        self.db.update_allocation(allocation_id, state=state, slurm_job_id=f"alloc-{allocation_id}")
        return allocation_id

    def create_running_fea_tasks(self, allocation_id: int, count: int, cpus: int = 4) -> list[int]:
        task_ids = []
        for index in range(count):
            task_id = self.db.create_task(
                TaskCreate(
                    f"running-fea-{index}",
                    "~/case",
                    "run",
                    cpus=cpus,
                    memory_mb=8192,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                allocation_id=allocation_id,
                account_name="a",
                started_at="CURRENT_TIMESTAMP",
            )
            task_ids.append(task_id)
        return task_ids

    def create_queued_fea_task(self, name: str = "queued-fea") -> int:
        return self.db.create_task(
            TaskCreate(
                name,
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
            )
        )

    def test_list_allocations_with_live_keeps_old_active_allocations_beyond_recent_limit(self) -> None:
        active_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="n106",
            total_cpus=3,
            total_memory_mb=926829,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(active_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="old-active")
        closed_ids = []
        for index in range(5):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu1",
                node_name="",
                total_cpus=8,
                total_memory_mb=32768,
            )
            self.db.update_allocation(allocation_id, state=AllocationStatus.CLOSED.value, slurm_job_id=f"closed-{index}")
            closed_ids.append(allocation_id)

        recent = self.db.list_allocations(limit=2)
        self.assertNotIn(active_id, {int(item["id"]) for item in recent})

        visible = self.db.list_allocations_with_live(limit=2)
        visible_ids = {int(item["id"]) for item in visible}
        self.assertIn(active_id, visible_ids)
        self.assertTrue(set(closed_ids[-2:]).issubset(visible_ids))

    def test_list_tasks_with_active_keeps_old_running_tasks_beyond_recent_limit(self) -> None:
        running_id = self.db.create_task(TaskCreate("old-service", "~/work", "serve", gpus=1, gpu_model="a6000"))
        self.db.update_task(running_id, status=TaskStatus.RUNNING.value)
        finished_ids = []
        for index in range(5):
            task_id = self.db.create_task(TaskCreate(f"finished-{index}", "~/work", "run"))
            self.db.update_task(task_id, status=TaskStatus.COMPLETED.value)
            finished_ids.append(task_id)

        recent = self.db.list_tasks(limit=2)
        self.assertNotIn(running_id, {int(item["id"]) for item in recent})

        visible = self.db.list_tasks_with_active(limit=2)
        visible_ids = {int(item["id"]) for item in visible}
        self.assertIn(running_id, visible_ids)
        self.assertTrue(set(finished_ids[-2:]).issubset(visible_ids))

    def test_list_finished_tasks_can_filter_by_name_before_limit(self) -> None:
        for index in range(5):
            task_id = self.db.create_task(TaskCreate(f"other-finished-{index}", "~/work", "run"))
            self.db.update_task(task_id, status=TaskStatus.COMPLETED.value)
        match_ids = []
        for index in range(3):
            task_id = self.db.create_task(TaskCreate(f"ipmsm-finished-{index}", "~/work", "run"))
            self.db.update_task(task_id, status=TaskStatus.COMPLETED.value)
            match_ids.append(task_id)

        visible = self.db.list_tasks_by_statuses(
            [TaskStatus.COMPLETED.value],
            limit=50,
            name_contains="ipmsm",
        )

        self.assertEqual({int(item["id"]) for item in visible}, set(match_ids))
        self.assertEqual(self.db.count_tasks_by_statuses([TaskStatus.COMPLETED.value], name_contains="ipmsm"), 3)

    def test_choose_account_prefers_freer_account(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account().name, "b")

    def test_choose_account_respects_required_capability(self) -> None:
        accounts = [
            AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10, capabilities=["conda:pyaedt"]),
            AccountConfig("b", "host", 22, "b", "key", "/work", 4, 10, 10),
        ]
        scheduler = Scheduler(self.db, accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account(required_capability="conda:pyaedt").name, "a")

    def test_choose_account_uses_synced_conda_overlay_capability(self) -> None:
        self.db.upsert_account_env_overlay("b", "pyaedt", "/work/miniconda3/envs/pyaedt", sync_job_id=1)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        account = scheduler.choose_account(required_capability="conda:pyaedt", env_profile="pyaedt")
        self.assertEqual(account.name, "b")

    def test_dynamic_env_profile_prepends_synced_overlay_setup(self) -> None:
        self.db.upsert_account_env_overlay("a", "pyaedt", "/work/miniconda3/envs/pyaedt", sync_job_id=1)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        payload = scheduler.apply_dynamic_env_profile({"env_profile": "pyaedt", "env_setup": "module load ansys"}, self.accounts[0])
        self.assertIn("conda activate pyaedt", payload["env_setup"])
        self.assertTrue(payload["env_setup"].endswith("module load ansys"))

    def test_conda_sync_command_helpers_reference_env_name(self) -> None:
        lookup = env_prefix_lookup_command("pyaedt")
        self.assertIn("conda env list --json", lookup)
        self.assertIn("pyaedt", lookup)
        self.assertIn("miniconda3", conda_bootstrap())

    def test_choose_account_respects_requested_account(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account(account_name="a").name, "a")

    def test_choose_account_accepts_ordered_account_candidates(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account(account_name="a,b").name, "a")
        self.assertEqual(scheduler.choose_account(account_name="b,a").name, "b")

    def test_submit_next_queued_job_updates_database(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.submit_next_queued_job()
        job = self.db.get_job(job_id)
        self.assertEqual(job["status"], JobStatus.SUBMITTED.value)
        self.assertEqual(job["account_name"], "b")
        self.assertEqual(job["slurm_job_id"], "777")

    def test_submit_next_queued_job_uses_requested_account(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py", account_name="a"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.submit_next_queued_job()
        job = self.db.get_job(job_id)
        self.assertEqual(job["status"], JobStatus.SUBMITTED.value)
        self.assertEqual(job["account_name"], "a")

    def test_submit_failure_keeps_remote_submit_log_paths(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py", account_name="a"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=SubmitFailureClient)
        scheduler.submit_next_queued_job()
        job = self.db.get_job(job_id)
        self.assertEqual(job["status"], JobStatus.FAILED.value)
        self.assertEqual(job["failure_message"], "git clone failed")
        self.assertEqual(job["remote_job_dir"], f"/remote/job-{job_id}")
        self.assertEqual(job["stdout_path"], f"/remote/job-{job_id}/submit.stdout.log")
        self.assertEqual(job["stderr_path"], f"/remote/job-{job_id}/submit.stderr.log")

    def test_choose_account_respects_total_job_limit(self) -> None:
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=5, pending=5, max_running=10, max_pending=10, max_total=10),
            "b": AccountSnapshot("b", running=4, pending=5, max_running=10, max_pending=10, max_total=10),
        }
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account().name, "b")

    def test_token_usage_summary(self) -> None:
        self.db.create_token_usage("codex", "slurm_scheduler", input_tokens=10, output_tokens=5, reset_cycle="2026-W24")
        self.db.create_token_usage("codex", "slurm_scheduler", total_tokens=20, reset_cycle="2026-W24")
        summary = self.db.token_usage_summary()
        self.assertEqual(summary[0]["provider"], "codex")
        self.assertEqual(summary[0]["project"], "slurm_scheduler")
        self.assertEqual(summary[0]["total_tokens"], 35)

    def test_cleanup_removes_only_safe_finished_task_artifacts(self) -> None:
        safe_id = self.db.create_task(TaskCreate("safe", "/case", "run"))
        unsafe_id = self.db.create_task(TaskCreate("unsafe", "/case", "run"))
        self.db.update_task(
            safe_id,
            status=TaskStatus.COMPLETED.value,
            account_name="a",
            remote_dir="/work/task-1-1000",
            stdout_path="/work/task-1-1000/stdout.log",
            stderr_path="/work/task-1-1000/stderr.log",
            exit_code_path="/work/task-1-1000/exit_code",
            wrapper_pid="123",
            finished_at=days_ago(7),
        )
        self.db.update_task(
            unsafe_id,
            status=TaskStatus.FAILED.value,
            account_name="a",
            remote_dir="/work/project",
            stdout_path="/work/project/stdout.log",
            stderr_path="/work/project/stderr.log",
            finished_at=days_ago(7),
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cleanup_interval_seconds=0,
            cleanup_finished_task_ttl_seconds=0,
        )
        scheduler.cleanup_remote_artifacts_if_due()
        self.assertEqual(FakeClient.removed, ["/work/task-1-1000"])
        self.assertEqual(self.db.get_task(safe_id)["remote_dir"], "")
        self.assertEqual(self.db.get_task(unsafe_id)["remote_dir"], "/work/project")

    def test_cleanup_finds_old_finished_task_outside_recent_task_limit(self) -> None:
        old_id = self.db.create_task(TaskCreate("old", "/case", "run"))
        self.db.update_task(
            old_id,
            status=TaskStatus.COMPLETED.value,
            account_name="a",
            remote_dir="/work/task-old-1000",
            stdout_path="/work/task-old-1000/stdout.log",
            stderr_path="/work/task-old-1000/stderr.log",
            exit_code_path="/work/task-old-1000/exit_code",
            finished_at=days_ago(7),
        )
        with self.db.connect() as conn:
            conn.executemany(
                "INSERT INTO tasks (name, remote_cwd, command, status) VALUES (?, ?, ?, ?)",
                [(f"queued-{index}", "/case", "run", TaskStatus.QUEUED.value) for index in range(5001)],
            )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cleanup_interval_seconds=0,
            cleanup_finished_task_ttl_seconds=0,
        )
        scheduler.cleanup_remote_artifacts_if_due()
        self.assertEqual(FakeClient.removed, ["/work/task-old-1000"])
        self.assertEqual(self.db.get_task(old_id)["remote_dir"], "")

    def test_cleanup_removes_finished_job_and_closed_allocation_artifacts(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py", account_name="a"))
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_job(
            job_id,
            status=JobStatus.FAILED.value,
            remote_job_dir="/work/job-1-1000",
            stdout_path="/work/job-1-1000/out",
            stderr_path="/work/job-1-1000/err",
            finished_at=days_ago(7),
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.CLOSED.value,
            remote_dir="/work/allocation-1-1000",
            stdout_path="/work/allocation-1-1000/out",
            stderr_path="/work/allocation-1-1000/err",
            closed_at=days_ago(7),
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cleanup_interval_seconds=0,
            cleanup_finished_job_ttl_seconds=0,
            cleanup_closed_allocation_ttl_seconds=0,
        )
        scheduler.cleanup_remote_artifacts_if_due()
        self.assertEqual(FakeClient.removed, ["/work/job-1-1000", "/work/allocation-1-1000"])
        self.assertEqual(self.db.get_job(job_id)["remote_job_dir"], "")
        self.assertEqual(self.db.get_allocation(allocation_id)["remote_dir"], "")

    def test_cancel_task_marks_queued_task_cancelled(self) -> None:
        task_id = self.db.create_task(TaskCreate("queued", "~/case", "run"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.cancel_task(task_id)
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.CANCELLED.value)
        self.assertEqual(FakeClient.cancelled_tasks, [])

    def test_list_tasks_by_statuses_keeps_running_visible_independent_of_recent_rows(self) -> None:
        running_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        self.db.update_task(running_id, status=TaskStatus.RUNNING.value)
        for index in range(20):
            done_id = self.db.create_task(TaskCreate(f"done-{index}", "~/case", "run"))
            self.db.update_task(done_id, status=TaskStatus.FAILED.value)
        recent = self.db.list_tasks(limit=5)
        self.assertNotIn(running_id, [int(task["id"]) for task in recent])
        running = self.db.list_tasks_by_statuses([TaskStatus.RUNNING.value], limit=5000)
        self.assertEqual([int(task["id"]) for task in running], [running_id])
        self.assertEqual(self.db.count_tasks_by_statuses([TaskStatus.FAILED.value]), 20)

    def test_gpu_task_attaches_before_higher_priority_cpu_backlog(self) -> None:
        self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=65536,
        )
        gpu_allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu1",
            node_name="g001",
            total_cpus=3,
            total_memory_mb=65536,
            total_gpus=2,
            gpu_model="a6000",
        )
        self.db.update_allocation(
            gpu_allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="gpu-alloc-1",
            free_cpus=3,
            free_memory_mb=65536,
            free_gpus=2,
        )
        cpu_task_id = self.db.create_task(TaskCreate("cpu-backlog", "~/cpu", "run", cpus=16, memory_mb=32768, priority=70))
        gpu_task_id = self.db.create_task(
            TaskCreate("gpu-work", "~/gpu", "run", cpus=3, memory_mb=32768, gpus=1, gpu_model="a6000")
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        self.assertEqual(FakeClient.attached_tasks[0], gpu_task_id)
        self.assertEqual(self.db.get_task(gpu_task_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(cpu_task_id)["status"], TaskStatus.QUEUED.value)

    def test_snapshot_failure_for_one_account_does_not_block_other_accounts(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=PartialSnapshotFailureClient)
        snapshots = scheduler.snapshots()
        self.assertEqual([snapshot.account_name for snapshot in snapshots], ["a"])

    def test_cancel_task_kills_running_wrapper(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            account_name="a",
            allocation_id=allocation_id,
            wrapper_pid="1234",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.cancel_task(task_id)
        task = self.db.get_task(task_id)
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(task["status"], TaskStatus.CANCELLED.value)
        self.assertEqual(FakeClient.cancelled_tasks, [task_id])
        self.assertEqual(allocation["free_cpus"], allocation["total_cpus"])

    def test_request_cancel_task_returns_fast_status_shape(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            account_name="a",
            allocation_id=allocation_id,
            wrapper_pid="1234",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        response = scheduler.request_cancel_task(task_id)
        self.assertEqual(response["ok"], True)
        self.assertEqual(response["id"], task_id)
        self.assertEqual(response["previous_status"], TaskStatus.RUNNING.value)
        self.assertEqual(response["status"], TaskStatus.CANCELLED.value)
        self.assertEqual(self.db.get_task(task_id)["status"], TaskStatus.CANCELLED.value)
        deadline = time.monotonic() + 1
        while task_id not in FakeClient.cancelled_tasks and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertIn(task_id, FakeClient.cancelled_tasks)

    def test_cancel_tasks_filters_by_name_and_status(self) -> None:
        first = self.db.create_task(TaskCreate("crypto-sweep-a", "~/case", "run"))
        second = self.db.create_task(TaskCreate("crypto-sweep-b", "~/case", "run"))
        third = self.db.create_task(TaskCreate("other", "~/case", "run"))
        self.db.update_task(second, status=TaskStatus.COMPLETED.value, finished_at=days_ago(7))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        cancelled = scheduler.cancel_tasks(name_contains="crypto-sweep")
        self.assertEqual(cancelled, [first])
        self.assertEqual(self.db.get_task(first)["status"], TaskStatus.CANCELLED.value)
        self.assertEqual(self.db.get_task(second)["status"], TaskStatus.COMPLETED.value)
        self.assertEqual(self.db.get_task(third)["status"], TaskStatus.QUEUED.value)

    def test_request_close_allocation_closes_idle_pool(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        response = scheduler.request_close_allocation(allocation_id)
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(response["ok"], True)
        self.assertEqual(response["previous_state"], AllocationStatus.WARM.value)
        self.assertEqual(response["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(response["closed_task_ids"], [])
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(allocation["drain_reason"], "manual close")
        self.assertEqual(FakeClient.cancelled, ["alloc-1"])

    def test_request_close_allocation_keeps_pool_live_when_cancel_fails(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FailingCancelClient)
        response = scheduler.request_close_allocation(allocation_id)
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(response["ok"], False)
        self.assertEqual(response["previous_state"], AllocationStatus.WARM.value)
        self.assertEqual(response["state"], AllocationStatus.WARM.value)
        self.assertEqual(allocation["state"], AllocationStatus.WARM.value)
        self.assertEqual(allocation["failure_message"], "scancel failed")
        self.assertIsNone(allocation["closed_at"])

    def test_request_close_allocation_rejects_gpu_warm_pool_without_dashboard_authority(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu4,gpu5",
            node_name="",
            total_cpus=16,
            total_memory_mb=131072,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="alloc-gpu",
            drain_reason="minimum GPU warm pool a6000",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        for force in (False, True):
            with self.subTest(force=force):
                with self.assertRaises(RuntimeError):
                    scheduler.request_close_allocation(allocation_id, force=force)
                allocation = self.db.get_allocation(allocation_id)
                self.assertEqual(allocation["state"], AllocationStatus.PENDING.value)
                self.assertEqual(allocation["drain_reason"], "minimum GPU warm pool a6000")
                self.assertEqual(FakeClient.cancelled, [])
        response = scheduler.request_close_allocation(allocation_id, allow_protected=True)
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(response["ok"], True)
        self.assertEqual(response["allow_protected"], True)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(allocation["drain_reason"], "manual close")
        self.assertEqual(FakeClient.cancelled, ["alloc-gpu"])

    def test_gpu_prewarm_toggle_overrides_config_and_persists(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, gpu_prewarm_enabled=True)
        self.assertTrue(scheduler.gpu_prewarm_enabled)
        scheduler.set_gpu_prewarm_enabled(False)
        self.assertFalse(scheduler.gpu_prewarm_enabled)
        rebuilt = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, gpu_prewarm_enabled=True)
        self.assertFalse(rebuilt.gpu_prewarm_enabled)
        scheduler.set_gpu_prewarm_enabled(True)
        self.assertTrue(rebuilt.gpu_prewarm_enabled)

    def test_request_close_allocation_rejects_active_tasks_without_force(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            account_name="a",
            allocation_id=allocation_id,
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        with self.assertRaises(RuntimeError):
            scheduler.request_close_allocation(allocation_id)
        self.assertEqual(self.db.get_allocation(allocation_id)["state"], AllocationStatus.ACTIVE.value)
        self.assertEqual(self.db.get_task(task_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(FakeClient.cancelled, [])

    def test_request_close_allocation_force_fails_active_tasks(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            account_name="a",
            allocation_id=allocation_id,
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        response = scheduler.request_close_allocation(allocation_id, force=True)
        self.assertEqual(response["ok"], True)
        self.assertEqual(response["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(response["closed_task_ids"], [task_id])
        self.assertEqual(self.db.get_allocation(allocation_id)["state"], AllocationStatus.CLOSED.value)
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.FAILED.value)
        self.assertEqual(task["failure_message"], "allocation manually closed")
        self.assertEqual(FakeClient.cancelled, ["alloc-1"])

    def test_task_create_stores_api_operational_fields(self) -> None:
        task_id = self.db.create_task(
            TaskCreate(
                "flight",
                "~/flight",
                "python worker.py",
                required_capability="flight-crawl",
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                priority=7,
                timeout_seconds=30,
                dedupe_key="flight:ICN:SFO",
                max_workers_per_node=200,
                payload_json='{"from":"ICN","to":"SFO"}',
            )
        )
        task = self.db.get_task(task_id)
        self.assertEqual(task["required_capability"], "flight-crawl")
        self.assertEqual(task["scheduling_profile"], SchedulingProfile.FEA_BURSTY.value)
        self.assertEqual(task["priority"], 7)
        self.assertEqual(task["timeout_seconds"], 30)
        self.assertEqual(task["dedupe_key"], "flight:ICN:SFO")
        self.assertEqual(task["max_workers_per_node"], 200)
        self.assertEqual(task["payload_json"], '{"from":"ICN","to":"SFO"}')
        self.assertEqual(self.db.find_active_task_by_dedupe_key("flight:ICN:SFO")["id"], task_id)

    def test_maintains_minimum_warm_allocation(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=4)
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        self.assertEqual(len(allocations), 1)
        self.assertEqual(allocations[0]["state"], AllocationStatus.PENDING.value)
        self.assertEqual(FakeClient.allocation_submits, ["b"])

    def test_warm_pool_can_prefer_configured_account(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=8,
            warm_pool_preferred_accounts=["a"],
        )
        scheduler.maintain_allocation_pool()
        self.assertEqual(FakeClient.allocation_submits, ["a"])

    def test_pending_allocation_becomes_warm(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["state"], AllocationStatus.WARM.value)

    def test_pending_allocation_reason_is_recorded(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        FakeClient.allocation_states[allocation["slurm_job_id"]] = JobStatus.SUBMITTED
        FakeClient.pending_reasons[allocation["slurm_job_id"]] = "(Resources)"
        scheduler.refresh_allocations()
        allocation = self.db.get_allocation(allocation["id"])
        self.assertEqual(allocation["state"], AllocationStatus.PENDING.value)
        self.assertEqual(allocation["pending_reason"], "(Resources)")

    def test_stale_pending_gpu_allocation_is_cancelled_and_backed_off(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            allocation_pending_timeout_seconds=1,
            allocation_pending_backoff_seconds=3600,
            gpu_prewarm_pinned_pending_timeout_seconds=0,
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        FakeClient.allocation_states[allocation["slurm_job_id"]] = JobStatus.SUBMITTED
        self.db.update_allocation(
            allocation["id"],
            submitted_at="2000-01-01 00:00:00",
            pending_reason="(Resources)",
        )
        scheduler.apply_allocation_lifecycle()
        closed = self.db.get_allocation(allocation["id"])
        self.assertEqual(closed["state"], AllocationStatus.CLOSED.value)
        self.assertIn("pending timeout", closed["drain_reason"])
        self.assertEqual(FakeClient.cancelled, [allocation["slurm_job_id"]])

        scheduler.maintain_allocation_pool()
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(live, [])
        self.assertTrue(scheduler.allocation_pool_in_backoff("gpu:a6000"))

    def test_stale_pending_a6000_warm_pool_priority_is_not_cancelled(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="",
            total_cpus=16,
            total_memory_mb=16384,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            drain_reason="minimum GPU warm pool a6000",
            pending_reason="(Priority)",
            submitted_at="2000-01-01 00:00:00",
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            allocation_pending_timeout_seconds=1,
            allocation_pending_backoff_seconds=3600,
        )
        scheduler.apply_allocation_lifecycle()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.PENDING.value)
        self.assertEqual(FakeClient.cancelled, [])
        self.assertFalse(scheduler.allocation_pool_in_backoff("gpu:a6000"))

    def test_assigns_task_to_warm_allocation(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        task_id = self.db.create_task(TaskCreate("ansys", "~/case", "ansys -b", cpus=4, memory_mb=2048))
        scheduler.assign_queued_tasks()
        task = self.db.get_task(task_id)
        allocation = self.db.get_allocation(task["allocation_id"])
        self.assertEqual(task["status"], TaskStatus.RUNNING.value)
        self.assertEqual(allocation["free_cpus"], 4)

    def test_gpu_task_accepts_ordered_gpu_model_candidates(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu4",
            node_name="gpu-a6000",
            total_cpus=16,
            total_memory_mb=65536,
            total_gpus=2,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="gpu-job")
        task_id = self.db.create_task(
            TaskCreate("gpu-task", "~/case", "run", cpus=4, memory_mb=2048, gpus=1, gpu_model="a6000ada,a6000")
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.RUNNING.value)
        self.assertEqual(task["allocation_id"], allocation_id)

    def test_attach_failure_keeps_remote_log_paths(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=AttachFailureClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        task_id = self.db.create_task(TaskCreate("large-payload", "~/case", "x" * 900_000, cpus=4, memory_mb=2048))
        scheduler.assign_queued_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.FAILED.value)
        self.assertEqual(task["failure_message"], "ssh exec failed")
        self.assertEqual(task["remote_dir"], f"/remote/task-{task_id}")
        self.assertEqual(task["stdout_path"], f"/remote/task-{task_id}/stdout.log")
        self.assertEqual(task["stderr_path"], f"/remote/task-{task_id}/stderr.log")

    def test_assign_queued_tasks_skips_blocked_head_task(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        blocked_id = self.db.create_task(
            TaskCreate("blocked-gpu", "~/case", "run-gpu", cpus=4, memory_mb=2048, gpus=1, gpu_model="a6000ada")
        )
        ready_id = self.db.create_task(TaskCreate("ready-cpu", "~/case", "run-cpu", cpus=4, memory_mb=2048))
        scheduler.assign_queued_tasks()
        blocked = self.db.get_task(blocked_id)
        ready = self.db.get_task(ready_id)
        self.assertEqual(blocked["status"], TaskStatus.QUEUED.value)
        self.assertEqual(ready["status"], TaskStatus.RUNNING.value)

    def test_assign_queued_tasks_prefers_higher_priority(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=4)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        low_id = self.db.create_task(TaskCreate("low", "~/case", "run", cpus=4, memory_mb=2048, priority=0))
        high_id = self.db.create_task(TaskCreate("high", "~/case", "run", cpus=4, memory_mb=2048, priority=10))
        scheduler.assign_queued_tasks()
        self.assertEqual(self.db.get_task(high_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(low_id)["status"], TaskStatus.QUEUED.value)

    def test_max_workers_per_node_limits_allocation_concurrency(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        first_id = self.db.create_task(TaskCreate("first", "~/case", "run", cpus=1, memory_mb=512, max_workers_per_node=1))
        second_id = self.db.create_task(TaskCreate("second", "~/case", "run", cpus=1, memory_mb=512, max_workers_per_node=1))
        scheduler.assign_queued_tasks()
        self.assertEqual(self.db.get_task(first_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(second_id)["status"], TaskStatus.QUEUED.value)

    def test_same_node_as_task_id_co_locates_on_reference_node(self) -> None:
        reference_allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu3",
            node_name="n104",
            total_cpus=64,
            total_memory_mb=262144,
            total_gpus=4,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        other_allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n115",
            total_cpus=128,
            total_memory_mb=524288,
        )
        self.db.update_allocation(reference_allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="service-alloc")
        self.db.update_allocation(other_allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="other-alloc")
        service_id = self.db.create_task(TaskCreate("vllm", "~/svc", "serve", cpus=4, memory_mb=8192))
        self.db.update_task(
            service_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=reference_allocation_id,
            account_name="a",
            started_at="CURRENT_TIMESTAMP",
        )
        client_id = self.db.create_task(
            TaskCreate("client", "~/svc", "curl localhost:8001", cpus=1, memory_mb=1024, same_node_as_task_id=service_id)
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        client = self.db.get_task(client_id)
        self.assertEqual(client["status"], TaskStatus.RUNNING.value)
        self.assertEqual(client["allocation_id"], reference_allocation_id)

    def test_same_node_as_uses_reference_allocation_not_other_same_node_pool(self) -> None:
        reference_allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="n104",
            total_cpus=3,
            total_memory_mb=32768,
            total_gpus=1,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        other_same_node_allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n104",
            total_cpus=128,
            total_memory_mb=524288,
        )
        self.db.update_allocation(
            reference_allocation_id,
            state=AllocationStatus.ACTIVE.value,
            slurm_job_id="service-alloc",
            free_cpus=0,
            free_memory_mb=0,
            free_gpus=0,
        )
        self.db.update_allocation(other_same_node_allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="other-alloc")
        service_id = self.db.create_task(TaskCreate("vllm", "~/svc", "serve", cpus=3, memory_mb=32768, gpus=1, gpu_model="a6000"))
        self.db.update_task(
            service_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=reference_allocation_id,
            account_name="a",
            started_at="CURRENT_TIMESTAMP",
        )
        client_id = self.db.create_task(
            TaskCreate("client", "~/svc", "curl localhost:8001", cpus=1, memory_mb=1024, same_node_as_task_id=service_id)
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        client = self.db.get_task(client_id)
        self.assertEqual(client["status"], TaskStatus.RUNNING.value)
        self.assertEqual(client["allocation_id"], reference_allocation_id)

    def test_same_node_as_cpu_client_overlaps_reference_allocation_when_slots_are_full(self) -> None:
        reference_allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="n104",
            total_cpus=3,
            total_memory_mb=32768,
            total_gpus=2,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            reference_allocation_id,
            state=AllocationStatus.ACTIVE.value,
            slurm_job_id="service-alloc",
            free_cpus=0,
            free_memory_mb=0,
            free_gpus=0,
        )
        service_id = self.db.create_task(TaskCreate("vllm", "~/svc", "serve", cpus=3, memory_mb=32768, gpus=2, gpu_model="a6000"))
        self.db.update_task(
            service_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=reference_allocation_id,
            account_name="a",
            started_at="CURRENT_TIMESTAMP",
        )
        client_id = self.db.create_task(
            TaskCreate("client", "~/svc", "curl localhost:8001", cpus=1, memory_mb=32768, same_node_as_task_id=service_id)
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        client = self.db.get_task(client_id)
        self.assertEqual(client["status"], TaskStatus.RUNNING.value)
        self.assertEqual(client["allocation_id"], reference_allocation_id)

    def test_same_node_as_waits_until_reference_task_has_node(self) -> None:
        other_allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n115",
            total_cpus=128,
            total_memory_mb=524288,
        )
        self.db.update_allocation(other_allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="other-alloc")
        service_id = self.db.create_task(TaskCreate("vllm", "~/svc", "serve", cpus=4, memory_mb=8192))
        self.db.update_task(service_id, status=TaskStatus.COMPLETED.value, finished_at="CURRENT_TIMESTAMP")
        client_id = self.db.create_task(
            TaskCreate("client", "~/svc", "curl localhost:8001", cpus=1, memory_mb=1024, same_node_as_task_id=service_id)
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        client = self.db.get_task(client_id)
        self.assertEqual(client["status"], TaskStatus.QUEUED.value)
        diagnostics = scheduler.task_queue_diagnostics(client)
        self.assertEqual(diagnostics["queue_state"], "pending")
        self.assertEqual(diagnostics["queue_reason"], f"same_node_as task {service_id} is not running")

    def test_stale_same_node_task_fails_when_reference_is_terminal(self) -> None:
        service_id = self.db.create_task(TaskCreate("vllm", "~/svc", "serve", cpus=4, memory_mb=8192))
        self.db.update_task(service_id, status=TaskStatus.FAILED.value, finished_at="CURRENT_TIMESTAMP")
        client_id = self.db.create_task(
            TaskCreate("client", "~/svc", "curl localhost:8001", cpus=1, memory_mb=1024, same_node_as_task_id=service_id)
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.fail_stale_same_node_tasks()
        client = self.db.get_task(client_id)
        self.assertEqual(client["status"], TaskStatus.FAILED.value)
        self.assertEqual(client["failure_message"], f"same_node_as task {service_id} is failed")

    def test_running_same_node_task_fails_when_reference_is_terminal(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="n104",
            total_cpus=3,
            total_memory_mb=32768,
            total_gpus=1,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="service-alloc")
        service_id = self.db.create_task(TaskCreate("vllm", "~/svc", "serve", cpus=3, memory_mb=32768, gpus=1, gpu_model="a6000"))
        self.db.update_task(service_id, status=TaskStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")
        client_id = self.db.create_task(
            TaskCreate("client", "~/svc", "curl localhost:8001", cpus=1, memory_mb=1024, same_node_as_task_id=service_id)
        )
        self.db.update_task(
            client_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=allocation_id,
            account_name="a",
            started_at="CURRENT_TIMESTAMP",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.fail_stale_same_node_tasks()
        client = self.db.get_task(client_id)
        self.assertEqual(client["status"], TaskStatus.FAILED.value)
        self.assertEqual(client["failure_message"], f"same_node_as task {service_id} is cancelled")

    def test_half_used_cpu_pool_prewarms_spare_allocation(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=8,
            allocation_scale_out_usage_threshold=0.50,
        )
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        self.db.create_task(TaskCreate("heavy", "~/case", "run", cpus=4, memory_mb=2048))
        scheduler.assign_queued_tasks()
        scheduler.maintain_allocation_pool()
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 2)

    def test_pending_allocation_counts_as_spare_capacity(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=8,
            allocation_scale_out_usage_threshold=0.50,
        )
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        self.db.create_task(TaskCreate("heavy", "~/case", "run", cpus=6, memory_mb=2048))
        scheduler.assign_queued_tasks()
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 2)
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 2)

    def test_queued_tasks_do_not_block_high_utilization_cpu_prewarm(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=262144,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.ACTIVE.value,
            slurm_job_id="busy-cpu",
            free_cpus=16,
            free_memory_mb=262144,
        )
        self.db.create_task(TaskCreate("fits-but-backlogged", "~/case", "run", cpus=4, memory_mb=2048))
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_scale_out_usage_threshold=0.70,
        )
        scheduler.maintain_allocation_pool()
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 2)
        self.assertTrue(any(item["drain_reason"] == "high CPU utilization" for item in live))

    def test_queued_demand_does_not_duplicate_fitting_pending_allocation(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        self.db.create_task(TaskCreate("queued", "~/case", "run", cpus=4, memory_mb=2048))
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 1)
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 1)

    def test_exclusive_task_opens_exclusive_allocation(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=4, memory_mb=2048, exclusive_node=True))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["exclusive_node"], 1)

    def test_multiple_exclusive_tasks_wait_when_one_allocation_is_pending(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=8,
        )
        self.db.create_task(TaskCreate("exclusive-1", "~/case", "run", cpus=4, memory_mb=2048, exclusive_node=True))
        self.db.create_task(TaskCreate("exclusive-2", "~/case", "run", cpus=4, memory_mb=2048, exclusive_node=True))
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        self.assertEqual(len(allocations), 1)
        self.assertTrue(all(allocation["exclusive_node"] == 1 for allocation in allocations))
        scheduler.refresh_allocations()
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 2)

    def test_exclusive_demand_allocation_uses_task_size(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
        )
        self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=12, memory_mb=98304, exclusive_node=True))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["exclusive_node"], 1)
        self.assertEqual(allocation["total_cpus"], 12)
        self.assertEqual(allocation["total_memory_mb"], 98304)

    def test_non_exclusive_cpu_demand_uses_largest_available_cpu_pool(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-big cpu2 idle 0 256 0.0 1031519 1000000\n"
                "cpu-small cpu1 idle 0 48 0.0 768000 700000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            allocation_memory="0",
        )
        self.db.create_task(TaskCreate("cpu-backlog", "~/case", "run", cpus=16, memory_mb=32768))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["exclusive_node"], 0)
        self.assertEqual(allocation["total_cpus"], 64)
        self.assertGreater(allocation["total_memory_mb"], 32768)

    def test_non_exclusive_cpu_demand_queues_full_cpu_node_instead_of_tiny_fragment(self) -> None:
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=cpu-fragment CPUTot=48 RealMemory=768000 Gres=(null) State=MIXED Partitions=cpu1\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-fragment cpu1 mix 40 48 40.0 768000 724494 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            allocation_memory="0",
        )
        self.db.create_task(TaskCreate("small-cpu", "~/case", "run", cpus=4, memory_mb=32768))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["partition"], "cpu1")
        self.assertEqual(allocation["node_name"], "")
        self.assertEqual(allocation["total_cpus"], 48)
        self.assertEqual(allocation["total_memory_mb"], 768000)

    def test_cpu_demand_allocation_preserves_selected_shape_node(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu2-capped cpu2 mix 128 256 8.0 1031519 550000 busy\n"
                "cpu2-good cpu2 mix 64 256 4.0 1031519 950000 available\n"
            )
        )
        for index in range(2):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu2",
                node_name="cpu2-capped",
                total_cpus=64,
                total_memory_mb=262144,
                resource_pool="cpu",
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=f"capped-{index}",
            )
        self.db.create_task(
            TaskCreate(
                "fea-demand",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_partition="cpu2",
            allocation_cpus=64,
            cpu_partition_allocation_limits={"cpu2": 2},
        )
        scheduler.maintain_allocation_pool()
        demand = [
            allocation
            for allocation in self.db.list_allocations()
            if allocation.get("drain_reason") == "queued CPU demand"
        ]
        self.assertEqual(len(demand), 1)
        self.assertEqual(demand[0]["node_name"], "cpu2-good")
        script = build_allocation_script(demand[0], "48:00:00")
        self.assertIn("#SBATCH --nodelist=cpu2-good", script)

    def test_non_exclusive_cpu_demand_uses_gpu_reserve_on_gpu_nodes(self) -> None:
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=gpu-free CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu4\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-free gpu4 idle 0 56 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            cpu_pool_allow_gpu_partitions=True,
            gpu_cpu_reserve=4,
        )
        self.db.create_task(TaskCreate("small-cpu", "~/case", "run", cpus=4, memory_mb=32768))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["partition"], "gpu4")
        self.assertEqual(allocation["total_cpus"], 52)

    def test_non_exclusive_cpu_demand_uses_gpu_fragment_when_cpu_nodes_lack_live_capacity(self) -> None:
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=cpu-fragment CPUTot=48 RealMemory=768000 Gres=(null) State=MIXED Partitions=cpu1\n"
                "NodeName=gpu-fragment CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu4\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-fragment cpu1 mix 40 48 40.0 768000 724494 busy_job\n"
                "gpu-fragment gpu4 mix 28 56 28.0 1024000 900000 gpu_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            cpu_pool_allow_gpu_partitions=True,
            gpu_cpu_reserve=4,
            allocation_memory="0",
        )
        self.db.create_task(TaskCreate("small-cpu", "~/case", "run", cpus=4, memory_mb=32768))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["partition"], "gpu4")
        self.assertEqual(allocation["total_cpus"], 24)

    def test_pending_cpu_demand_replaced_by_gpu_fragment_when_cpu_nodes_lack_live_capacity(self) -> None:
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=cpu-fragment CPUTot=48 RealMemory=768000 Gres=(null) State=MIXED Partitions=cpu1\n"
                "NodeName=gpu-fragment CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu4\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-fragment cpu1 mix 40 48 40.0 768000 724494 busy_job\n"
                "gpu-fragment gpu4 mix 28 56 28.0 1024000 900000 gpu_job\n"
            )
        )
        old_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=48,
            total_memory_mb=768000,
        )
        self.db.update_allocation(
            old_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="old-cpu1",
            drain_reason="queued CPU demand",
        )
        self.db.create_task(TaskCreate("small-cpu", "~/case", "run", cpus=4, memory_mb=32768))
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            cpu_pool_allow_gpu_partitions=True,
            gpu_cpu_reserve=4,
            allocation_memory="0",
        )
        scheduler.scale_in_idle_allocations()
        self.assertEqual(self.db.get_allocation(old_id)["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(FakeClient.cancelled, ["old-cpu1"])
        scheduler.maintain_allocation_pool()
        live_allocations = [
            allocation
            for allocation in self.db.list_allocations()
            if allocation["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live_allocations), 1)
        self.assertEqual(live_allocations[0]["partition"], "gpu4")
        self.assertEqual(live_allocations[0]["total_cpus"], 24)

    def test_cpu_partition_allocation_limit_closes_only_empty_excess_pools_on_same_node(self) -> None:
        allocation_ids = []
        for index in range(3):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu2",
                node_name="cpu2-a",
                total_cpus=64,
                total_memory_mb=512000,
                resource_pool="cpu",
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=f"cpu2-job-{index}",
                drain_reason="queued CPU demand",
            )
            allocation_ids.append(allocation_id)
        active_task = self.db.create_task(TaskCreate("running", "~/case", "run", cpus=64, memory_mb=65536))
        self.db.update_task(
            active_task,
            status=TaskStatus.RUNNING.value,
            allocation_id=allocation_ids[1],
            account_name="a",
            started_at="CURRENT_TIMESTAMP",
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cpu_partition_allocation_limits={"cpu2": 2},
        )
        scheduler.enforce_cpu_partition_allocation_limits()
        self.assertEqual(self.db.get_allocation(allocation_ids[1])["state"], AllocationStatus.ACTIVE.value)
        states = [self.db.get_allocation(allocation_id)["state"] for allocation_id in allocation_ids]
        self.assertEqual(states.count(AllocationStatus.CLOSED.value), 1)
        self.assertEqual(states.count(AllocationStatus.ACTIVE.value), 2)
        self.assertEqual(len(FakeClient.cancelled), 1)
        self.assertIn(FakeClient.cancelled[0], {"cpu2-job-0", "cpu2-job-2"})

    def test_cpu_partition_allocation_limit_allows_multiple_cpu2_nodes(self) -> None:
        allocation_ids = []
        for index in range(3):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu2",
                node_name=f"cpu2-{index}",
                total_cpus=64,
                total_memory_mb=512000,
                resource_pool="cpu",
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=f"cpu2-job-{index}",
                drain_reason="queued CPU demand",
            )
            allocation_ids.append(allocation_id)

        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cpu_partition_allocation_limits={"cpu2": 2},
        )
        scheduler.enforce_cpu_partition_allocation_limits()
        states = [self.db.get_allocation(allocation_id)["state"] for allocation_id in allocation_ids]
        self.assertEqual(states.count(AllocationStatus.ACTIVE.value), 3)
        self.assertEqual(FakeClient.cancelled, [])

    def test_cpu_partition_allocation_limit_includes_old_live_pools_on_same_node(self) -> None:
        allocation_ids = []
        for index in range(3):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu2",
                node_name="cpu2-old",
                total_cpus=64,
                total_memory_mb=512000,
                resource_pool="cpu",
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=f"old-cpu2-job-{index}",
                drain_reason="queued CPU demand",
            )
            allocation_ids.append(allocation_id)
        for index in range(520):
            closed_id = self.db.create_allocation(
                account_name="a",
                partition="cpu1",
                node_name=f"closed-{index}",
                total_cpus=64,
                total_memory_mb=512000,
                resource_pool="cpu",
            )
            self.db.update_allocation(closed_id, state=AllocationStatus.CLOSED.value, closed_at="CURRENT_TIMESTAMP")

        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cpu_partition_allocation_limits={"cpu2": 2},
        )

        self.assertEqual(scheduler.live_allocation_count_for_partition("cpu2", resource_pool="cpu"), 3)
        self.assertEqual(scheduler.live_allocation_count_for_partition_node("cpu2", "cpu2-old", resource_pool="cpu"), 3)
        scheduler.enforce_cpu_partition_allocation_limits()
        states = [self.db.get_allocation(allocation_id)["state"] for allocation_id in allocation_ids]
        self.assertEqual(states.count(AllocationStatus.CLOSED.value), 1)
        self.assertEqual(states.count(AllocationStatus.ACTIVE.value), 2)

    def test_queue_reason_reports_cpu_partition_limit_when_only_fitting_partition_is_capped(self) -> None:
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=cpu-small CPUTot=48 RealMemory=768000 Gres=(null) State=IDLE Partitions=cpu1\n"
                "NodeName=cpu-big CPUTot=256 RealMemory=1031519 Gres=(null) State=IDLE Partitions=cpu2\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-small cpu1 idle 0 48 0.0 768000 700000\n"
                "cpu-big cpu2 idle 0 256 0.0 1031519 1000000\n"
            )
        )
        for index in range(2):
            allocation_id = self.db.create_allocation(
                account_name="b",
                partition="cpu2",
                node_name="cpu-big",
                total_cpus=64,
                total_memory_mb=512000,
                resource_pool="cpu",
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=f"cpu2-job-{index}",
            )
        task_id = self.db.create_task(
            TaskCreate("wide-cpu", "~/case", "run", account_name="a", cpus=64, memory_mb=65536)
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            allocation_memory="0",
            cpu_partition_allocation_limits={"cpu2": 2},
        )
        diagnostics = scheduler.task_queue_diagnostics(self.db.get_task(task_id))
        self.assertEqual(diagnostics["queue_state"], "blocked")
        self.assertEqual(
            diagnostics["queue_reason"],
            "cannot open 64 CPU pool: CPU allocation limit reached for cpu2/cpu-big 2/2",
        )

    def test_non_exclusive_cpu_demand_does_not_open_pool_smaller_than_task(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "fragmented cpu2 mix 251 256 251.0 1031519 1000000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            allocation_memory="0",
        )
        self.db.create_task(TaskCreate("wide-cpu", "~/case", "run", account_name="a", cpus=48, memory_mb=49152))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertGreaterEqual(allocation["total_cpus"], 48)

    def test_fragmented_cpu_capacity_opens_multiple_fit_aware_demand_pools(self) -> None:
        for index in range(6):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu1",
                node_name=f"n{index:03d}",
                total_cpus=64,
                total_memory_mb=262144,
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=f"active-{index}",
                free_cpus=16,
                free_memory_mb=262144,
            )
        for index in range(12):
            self.db.create_task(TaskCreate(f"wide-{index}", "~/case", "run", cpus=48, memory_mb=2048))
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            allocation_max_new_per_loop=8,
        )
        scheduler.maintain_allocation_pool()
        demand_allocations = [
            allocation
            for allocation in self.db.list_allocations(limit=100)
            if allocation["state"] == AllocationStatus.PENDING.value
            and allocation["drain_reason"] == "queued CPU demand"
        ]
        self.assertEqual(len(demand_allocations), 8)
        self.assertTrue(all(int(allocation["total_cpus"]) >= 48 for allocation in demand_allocations))

    def test_pending_wide_cpu_pool_reserves_slot_without_duplicate_pool(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=48,
            total_memory_mb=98304,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-wide",
            drain_reason="queued CPU demand",
        )
        self.db.create_task(TaskCreate("wide", "~/case", "run", cpus=48, memory_mb=49152))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.maintain_allocation_pool()
        demand_allocations = [
            allocation
            for allocation in self.db.list_allocations(limit=100)
            if str(allocation.get("drain_reason") or "").startswith("queued ")
        ]
        self.assertEqual(len(demand_allocations), 1)
        self.assertEqual(demand_allocations[0]["id"], allocation_id)

    def test_gpu_task_waiting_for_pending_gpu_pool_reports_gpu_reason(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu4,gpu5",
            node_name="",
            total_cpus=16,
            total_memory_mb=131072,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            drain_reason="minimum GPU warm pool a6000",
        )
        task_id = self.db.create_task(
            TaskCreate("gpu-task", "~/case", "run", cpus=1, memory_mb=32768, gpus=1, gpu_model="a6000")
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        diagnostics = scheduler.task_queue_diagnostics(self.db.get_task(task_id))
        self.assertEqual(diagnostics["queue_state"], "pending")
        self.assertEqual(
            diagnostics["queue_reason"],
            f"waiting for pending 1 a6000 GPU pool: allocations {allocation_id}",
        )

    def test_account_pending_limit_blocks_demand_pool_and_reports_reason(self) -> None:
        self.accounts = [AccountConfig("a", "host", 22, "a", "key", "/work", 4, 1, 10)]
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=0, pending=1, max_running=4, max_pending=1, max_total=10),
        }
        task_id = self.db.create_task(TaskCreate("wide", "~/case", "run", account_name="a", cpus=48, memory_mb=49152))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.maintain_allocation_pool()
        self.assertEqual(self.db.list_allocations(), [])
        diagnostics = scheduler.task_queue_diagnostics(self.db.get_task(task_id))
        self.assertEqual(diagnostics["queue_state"], "blocked")
        self.assertEqual(diagnostics["queue_reason"], "account a job limit reached")

    def test_exclusive_cpu_demand_avoids_busy_single_job_partition(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-free cpu1 idle 0 48 0.0 768000 700000\n"
                "cpu2-free cpu2 idle 0 256 0.0 1031519 1000000\n"
                "gpu-free gpu3 mix 4 56 0.0 1024000 900000\n"
            )
        )
        existing_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="cpu2-used",
            total_cpus=64,
            total_memory_mb=65536,
            resource_pool="cpu",
        )
        self.db.update_allocation(existing_id, state=AllocationStatus.WARM.value, slurm_job_id="cpu2-pool")
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
        )
        self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=12, memory_mb=98304, exclusive_node=True))
        scheduler.maintain_allocation_pool()
        allocations = [allocation for allocation in self.db.list_allocations() if allocation["id"] != existing_id]
        self.assertEqual(len(allocations), 1)
        self.assertEqual(allocations[0]["partition"], "cpu1")
        self.assertEqual(allocations[0]["total_cpus"], 12)

    def test_pending_demand_allocation_closes_when_no_queued_task_needs_it(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=12,
            total_memory_mb=98304,
            resource_pool="cpu",
            exclusive_node=True,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-demand",
            drain_reason="queued CPU demand",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("pending-demand", FakeClient.cancelled)

    def test_undersized_shared_cpu_demand_allocation_closes_even_when_queued_task_needs_it(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-big cpu2 idle 0 256 0.0 1031519 1000000\n"
                "cpu-small cpu1 idle 0 48 0.0 768000 700000\n"
            )
        )
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="",
            total_cpus=16,
            total_memory_mb=32768,
            resource_pool="cpu",
            exclusive_node=False,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="small-pending-demand",
            drain_reason="queued CPU demand",
        )
        self.db.create_task(TaskCreate("cpu-backlog", "~/case", "run", cpus=16, memory_mb=32768))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("small-pending-demand", FakeClient.cancelled)

    def test_unsubmitted_pending_cpu_demand_allocation_closes_after_restart(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-fragment cpu1 mix 40 48 40.0 768000 724494 busy_job\n"
            )
        )
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=8,
            total_memory_mb=724494,
            resource_pool="cpu",
            exclusive_node=False,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.PENDING.value)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(FakeClient.cancelled, [])

    def test_qos_blocked_cpu_demand_allocation_closes(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="n113",
            total_cpus=96,
            total_memory_mb=505118,
            resource_pool="cpu",
            exclusive_node=False,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="qos-blocked",
            pending_reason="(QOSMaxCpuPerNode)",
            drain_reason="queued CPU demand",
        )
        self.db.create_task(TaskCreate("cpu-backlog", "~/case", "run", cpus=4, memory_mb=32768))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("qos-blocked", FakeClient.cancelled)

    def test_warm_demand_allocation_closes_when_no_queued_task_needs_it(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=12,
            total_memory_mb=98304,
            resource_pool="cpu",
            exclusive_node=True,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="warm-demand",
            drain_reason="queued CPU demand",
            started_at="CURRENT_TIMESTAMP",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("warm-demand", FakeClient.cancelled)

    def test_just_warm_demand_allocation_gets_one_attach_poll_before_scale_in(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="cpu2-soft-blocked",
            total_cpus=64,
            total_memory_mb=262144,
            resource_pool="cpu",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="warm-demand",
            drain_reason="queued CPU demand",
            started_at="CURRENT_TIMESTAMP",
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu2-soft-blocked cpu2 mix 64 256 4.0 100000 55000 busy\n"
            )
        )
        self.db.create_task(
            TaskCreate(
                "waiting-fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
        )
        self.assertEqual(scheduler.queued_task_allocation_reservations(), {})
        scheduler.scale_in_idle_allocations()
        self.assertEqual(self.db.get_allocation(allocation_id)["state"], AllocationStatus.WARM.value)
        self.assertEqual(FakeClient.cancelled, [])

        with self.db.connect() as conn:
            conn.execute(
                "UPDATE allocations SET started_at = datetime('now', '-61 seconds') WHERE id = ?",
                (allocation_id,),
            )
        scheduler.scale_in_idle_allocations()
        self.assertEqual(self.db.get_allocation(allocation_id)["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(FakeClient.cancelled, ["warm-demand"])

    def test_warm_demand_allocation_stays_when_queued_task_needs_it(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=12,
            total_memory_mb=98304,
            resource_pool="cpu",
            exclusive_node=True,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="warm-demand",
            drain_reason="queued CPU demand",
        )
        self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=12, memory_mb=98304, exclusive_node=True))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.WARM.value)
        self.assertEqual(FakeClient.cancelled, [])

    def test_exclusive_task_closes_allocation_after_finish(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=12,
            total_memory_mb=98304,
            resource_pool="cpu",
            exclusive_node=True,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="exclusive-alloc")
        task_id = self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=12, memory_mb=2048, exclusive_node=True))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=allocation_id,
            account_name="a",
        )
        FakeClient.task_states[task_id] = JobStatus.COMPLETED
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.refresh_tasks()
        task = self.db.get_task(task_id)
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(task["status"], TaskStatus.COMPLETED.value)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("exclusive-alloc", FakeClient.cancelled)

    def test_running_task_timeout_marks_failed_with_exit_code(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=4,
            total_memory_mb=8192,
            resource_pool="cpu",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-timeout")
        task_id = self.db.create_task(TaskCreate("timeout", "~/case", "run", timeout_seconds=1))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=allocation_id,
            account_name="a",
            wrapper_pid="1234",
            started_at="2000-01-01 00:00:00",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.refresh_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.FAILED.value)
        self.assertEqual(task["exit_code"], 124)
        self.assertIn("timed out", task["failure_message"])
        self.assertEqual(FakeClient.cancelled_tasks, [task_id])

    def test_refresh_tasks_caps_large_fea_set_and_rotates(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=262144,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        standard_id = self.db.create_task(TaskCreate("standard", "~/case", "run", cpus=1, memory_mb=512))
        self.db.update_task(
            standard_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=allocation_id,
            account_name="a",
            exit_code_path="/remote/std.exit",
        )
        fea_ids = []
        for index in range(10):
            task_id = self.db.create_task(
                TaskCreate(
                    f"fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=32768,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                allocation_id=allocation_id,
                account_name="a",
                exit_code_path=f"/remote/fea-{index}.exit",
            )
            FakeClient.task_states[task_id] = JobStatus.COMPLETED
            fea_ids.append(task_id)
        FakeClient.task_states[standard_id] = JobStatus.COMPLETED
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, task_refresh_max_per_tick=5)
        scheduler.refresh_tasks()
        self.assertEqual(self.db.get_task(standard_id)["status"], TaskStatus.COMPLETED.value)
        self.assertEqual(
            sum(1 for task_id in fea_ids if self.db.get_task(task_id)["status"] == TaskStatus.COMPLETED.value),
            4,
        )
        scheduler.refresh_tasks()
        self.assertEqual(
            sum(1 for task_id in fea_ids if self.db.get_task(task_id)["status"] == TaskStatus.COMPLETED.value),
            9,
        )

    def test_task_required_capability_uses_matching_allocation_account(self) -> None:
        accounts = [
            AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10, capabilities=["conda:pyaedt"]),
            AccountConfig("b", "host", 22, "b", "key", "/work", 4, 10, 10),
        ]
        allocation_id = self.db.create_allocation(
            account_name="b",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(
            TaskCreate(
                "ansys",
                "~/case",
                "run",
                required_capability="conda:pyaedt",
                cpus=4,
                memory_mb=2048,
            )
        )
        scheduler = Scheduler(self.db, accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        self.assertIsNone(scheduler.best_allocation_for_task(self.db.get_task(task_id)))
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(FakeClient.allocation_submits), 1)

    def test_task_requested_account_uses_matching_allocation_account(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="b",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(TaskCreate("ansys", "~/case", "run", account_name="a", cpus=4, memory_mb=2048))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        self.assertIsNone(scheduler.best_allocation_for_task(self.db.get_task(task_id)))
        scheduler.maintain_allocation_pool()
        self.assertEqual(FakeClient.allocation_submits, ["a"])

    def test_drained_allocation_is_cancelled_when_empty(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=8,
            allocation_drain_after_seconds=1,
        )
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        allocation = self.db.list_allocations()[0]
        self.db.update_allocation(allocation["id"], started_at="2000-01-01 00:00:00")
        scheduler.apply_allocation_lifecycle()
        allocation = self.db.get_allocation(allocation["id"])
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(FakeClient.cancelled, [allocation["slurm_job_id"]])

    def test_allocation_prewarm_respects_account_job_limit(self) -> None:
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=10, pending=0, max_running=10, max_pending=10, max_total=10),
            "b": AccountSnapshot("b", running=10, pending=0, max_running=10, max_pending=10, max_total=10),
        }
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        self.assertEqual(self.db.list_allocations(), [])

    def test_allocation_shape_uses_pestat_and_gpu_reserve(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "gpu-node gpu3 idle 0 56 0.0 1024000 900000\n"
            "cpu-node cpu2 idle 0 128 0.0 1031519 800000\n"
        )
        self.db.replace_pestat_nodes(nodes)
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_partition="gpu3",
            gpu_cpu_reserve=4,
        )
        shape = scheduler.choose_allocation_shape()
        self.assertEqual(shape["partition"], "gpu3")
        self.assertEqual(shape["node_name"], "gpu-node")
        self.assertEqual(shape["cpus"], 52)

    def test_allocation_shape_prefers_larger_pestat_capacity(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "small cpu2 idle 0 32 0.0 128000 120000\n"
            "large cpu2 idle 0 128 0.0 512000 500000\n"
        )
        self.db.replace_pestat_nodes(nodes)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_partition="cpu2")
        shape = scheduler.choose_allocation_shape()
        self.assertEqual(shape["node_name"], "large")
        self.assertEqual(shape["cpus"], 64)

    def test_shared_cpu_pool_can_use_mixed_single_job_partition_capacity(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "mixed-cpu cpu2 mix 4 256 0.0 1031519 1000000\n"
            "idle-gpu gpu3 mix 4 56 0.0 876000 800000\n"
        )
        self.db.replace_pestat_nodes(nodes)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_partition="auto")
        shape = scheduler.choose_allocation_shape(resource_pool="cpu")
        self.assertEqual(shape["partition"], "cpu2")
        self.assertEqual(shape["node_name"], "mixed-cpu")
        self.assertEqual(shape["cpus"], 64)

    def test_cpu_partition_allocation_limit_is_per_node_for_shape_selection(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu2-a cpu2 idle 0 256 0.0 1031519 1000000\n"
                "cpu2-b cpu2 idle 0 256 0.0 1031519 1000000\n"
                "cpu2-c cpu2 idle 0 256 0.0 1031519 1000000\n"
                "cpu1-a cpu1 idle 0 64 0.0 768000 700000\n"
            )
        )
        for index, node_name in enumerate(["cpu2-a", "cpu2-b"]):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu2",
                node_name=node_name,
                total_cpus=64,
                total_memory_mb=512000,
                resource_pool="cpu",
            )
            self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id=f"cpu2-{index}")
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_partition="auto",
            cpu_partition_allocation_limits={"cpu2": 2},
        )
        shape = scheduler.choose_allocation_shape(resource_pool="cpu")
        self.assertEqual(shape["partition"], "cpu2")
        self.assertIn(shape["node_name"], {"cpu2-a", "cpu2-b", "cpu2-c"})

    def test_cpu_pool_can_use_gpu_partition_when_no_cpu_candidate_exists(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=cpu-old CPUTot=48 RealMemory=768000 Gres=(null) State=IDLE Partitions=cpu1\n"
            "NodeName=gpu-fast CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-fast gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        shape = scheduler.choose_allocation_shape(resource_pool="cpu")
        self.assertEqual(shape["partition"], "gpu5")
        self.assertEqual(shape["node_name"], "gpu-fast")
        self.assertEqual(shape["gpus"], 0)

    def test_cpu_pool_can_disable_gpu_partition_candidates(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=cpu-old CPUTot=48 RealMemory=768000 Gres=(null) State=IDLE Partitions=cpu1\n"
            "NodeName=gpu-fast CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-old cpu1 idle 0 48 0.0 768000 700000\n"
                "gpu-fast gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cpu_pool_allow_gpu_partitions=False,
        )
        shape = scheduler.choose_allocation_shape(resource_pool="cpu")
        self.assertEqual(shape["partition"], "cpu1")

    def test_cpu2_single_job_partition_skips_occupied_node(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "busy cpu2 mix 64 128 64.0 512000 100000 busy_job\n"
            "free cpu2 idle 0 128 0.0 512000 500000\n"
        )
        self.db.replace_pestat_nodes(nodes)
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="busy",
            total_cpus=64,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_partition="cpu2",
            single_job_per_node_partitions=["cpu2"],
        )
        shape = scheduler.choose_allocation_shape()
        self.assertEqual(shape["node_name"], "free")

    def test_cpu2_direct_job_gets_free_node(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "busy cpu2 mix 64 128 64.0 512000 100000 busy_job\n"
                "free cpu2 idle 0 128 0.0 512000 500000\n"
            )
        )
        job_id = self.db.create_job(JobCreate("", "", "run.py", job_mode="packed_srun", partition="cpu2"))
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            single_job_per_node_partitions=["cpu2"],
        )
        job = self.db.get_job(job_id)
        self.assertTrue(scheduler.prepare_single_job_node(job))
        self.assertEqual(self.db.get_job(job_id)["node_name"], "free")

    def test_cpu2_duplicate_queued_jobs_do_not_deadlock_before_first_submit(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "free cpu2 idle 0 128 0.0 512000 500000\n"
            )
        )
        first = self.db.create_job(JobCreate("", "", "run.py", job_mode="packed_srun", partition="cpu2", node_name="free"))
        self.db.create_job(JobCreate("", "", "run.py", job_mode="packed_srun", partition="cpu2", node_name="free"))
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            single_job_per_node_partitions=["cpu2"],
        )
        self.assertTrue(scheduler.prepare_single_job_node(self.db.get_job(first)))

    def test_gpu_prewarm_prefers_a6000ada(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-ada CPUTot=64 RealMemory=1024000 Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:0 State=IDLE Partitions=gpu3\n"
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-ada gpu3 idle 0 64 0.0 1024000 900000\n"
                "gpu-a6000 gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000ada", "a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        gpu_allocations = [item for item in allocations if item["resource_pool"].startswith("gpu:")]
        self.assertEqual(len(gpu_allocations), 1)
        self.assertEqual(gpu_allocations[0]["gpu_model"], "a6000ada")
        self.assertEqual(gpu_allocations[0]["total_gpus"], 2)
        self.assertEqual(gpu_allocations[0]["total_cpus"], 60)

    def test_gpu_prewarm_pins_two_when_three_are_free(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:1(IDX:0) State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 mix 8 64 1.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["resource_pool"], "gpu:a6000")
        self.assertEqual(allocation["node_name"], "")
        self.assertEqual(allocation["total_cpus"], 8)
        self.assertEqual(allocation["total_gpus"], 2)

    def test_gpu_prewarm_a6000_queues_pool_when_ready_node_cpu_is_too_small(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2(IDX:0-1) State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 mix 60 64 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["resource_pool"], "gpu:a6000")
        self.assertEqual(allocation["node_name"], "")
        self.assertEqual(allocation["total_cpus"], 8)
        self.assertEqual(allocation["total_gpus"], 2)
        self.assertEqual(allocation["total_memory_mb"], 131072)

    def test_gpu_prewarm_a6000_pins_pool_when_partial_node_fits(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2(IDX:0-1) State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 mix 56 64 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["resource_pool"], "gpu:a6000")
        self.assertEqual(allocation["node_name"], "")
        self.assertEqual(allocation["total_cpus"], 8)
        self.assertEqual(allocation["total_gpus"], 2)
        self.assertEqual(allocation["total_memory_mb"], 131072)

    def test_gpu_fallback_caps_cpu_request_to_partition_node_capacity(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-ada gpu3 mix 4 56 0.0 876000 800000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
            gpu_prewarm_gpus_per_allocation=2,
        )
        shape = scheduler.choose_allocation_shape(resource_pool="gpu:a6000ada", gpu_model="a6000ada", gpus=2)
        self.assertEqual(shape["partition"], "gpu3")
        self.assertLessEqual(shape["cpus"], 56)

    def test_gpu_fallback_partition_respects_requested_gpu_model(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-ada CPUTot=56 RealMemory=876000 Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:0 State=IDLE Partitions=gpu3\n"
            "NodeName=gpu-a6000-gpu4 CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu4\n"
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-ada gpu3 idle 0 56 0.0 876000 800000\n"
                "gpu-a6000-gpu4 gpu4 idle 0 56 0.0 1024000 900000\n"
                "gpu-a6000 gpu5 mix 28 64 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
            gpu_prewarm_gpus_per_allocation=4,
            gpu_prewarm_min_gpus_per_allocation=4,
        )
        shape = scheduler.choose_allocation_shape(
            resource_pool="gpu:a6000",
            gpu_model="a6000",
            gpus=4,
            requested_cpus=16,
        )
        self.assertEqual(shape["partition"], "gpu5")
        self.assertEqual(shape["node_name"], "gpu-a6000")
        self.assertEqual(shape["gpu_model"], "a6000")
        self.assertEqual(shape["cpus"], 16)

    def test_gpu_full_node_warm_shape_uses_fixed_a6000_cpu_floor(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=48 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 mix 12 48 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
        )
        shape = scheduler.choose_allocation_shape(resource_pool="gpu:a6000", gpu_model="a6000", gpus=4)
        self.assertEqual(shape["partition"], "gpu5")
        self.assertEqual(shape["node_name"], "gpu-a6000")
        self.assertEqual(shape["cpus"], 16)

    def test_gpu5_priority_requires_enough_free_cpu_for_a6000_full_gpu(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu4-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu4\n"
            "NodeName=gpu5-a CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu4-a gpu4 idle 0 56 0.0 1024000 900000\n"
                "gpu5-a gpu5 mix 60 64 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
        )
        shape = scheduler.choose_allocation_shape(resource_pool="gpu:a6000", gpu_model="a6000", gpus=4)
        self.assertEqual(shape["partition"], "gpu4")
        self.assertEqual(shape["cpus"], 16)

    def test_gpu_shape_uses_pestat_sched_free_cpu_not_load_adjusted_cpu(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 mix 56 64 56.14 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
        )
        shape = scheduler.choose_allocation_shape(
            resource_pool="gpu:a6000",
            gpu_model="a6000",
            gpus=2,
            requested_memory_mb=131072,
        )
        self.assertEqual(shape["partition"], "gpu5")
        self.assertEqual(shape["node_name"], "gpu-a6000")
        self.assertEqual(shape["cpus"], 8)

    def test_gpu_full_node_warm_shape_queues_when_only_cpu_is_temporarily_short(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu5-a CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu5-a gpu5 mix 54 64 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
        )
        shape = scheduler.choose_allocation_shape(
            resource_pool="gpu:a6000",
            gpu_model="a6000",
            gpus=4,
            requested_memory_mb=131072,
        )
        self.assertEqual(shape["partition"], "gpu5")
        self.assertEqual(shape["node_name"], "")
        self.assertEqual(shape["cpus"], 16)
        self.assertEqual(shape["gpus"], 4)

    def test_gpu_warm_shape_queues_across_gpu4_and_gpu5_when_no_node_currently_fits(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu4-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu4\n"
            "NodeName=gpu5-a CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu4-a gpu4 mix 50 56 10.0 1024000 900000 busy_job\n"
                "gpu5-a gpu5 mix 58 64 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
        )
        shape = scheduler.choose_allocation_shape(
            resource_pool="gpu:a6000",
            gpu_model="a6000",
            gpus=0,
            requested_memory_mb=131072,
        )
        self.assertEqual(shape["partition"], "gpu4,gpu5")
        self.assertEqual(shape["node_name"], "")
        self.assertEqual(shape["cpus"], 8)
        self.assertEqual(shape["gpus"], 2)

    def test_gpu_fallback_can_queue_against_reserved_full_a6000_node(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu4-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu4\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu4-a gpu4 resv 0 56 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
        )
        shape = scheduler.choose_allocation_shape(resource_pool="gpu:a6000", gpu_model="a6000", gpus=4)
        self.assertEqual(shape["partition"], "gpu4")
        self.assertEqual(shape["node_name"], "")
        self.assertEqual(shape["cpus"], 16)

    def test_partition_rank_prefers_gpu5_over_gpu4_for_a6000_full_gpu(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu4-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu4\n"
            "NodeName=gpu4-b CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu4\n"
            "NodeName=gpu5-a CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        ranked = partition_rank([item.__dict__ for item in inventory], needs_gpu=True)
        self.assertEqual(ranked[0]["partition"], "gpu5")

    def test_gpu_prewarm_takes_two_gpus_when_available(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=48 RealMemory=687626 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 idle 0 48 0.0 687626 680000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["resource_pool"], "gpu:a6000")
        self.assertEqual(allocation["node_name"], "")
        self.assertEqual(allocation["total_gpus"], 2)
        self.assertEqual(allocation["total_cpus"], 8)
        self.assertEqual(allocation["total_memory_mb"], 131072)

    def test_gpu_prewarm_can_request_four_gpus_with_four_cpus_when_configured(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 mix 60 64 60.14 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
            gpu_prewarm_gpus_per_allocation=4,
            gpu_prewarm_min_gpus_per_allocation=4,
            gpu_prewarm_cpus_per_allocation=4,
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["resource_pool"], "gpu:a6000")
        self.assertEqual(allocation["node_name"], "")
        self.assertEqual(allocation["total_gpus"], 4)
        self.assertEqual(allocation["total_cpus"], 4)
        self.assertEqual(allocation["total_memory_mb"], 131072)

    def test_gpu_prewarm_prefers_partition_with_more_current_fit_nodes(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu2-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2 State=MIXED Partitions=gpu2\n"
            "NodeName=gpu4-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2 State=MIXED Partitions=gpu4\n"
            "NodeName=gpu4-b CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2 State=MIXED Partitions=gpu4\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu2-a gpu2 mix 8 56 0.0 1024000 900000 job\n"
                "gpu4-a gpu4 mix 8 56 0.0 1024000 900000 job\n"
                "gpu4-b gpu4 mix 8 56 0.0 1024000 900000 job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["partition"], "gpu4")
        self.assertEqual(allocation["node_name"], "")
        self.assertEqual(allocation["total_gpus"], 2)
        self.assertEqual(allocation["total_cpus"], 8)

    def test_gpu_prewarm_retries_different_node_after_pinned_pending_timeout(self) -> None:
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu4",
            node_name="gpu4-a",
            total_cpus=8,
            total_memory_mb=131072,
            total_gpus=2,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            submitted_at="2000-01-01 00:00:00",
            pending_reason="(Priority)",
            drain_reason="minimum GPU warm pool a6000",
        )
        inventory = parse_scontrol_nodes(
            "NodeName=gpu4-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2 State=MIXED Partitions=gpu4\n"
            "NodeName=gpu4-b CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2 State=MIXED Partitions=gpu4\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu4-a gpu4 mix 8 56 0.0 1024000 900000 job\n"
                "gpu4-b gpu4 mix 8 56 0.0 1024000 900000 job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
            gpu_prewarm_pinned_pending_timeout_seconds=60,
        )
        scheduler.apply_allocation_lifecycle()
        old_pool = self.db.get_allocation(pending_id)
        self.assertEqual(old_pool["state"], AllocationStatus.CLOSED.value)
        self.assertIn("pinned warm pool retry", old_pool["drain_reason"])
        scheduler.maintain_allocation_pool()
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]["node_name"], "")
        self.assertEqual(FakeClient.cancelled, ["pending-a6000"])

    def test_gpu_prewarm_pinned_retry_runs_before_general_pending_timeout(self) -> None:
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu4",
            node_name="gpu4-a",
            total_cpus=4,
            total_memory_mb=131072,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            submitted_at="2000-01-01 00:00:00",
            pending_reason="(Priority)",
            drain_reason="minimum GPU warm pool a6000",
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_pending_timeout_seconds=9_999_999_999,
            gpu_prewarm_enabled=True,
            gpu_prewarm_pinned_pending_timeout_seconds=300,
        )
        scheduler.apply_allocation_lifecycle()
        old_pool = self.db.get_allocation(pending_id)
        self.assertEqual(old_pool["state"], AllocationStatus.CLOSED.value)
        self.assertIn("pinned warm pool retry", old_pool["drain_reason"])
        self.assertEqual(FakeClient.cancelled, ["pending-a6000"])

    def test_gpu_prewarm_opens_spare_when_existing_a6000_pool_is_partly_used(self) -> None:
        existing_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="gpu-busy",
            total_cpus=64,
            total_memory_mb=1024000,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            existing_id,
            state=AllocationStatus.ACTIVE.value,
            slurm_job_id="busy-a6000",
            free_cpus=16,
            free_gpus=2,
        )
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-free CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-free gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        self.assertEqual(len(allocations), 2)
        spare = max(allocations, key=lambda item: int(item["id"]))
        self.assertEqual(spare["state"], AllocationStatus.PENDING.value)
        self.assertEqual(spare["resource_pool"], "gpu:a6000")
        self.assertEqual(spare["total_gpus"], 2)
        self.assertEqual(spare["total_cpus"], 8)
        self.assertEqual(spare["total_memory_mb"], 131072)

    def test_gpu_prewarm_closes_undersized_pending_warm_pool(self) -> None:
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu4",
            node_name="",
            total_cpus=16,
            total_memory_mb=16384,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            drain_reason="minimum GPU warm pool a6000",
        )
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-free CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-free gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        old_pool = self.db.get_allocation(pending_id)
        self.assertEqual(old_pool["state"], AllocationStatus.CLOSED.value)
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]["total_memory_mb"], 131072)
        self.assertEqual(live[0]["total_gpus"], 2)
        self.assertEqual(live[0]["total_cpus"], 8)

    def test_gpu_prewarm_closes_pending_pool_when_cpu_policy_changes(self) -> None:
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="",
            total_cpus=16,
            total_memory_mb=131072,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            drain_reason="minimum GPU warm pool a6000",
        )
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-free CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-free gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000"],
            gpu_prewarm_gpus_per_allocation=4,
            gpu_prewarm_min_gpus_per_allocation=4,
            gpu_prewarm_cpus_per_allocation=4,
        )
        scheduler.maintain_allocation_pool()
        old_pool = self.db.get_allocation(pending_id)
        self.assertEqual(old_pool["state"], AllocationStatus.CLOSED.value)
        self.assertIn("CPU count policy change", old_pool["drain_reason"])
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]["total_gpus"], 4)
        self.assertEqual(live[0]["total_cpus"], 4)

    def test_gpu_prewarm_closes_partial_gpu_pending_warm_pool(self) -> None:
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu4",
            node_name="gpu-partial",
            total_cpus=12,
            total_memory_mb=131072,
            total_gpus=3,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            drain_reason="minimum GPU warm pool a6000",
        )
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-free CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-free gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        old_pool = self.db.get_allocation(pending_id)
        self.assertEqual(old_pool["state"], AllocationStatus.CLOSED.value)
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]["total_gpus"], 2)
        self.assertEqual(live[0]["total_cpus"], 8)

    def test_gpu_prewarm_closes_old_shape_and_queues_multi_partition_when_no_node_fits(self) -> None:
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="",
            total_cpus=16,
            total_memory_mb=131072,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            drain_reason="minimum GPU warm pool a6000",
        )
        inventory = parse_scontrol_nodes(
            "NodeName=gpu4-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu4\n"
            "NodeName=gpu5-a CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu4-a gpu4 mix 46 56 10.0 1024000 900000 busy_job\n"
                "gpu5-a gpu5 mix 54 64 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        old_pool = self.db.get_allocation(pending_id)
        self.assertEqual(old_pool["state"], AllocationStatus.CLOSED.value)
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]["partition"], "gpu4,gpu5")
        self.assertEqual(live[0]["total_gpus"], 2)
        self.assertEqual(live[0]["total_cpus"], 8)

    def test_gpu_prewarm_pending_full_spare_prevents_duplicate(self) -> None:
        busy_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="gpu-busy",
            total_cpus=64,
            total_memory_mb=1024000,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="",
            total_cpus=8,
            total_memory_mb=1024000,
            total_gpus=2,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(busy_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="busy-a6000", free_gpus=2)
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            drain_reason="minimum GPU warm pool a6000",
        )
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-free CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-free gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 2)

    def test_gpu_prewarm_staggers_second_a6000_spare(self) -> None:
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="",
            total_cpus=8,
            total_memory_mb=1024000,
            total_gpus=2,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(pending_id, state=AllocationStatus.PENDING.value, slurm_job_id="pending-a6000")
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-free CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-free gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=2,
            gpu_prewarm_max_warm_allocations=4,
            gpu_prewarm_preferred_models=["a6000"],
            gpu_prewarm_stagger_seconds=86400,
        )
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 1)

        self.db.update_allocation(pending_id, submitted_at="2000-01-01 00:00:00")
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        self.assertEqual(len(allocations), 2)
        newest = max(allocations, key=lambda item: int(item["id"]))
        self.assertEqual(newest["resource_pool"], "gpu:a6000")
        self.assertEqual(newest["total_cpus"], 8)
        self.assertEqual(newest["total_gpus"], 2)

    def test_gpu_prewarm_min_two_ignores_partly_used_a6000_pool(self) -> None:
        busy_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="gpu-busy",
            total_cpus=16,
            total_memory_mb=1024000,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="",
            total_cpus=16,
            total_memory_mb=1024000,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(busy_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="busy-a6000", free_gpus=2)
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-a6000",
            submitted_at="2000-01-01 00:00:00",
        )
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-free CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-free gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=2,
            gpu_prewarm_max_warm_allocations=4,
            gpu_prewarm_preferred_models=["a6000"],
            gpu_prewarm_stagger_seconds=86400,
        )
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        self.assertEqual(len(allocations), 3)
        spares = [item for item in allocations if item["state"] == AllocationStatus.PENDING.value]
        self.assertEqual(len(spares), 2)

    def test_gpu_prewarm_does_not_open_lower_fallback_when_preferred_is_only_pending(self) -> None:
        for model in ["a6000ada", "a6000"]:
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="gpu3",
                node_name=f"{model}-pending",
                total_cpus=32,
                total_memory_mb=65536,
                total_gpus=2,
                gpu_model=model,
                resource_pool=f"gpu:{model}",
            )
            self.db.update_allocation(allocation_id, state=AllocationStatus.PENDING.value, slurm_job_id=f"{model}-job")
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=gpu-rtx CPUTot=64 RealMemory=1024000 Gres=gpu:rtx3090:4 GresUsed=gpu:rtx3090:0 State=IDLE Partitions=gpu6\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-rtx gpu6 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000ada", "a6000"],
        )
        scheduler.maintain_allocation_pool()
        gpu_allocations = [item for item in self.db.list_allocations() if item["resource_pool"].startswith("gpu:")]
        self.assertEqual(len(gpu_allocations), 2)
        self.assertFalse(any(item["gpu_model"] == "rtx3090" for item in gpu_allocations))

    def test_gpu_prewarm_keeps_preferred_queue_when_fallback_is_ready(self) -> None:
        fallback_id = self.db.create_allocation(
            account_name="a",
            partition="gpu6",
            node_name="gpu-rtx",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=2,
            gpu_model="rtx3090",
            resource_pool="gpu:rtx3090",
        )
        self.db.update_allocation(fallback_id, state=AllocationStatus.WARM.value, slurm_job_id="rtx-job")
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=gpu-ada CPUTot=64 RealMemory=1024000 Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:0 State=IDLE Partitions=gpu3\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-ada gpu3 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000ada", "a6000"],
        )
        scheduler.maintain_allocation_pool()
        gpu_allocations = [item for item in self.db.list_allocations() if item["resource_pool"].startswith("gpu:")]
        self.assertEqual(len(gpu_allocations), 2)
        self.assertTrue(any(item["gpu_model"] == "a6000ada" and item["state"] == AllocationStatus.PENDING.value for item in gpu_allocations))

    def test_cpu_task_can_borrow_idle_gpu_allocation_without_gpu_cpu_reserve(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu3",
            node_name="gpu-ada",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=1,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            gpu_prewarm_cpu_reserve_per_free_gpu=8,
        )
        fitting = {"cpus": 32, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        too_large = {"cpus": 33, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        self.assertIsNotNone(scheduler.best_allocation_for_task(fitting))
        self.assertIsNone(scheduler.best_allocation_for_task(too_large))

    def test_cpu_task_reserves_cpu_when_gpu_allocation_has_gpu_work(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu3",
            node_name="gpu-ada",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=2,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            free_cpus=24,
            free_gpus=1,
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            gpu_prewarm_cpu_reserve_per_free_gpu=8,
        )
        fitting = {"cpus": 16, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        too_large = {"cpus": 17, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        self.assertIsNotNone(scheduler.best_allocation_for_task(fitting))
        self.assertIsNone(scheduler.best_allocation_for_task(too_large))

    def test_standard_cpu_task_capacity_uses_hard_cpu_slots(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=262144,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            free_cpus=64,
            free_memory_mb=262144,
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        task = {
            "cpus": 4,
            "memory_mb": 8192,
            "scheduling_profile": SchedulingProfile.STANDARD.value,
            "gpus": 0,
            "partition": "auto",
            "node_name": "",
        }
        capacity = scheduler.task_fit_capacity(task)
        self.assertEqual(capacity["fit_slots"], 16)
        self.assertEqual(capacity["memory_pressure_state"], "ok")

    def test_fea_bursty_task_ignores_hard_cpu_and_memory_slots_when_node_is_healthy(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            free_cpus=0,
            free_memory_mb=0,
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 12.0 100000 90000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, fea_max_attach_per_loop=8)
        standard = {"cpus": 4, "memory_mb": 8192, "gpus": 0, "partition": "auto", "node_name": ""}
        fea = {
            **standard,
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
        }
        # Allocation-level hard slots (free_cpus=0/free_mem=0) are ignored for
        # FEA. The memory budget alone would give (90000-60000)//8192 = 3, but
        # the per-allocation FEA CPU cap (total_cpus 8 * factor 1.0 = 8, at 4
        # cpu/task) bounds it to 2 so FEA cannot overshoot the reservation.
        self.assertIsNone(scheduler.best_allocation_for_task(standard))
        self.assertEqual(scheduler.best_allocation_for_task(fea)["id"], allocation_id)
        capacity = scheduler.task_fit_capacity(fea)
        self.assertEqual(capacity["fit_slots"], 2)
        self.assertEqual(capacity["memory_pressure_state"], "ok")

    def test_fea_bursty_max_workers_is_enforced_per_physical_node_for_reservations(self) -> None:
        allocation_ids = []
        for index, node_name in enumerate(["n001", "n001", "n002"]):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu1",
                node_name=node_name,
                total_cpus=64,
                total_memory_mb=100000,
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.WARM.value,
                slurm_job_id=f"alloc-{index}",
            )
            allocation_ids.append(allocation_id)
        for index in range(8):
            task_id = self.db.create_task(
                TaskCreate(
                    f"running-fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=32768,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    max_workers_per_node=8,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                allocation_id=allocation_ids[0],
                account_name="a",
                started_at="CURRENT_TIMESTAMP",
            )
        queued_tasks = [
            self.db.get_task(
                self.db.create_task(
                    TaskCreate(
                        f"queued-fea-{index}",
                        "~/case",
                        "run",
                        cpus=4,
                        memory_mb=32768,
                        scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                        max_workers_per_node=8,
                    )
                )
            )
            for index in range(2)
        ]
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 12.0 100000 80000 some_job\n"
                "n002 cpu1 mix 0 64 1.0 100000 80000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, fea_max_attach_per_loop=24)
        reservations = scheduler.queued_task_allocation_reservations(queued_tasks)
        self.assertNotIn(allocation_ids[1], reservations)
        self.assertEqual(reservations, {allocation_ids[2]: [task["id"] for task in queued_tasks]})

    def test_fea_bursty_ready_attach_respects_physical_node_worker_limit(self) -> None:
        first_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=100000,
        )
        second_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(first_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        self.db.update_allocation(second_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-2")
        for index in range(8):
            task_id = self.db.create_task(
                TaskCreate(
                    f"running-fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=32768,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    max_workers_per_node=8,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                allocation_id=first_id,
                account_name="a",
                started_at="CURRENT_TIMESTAMP",
            )
        queued_id = self.db.create_task(
            TaskCreate(
                "queued-fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                max_workers_per_node=8,
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 12.0 100000 80000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, fea_max_attach_per_loop=24)
        scheduler.assign_ready_fea_tasks()
        self.assertEqual(self.db.get_task(queued_id)["status"], TaskStatus.QUEUED.value)
        self.assertEqual(self.db.get_task(queued_id)["allocation_id"], None)

    def test_fea_bursty_can_exceed_base_worker_limit_when_load_and_memory_allow(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        for index in range(8):
            task_id = self.db.create_task(
                TaskCreate(
                    f"running-fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=8192,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    max_workers_per_node=8,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                allocation_id=allocation_id,
                account_name="a",
                # Mature workers: past the footprint window, so pestat readings
                # are trusted for them.
                started_at=days_ago(1),
                attached_at=days_ago(1),
            )
        queued_id = self.db.create_task(
            TaskCreate(
                "queued-fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=8192,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                max_workers_per_node=8,
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 10.0 100000 100000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, fea_max_attach_per_loop=24)
        diagnostics = scheduler.task_queue_diagnostics(self.db.get_task(queued_id))
        self.assertEqual(diagnostics["queue_state"], "ready")
        scheduler.assign_ready_fea_tasks()
        self.assertEqual(self.db.get_task(queued_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(queued_id)["allocation_id"], allocation_id)

    def test_fea_bursty_prefers_less_loaded_physical_node(self) -> None:
        busy_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=200000,
        )
        quiet_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n002",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(busy_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="busy")
        self.db.update_allocation(quiet_id, state=AllocationStatus.WARM.value, slurm_job_id="quiet")
        for index in range(16):
            task_id = self.db.create_task(
                TaskCreate(
                    f"running-fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=8192,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    max_workers_per_node=8,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                allocation_id=busy_id,
                account_name="a",
                started_at="CURRENT_TIMESTAMP",
            )
        queued_id = self.db.create_task(
            TaskCreate(
                "queued-fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=8192,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                max_workers_per_node=8,
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 10.0 200000 200000 some_job\n"
                "n002 cpu1 mix 0 64 1.0 100000 100000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, fea_max_attach_per_loop=24)
        scheduler.assign_ready_fea_tasks()
        self.assertEqual(self.db.get_task(queued_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(queued_id)["allocation_id"], quiet_id)

    def test_ready_fea_fast_lane_attaches_before_regular_tick_refresh(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            free_cpus=0,
            free_memory_mb=0,
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 0 8 1.0 100000 65000 some_job\n"
            )
        )
        task_id = self.db.create_task(
            TaskCreate(
                "fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                node_name="n001",
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, fea_max_attach_per_loop=24)
        scheduler.assign_ready_fea_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.RUNNING.value)
        self.assertEqual(task["allocation_id"], allocation_id)

    def test_ready_standard_fast_lane_attaches_large_cpu_task_before_fea_backlog(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=131072,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        fea_id = self.db.create_task(
            TaskCreate(
                "fea",
                "~/case",
                "run-fea",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                node_name="n001",
            )
        )
        standard_id = self.db.create_task(TaskCreate("standard-48", "~/case", "run", cpus=48, memory_mb=49152))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_max_new_per_loop=8)
        scheduler.assign_ready_standard_tasks()
        standard = self.db.get_task(standard_id)
        fea = self.db.get_task(fea_id)
        self.assertEqual(standard["status"], TaskStatus.RUNNING.value)
        self.assertEqual(standard["allocation_id"], allocation_id)
        self.assertEqual(fea["status"], TaskStatus.QUEUED.value)

    def test_ready_gpu_fast_lane_attaches_before_regular_tick_refresh(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="n001",
            total_cpus=4,
            total_memory_mb=131072,
            total_gpus=4,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-gpu",
            free_cpus=4,
            free_memory_mb=131072,
            free_gpus=4,
        )
        task_id = self.db.create_task(
            TaskCreate(
                "gpu-ready",
                "~/case",
                "run-gpu",
                cpus=1,
                memory_mb=32768,
                gpus=1,
                gpu_model="a6000",
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_max_new_per_loop=8)
        scheduler.assign_ready_gpu_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.RUNNING.value)
        self.assertEqual(task["allocation_id"], allocation_id)

    def test_ready_fea_background_attach_reserves_without_blocking_scheduler(self) -> None:
        BlockingAttachClient.attach_started.clear()
        BlockingAttachClient.release_attach.clear()
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 0 8 1.0 100000 65000 some_job\n"
            )
        )
        task_id = self.db.create_task(
            TaskCreate(
                "fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                node_name="n001",
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=BlockingAttachClient, fea_max_attach_per_loop=24)
        scheduler.assign_ready_fea_tasks(background=True)
        self.assertTrue(BlockingAttachClient.attach_started.wait(timeout=1))
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.ATTACHING.value)
        self.assertEqual(task["allocation_id"], allocation_id)
        BlockingAttachClient.release_attach.set()
        for _ in range(20):
            if self.db.get_task(task_id)["status"] == TaskStatus.RUNNING.value:
                break
            threading.Event().wait(0.05)
        self.assertEqual(self.db.get_task(task_id)["status"], TaskStatus.RUNNING.value)

    def test_fea_bursty_soft_memory_pressure_blocks_new_attach(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 12.0 100000 55000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        task = {
            "cpus": 4,
            "memory_mb": 8192,
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
            "gpus": 0,
            "partition": "auto",
            "node_name": "",
        }
        self.assertIsNone(scheduler.best_allocation_for_task(task))
        capacity = scheduler.task_fit_capacity(task)
        self.assertEqual(capacity["fit_slots"], 0)
        self.assertEqual(capacity["memory_pressure_state"], "soft_blocked")

    def test_fea_bursty_preferred_node_falls_back_when_requested_node_is_soft_blocked(self) -> None:
        blocked_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n114",
            total_cpus=64,
            total_memory_mb=100000,
        )
        healthy_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n115",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(blocked_id, state=AllocationStatus.WARM.value, slurm_job_id="blocked")
        self.db.update_allocation(healthy_id, state=AllocationStatus.WARM.value, slurm_job_id="healthy")
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n114 cpu1 mix 8 64 12.0 100000 55000 some_job\n"
                "n115 cpu1 mix 8 64 12.0 100000 80000 some_job\n"
            )
        )
        task_id = self.db.create_task(
            TaskCreate(
                "fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                node_name="n114",
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.RUNNING.value)
        self.assertEqual(task["allocation_id"], healthy_id)
        self.assertEqual(task["node_name"], "n114")
        diagnostics = scheduler.task_queue_diagnostics({**task, "status": TaskStatus.QUEUED.value})
        self.assertTrue(diagnostics["preferred_node_relaxed"])

    def test_fea_bursty_queue_reason_reports_node_worker_limit(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n115",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="healthy")
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n115 cpu1 mix 8 64 10.0 100000 80000 some_job\n"
            )
        )
        for index in range(8):
            task_id = self.db.create_task(
                TaskCreate(
                    f"running-fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=32768,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    max_workers_per_node=8,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                allocation_id=allocation_id,
                account_name="a",
                started_at="CURRENT_TIMESTAMP",
            )
        queued_id = self.db.create_task(
            TaskCreate(
                "queued-fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                max_workers_per_node=8,
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        diagnostics = scheduler.task_queue_diagnostics(self.db.get_task(queued_id))
        self.assertEqual(diagnostics["queue_state"], "blocked")
        self.assertEqual(diagnostics["queue_reason"], "FEA max_workers_per_node reached: n115 8/8")

    def test_fea_bursty_strict_node_policy_keeps_requested_node_constraint(self) -> None:
        blocked_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n114",
            total_cpus=64,
            total_memory_mb=100000,
        )
        healthy_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n115",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(blocked_id, state=AllocationStatus.WARM.value, slurm_job_id="blocked")
        self.db.update_allocation(healthy_id, state=AllocationStatus.WARM.value, slurm_job_id="healthy")
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n114 cpu1 mix 8 64 12.0 100000 55000 some_job\n"
                "n115 cpu1 mix 8 64 12.0 100000 80000 some_job\n"
            )
        )
        task_id = self.db.create_task(
            TaskCreate(
                "fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                node_name="n114",
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, fea_node_name_policy="strict")
        scheduler.assign_queued_tasks()
        self.assertEqual(self.db.get_task(task_id)["status"], TaskStatus.QUEUED.value)

    def test_fea_bursty_prefers_requested_node_when_healthy(self) -> None:
        requested_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n114",
            total_cpus=64,
            total_memory_mb=100000,
        )
        fallback_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n115",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(requested_id, state=AllocationStatus.WARM.value, slurm_job_id="requested")
        self.db.update_allocation(fallback_id, state=AllocationStatus.WARM.value, slurm_job_id="fallback")
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n114 cpu1 mix 8 64 12.0 100000 80000 some_job\n"
                "n115 cpu1 mix 8 64 12.0 100000 80000 some_job\n"
            )
        )
        task_id = self.db.create_task(
            TaskCreate(
                "fea",
                "~/case",
                "run",
                cpus=4,
                memory_mb=32768,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                node_name="n114",
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.RUNNING.value)
        self.assertEqual(task["allocation_id"], requested_id)

    def test_fea_bursty_queued_demand_consumes_inflight_attach_slots(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=64,
            total_memory_mb=262144,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-cpu",
            free_cpus=64,
            free_memory_mb=262144,
        )
        task_ids = [
            self.db.create_task(
                TaskCreate(
                    f"fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=32768,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
            for index in range(9)
        ]
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, fea_max_attach_per_loop=8)
        task = scheduler.next_queued_task_without_inflight_capacity()
        self.assertIsNotNone(task)
        self.assertEqual(task["id"], task_ids[8])

    def test_fea_bursty_excess_queued_demand_opens_another_pool(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=64,
            total_memory_mb=262144,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-cpu",
            drain_reason="queued CPU demand",
        )
        for index in range(9):
            self.db.create_task(
                TaskCreate(
                    f"fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=32768,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            fea_max_attach_per_loop=8,
        )
        scheduler.maintain_allocation_pool()
        live_allocations = [
            allocation
            for allocation in self.db.list_allocations()
            if allocation["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live_allocations), 2)

    def test_pending_fea_fit_slots_respect_allocation_cpu_cap(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="",
            total_cpus=64,
            total_memory_mb=262144,
            resource_pool="cpu",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-cpu",
            drain_reason="queued CPU demand",
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            fea_max_attach_per_loop=24,
            fea_node_requested_cpu_factor=1.0,
        )
        task = {
            "id": 999,
            "cpus": 4,
            "memory_mb": 32768,
            "gpus": 0,
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
            "max_workers_per_node": 0,
        }
        self.assertEqual(scheduler.fit_slots_for_allocation(self.db.get_allocation(allocation_id), task), 16)

    def test_fea_overload_state_ignores_node_wide_pestat_load(self) -> None:
        allocation_id = self.create_fea_allocation("n001", total_cpus=64)
        self.create_running_fea_tasks(allocation_id, count=8, cpus=4)
        self.create_queued_fea_task()
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 0 256 809.37 1024000 900000 other_users\n"
                "n002 cpu1 mix 0 256 1.00 1024000 900000 idle\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            fea_overload_scale_out_load_factor=2.0,
            fea_overload_scale_out_seconds=300,
        )
        scheduler.update_fea_overload_state()
        opened = scheduler.scale_out_for_fea_overload()
        self.assertFalse(opened)
        self.assertEqual(scheduler._fea_overload_since_by_node, {})
        self.assertEqual(len(self.db.list_allocations()), 1)
        self.assertEqual(FakeClient.allocation_submits, [])

    def test_fea_overload_state_allows_exactly_two_hundred_percent(self) -> None:
        allocation_id = self.create_fea_allocation("n001", total_cpus=64)
        self.create_running_fea_tasks(allocation_id, count=32, cpus=4)
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            fea_overload_scale_out_load_factor=2.0,
            fea_overload_scale_out_seconds=300,
        )
        self.assertEqual(
            scheduler.fea_owned_node_pressures()["n001"],
            {"workers": 32, "requested_cpus": 128, "owned_cpus": 64},
        )
        scheduler.update_fea_overload_state()
        self.assertNotIn("n001", scheduler._fea_overload_since_by_node)

    def test_fea_sustained_owned_cpu_overload_opens_one_cpu_pool(self) -> None:
        allocation_id = self.create_fea_allocation("n001", total_cpus=64)
        self.create_running_fea_tasks(allocation_id, count=33, cpus=4)
        self.create_queued_fea_task()
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 128 256 40.0 1024000 800000 busy\n"
                "n002 cpu1 mix 0 256 1.0 1024000 900000 idle\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            fea_overload_scale_out_load_factor=2.0,
            fea_overload_scale_out_seconds=300,
        )
        scheduler.update_fea_overload_state()
        scheduler._fea_overload_since_by_node["n001"] = time.monotonic() - 301
        opened = scheduler.scale_out_for_fea_overload()
        self.assertTrue(opened)
        allocations = self.db.list_allocations(limit=100)
        new_allocations = [allocation for allocation in allocations if int(allocation["id"]) != allocation_id]
        self.assertEqual(len(new_allocations), 1)
        self.assertEqual(new_allocations[0]["state"], AllocationStatus.PENDING.value)
        self.assertEqual(new_allocations[0]["resource_pool"], "cpu")
        self.assertIn(
            "queued FEA overload scale-out n001 owned requested CPU 132/64",
            new_allocations[0]["drain_reason"],
        )
        self.assertEqual(len(FakeClient.allocation_submits), 1)

        opened_again = scheduler.scale_out_for_fea_overload()
        self.assertFalse(opened_again)
        self.assertEqual(len(self.db.list_allocations(limit=100)), 2)

    def test_sustained_fea_overload_blocks_more_attach_to_that_node(self) -> None:
        allocation_id = self.create_fea_allocation("n001", total_cpus=64, state=AllocationStatus.WARM.value)
        self.create_running_fea_tasks(allocation_id, count=33, cpus=4)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 32 64 20.0 262144 220000 busy\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            fea_load_target=4.0,
            fea_overload_scale_out_load_factor=2.0,
            fea_overload_scale_out_seconds=300,
        )
        scheduler.update_fea_overload_state()
        scheduler._fea_overload_since_by_node["n001"] = time.monotonic() - 301
        allocation = self.db.get_allocation(allocation_id)
        task = {
            "cpus": 4,
            "memory_mb": 8192,
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
            "gpus": 0,
            "partition": "auto",
            "node_name": "",
        }
        self.assertFalse(scheduler.fea_allocation_accepts_task(allocation))
        self.assertEqual(scheduler.fit_slots_for_allocation(allocation, task), 0)

    def test_fea_hard_memory_pressure_cancels_newest_fea_task_only(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        old_fea = self.db.create_task(
            TaskCreate(
                "fea-old",
                "~/case",
                "run",
                cpus=4,
                memory_mb=8192,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
            )
        )
        new_fea = self.db.create_task(
            TaskCreate(
                "fea-new",
                "~/case",
                "run",
                cpus=4,
                memory_mb=8192,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
            )
        )
        standard = self.db.create_task(TaskCreate("standard", "~/case", "run", cpus=4, memory_mb=8192))
        for task_id, attached_at in [
            (old_fea, "2026-01-01 00:00:00"),
            (new_fea, "2026-01-01 00:01:00"),
            (standard, "2026-01-01 00:02:00"),
        ]:
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                account_name="a",
                allocation_id=allocation_id,
                wrapper_pid=str(1000 + task_id),
                attached_at=attached_at,
                started_at=attached_at,
            )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 12.0 100000 35000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.handle_fea_memory_pressure()
        requeued = self.db.get_task(new_fea)
        self.assertEqual(requeued["status"], TaskStatus.QUEUED.value)
        self.assertEqual(requeued["attempt_count"], 1)
        self.assertIsNone(requeued["allocation_id"])
        self.assertEqual(requeued["failure_message"], "")
        self.assertEqual(self.db.get_task(old_fea)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(standard)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(FakeClient.cancelled_tasks, [new_fea])

    def test_fea_hard_memory_pressure_fails_task_at_attempt_cap(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(
            TaskCreate(
                "fea-retried",
                "~/case",
                "run",
                cpus=4,
                memory_mb=8192,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
            )
        )
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            account_name="a",
            allocation_id=allocation_id,
            wrapper_pid="2000",
            attempt_count=2,
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 12.0 100000 35000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.handle_fea_memory_pressure()
        failed = self.db.get_task(task_id)
        self.assertEqual(failed["status"], TaskStatus.FAILED.value)
        self.assertEqual(failed["attempt_count"], 3)
        self.assertIn("after 3 attempts", failed["failure_message"])

    def test_fea_dynamic_extra_slots_reserves_declared_footprint_of_young_workers(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 4.0 100000 90000 some_job\n"
            )
        )
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        allocation = self.db.get_allocation(allocation_id)
        task = {"id": 10, "cpus": 4, "memory_mb": 8192}
        baseline = scheduler.fea_dynamic_extra_slots(allocation, task)
        self.assertGreater(baseline, 0)
        # Freshly attached FEA workers have not grown into their footprint yet;
        # their declared cpus/memory must be reserved out of the budgets.
        for index in range(3):
            task_id = self.db.create_task(
                TaskCreate(
                    f"fea-young-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=8192,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                account_name="a",
                allocation_id=allocation_id,
                wrapper_pid=str(4000 + index),
                attached_at="CURRENT_TIMESTAMP",
            )
        scheduler._fea_footprint_cache = None
        discounted = scheduler.fea_dynamic_extra_slots(allocation, task)
        self.assertLess(discounted, baseline)
        # Once workers are older than the maturity window, observed pestat
        # numbers are trusted again.
        with self.db.connect() as conn:
            conn.execute("UPDATE tasks SET attached_at = datetime('now', '-2 hours')")
        scheduler._fea_footprint_cache = None
        matured = scheduler.fea_dynamic_extra_slots(allocation, task)
        self.assertGreater(matured, discounted)

    def test_fea_dynamic_extra_slots_caps_attaches_per_node_per_tick(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 4.0 100000 90000 some_job\n"
            )
        )
        scheduler = Scheduler(
            self.db, self.accounts, 30, client_factory=FakeClient, fea_max_attach_per_node_per_loop=2
        )
        allocation = {"id": 1, "state": AllocationStatus.WARM.value, "node_name": "n001"}
        task = {"id": 10, "cpus": 1, "memory_mb": 128}
        scheduler._record_attach_delta(allocation, task)
        scheduler._record_attach_delta(allocation, task)
        self.assertEqual(scheduler.fea_dynamic_extra_slots(allocation, task), 0)

    def test_fea_stale_pestat_allows_single_slot_when_last_row_was_healthy(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 4.0 100000 90000 some_job\n"
            )
        )
        with self.db.connect() as conn:
            conn.execute("UPDATE pestat_nodes SET observed_at = datetime('now', '-300 seconds')")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        allocation = {"id": 1, "state": AllocationStatus.WARM.value, "node_name": "n001"}
        task = {"id": 10, "cpus": 4, "memory_mb": 8192}
        self.assertIsNone(scheduler.pestat_node_for_allocation(allocation))
        self.assertEqual(scheduler.fea_dynamic_extra_slots(allocation, task), 1)

    def test_fea_fit_respects_node_cap_without_max_workers_per_node(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=48,
            total_memory_mb=380000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        for index in range(12):
            task_id = self.db.create_task(
                TaskCreate(
                    f"fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=4096,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                account_name="a",
                allocation_id=allocation_id,
                wrapper_pid=str(6000 + index),
                attached_at=days_ago(1),
                started_at=days_ago(1),
            )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 48 4.0 380000 350000 busy\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        allocation = self.db.get_allocation(allocation_id)
        # 12 workers x 4 cpus = 48 == node cap: a max_workers_per_node=0 task
        # must see zero fit slots here (it used to see fea_max_attach_per_loop).
        queued = {
            "id": 999,
            "cpus": 4,
            "memory_mb": 4096,
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
            "max_workers_per_node": 0,
        }
        self.assertEqual(scheduler.fit_slots_for_allocation(allocation, queued), 0)
        self.assertIsNone(scheduler.best_allocation_for_task(queued))

    def test_fea_best_allocation_skips_full_cpu_cap_and_uses_spare_allocation(self) -> None:
        full_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=100000,
        )
        spare_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n002",
            total_cpus=8,
            total_memory_mb=100000,
        )
        for allocation_id, job_id in ((full_id, "alloc-full"), (spare_id, "alloc-spare")):
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=job_id,
            )
        for index in range(2):
            task_id = self.db.create_task(
                TaskCreate(
                    f"running-fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=4096,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                account_name="a",
                allocation_id=full_id,
                wrapper_pid=str(7000 + index),
            )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 8 1.0 100000 90000 busy\n"
                "n002 cpu1 mix 8 8 1.0 100000 90000 idle\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        queued = {
            "id": 999,
            "cpus": 4,
            "memory_mb": 4096,
            "gpus": 0,
            "partition": "auto",
            "node_name": "",
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
            "max_workers_per_node": 0,
        }
        self.assertEqual(scheduler.best_allocation_for_task(queued)["id"], spare_id)

    def test_fea_node_cpu_cap_limits_total_requested_cpus(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=48,
            total_memory_mb=380000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        for index in range(10):
            task_id = self.db.create_task(
                TaskCreate(
                    f"fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=4096,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                account_name="a",
                allocation_id=allocation_id,
                wrapper_pid=str(3000 + index),
            )
        # Node reports low load and plenty of memory, so the load-based ramp
        # alone would allow many more workers.
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 48 4.0 380000 350000 busy\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        allocation = self.db.get_allocation(allocation_id)
        task = {"id": 999, "cpus": 4, "memory_mb": 4096, "scheduling_profile": SchedulingProfile.FEA_BURSTY.value}
        # 10 workers x 4 cpus = 40 requested; cap 48 * 1.0 -> only 2 more 4-cpu workers.
        self.assertEqual(scheduler.fea_node_cpu_cap_remaining(allocation, task), 2)
        limit = scheduler.fea_effective_worker_limit(allocation, task, current_workers=10, base_limit=32)
        self.assertEqual(limit, 12)
        uncapped = Scheduler(
            self.db, self.accounts, 30, client_factory=FakeClient, fea_node_requested_cpu_factor=0
        )
        self.assertIsNone(uncapped.fea_node_cpu_cap_remaining(allocation, task))
        self.assertGreaterEqual(
            uncapped.fea_effective_worker_limit(allocation, task, current_workers=10, base_limit=32), 32
        )

    def test_attach_completion_does_not_stomp_concurrent_requeue(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=48,
            total_memory_mb=380000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(
            TaskCreate("racy", "~/case", "run", cpus=4, scheduling_profile=SchedulingProfile.FEA_BURSTY.value)
        )
        self.db.update_task(
            task_id,
            status=TaskStatus.ATTACHING.value,
            account_name="a",
            allocation_id=allocation_id,
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        allocation = self.db.get_allocation(allocation_id)
        task = self.db.get_task(task_id)
        # A rebalance requeues the task while its attach is still in flight.
        scheduler.requeue_task_for_rebalance(task, "test rebalance")
        finished = scheduler.finish_reserved_task_attach(task, allocation, self.accounts[0])
        self.assertFalse(finished)
        after = self.db.get_task(task_id)
        self.assertEqual(after["status"], TaskStatus.QUEUED.value)
        self.assertIsNone(after["allocation_id"])
        # The freshly started remote worker was cancelled again.
        self.assertIn(task_id, FakeClient.cancelled_tasks)

    def test_cleanup_globs_persist_and_parse_for_terminal_hook(self) -> None:
        task_id = self.db.create_task(
            TaskCreate("with-cleanup", "~/case", "run", cleanup_globs="simulation,aedt_temp")
        )
        task = self.db.get_task(task_id)
        self.assertEqual(task["cleanup_globs"], "simulation,aedt_temp")

    def test_count_tasks_grouped_by_status_with_prefix(self) -> None:
        for index in range(3):
            self.db.create_task(TaskCreate(f"mft-camp-w1-{index}", "~/case", "run"))
        other = self.db.create_task(TaskCreate("other-job", "~/case", "run"))
        self.db.update_task(other, status=TaskStatus.COMPLETED.value)
        done = self.db.create_task(TaskCreate("mft-camp-w1-done", "~/case", "run"))
        self.db.update_task(done, status=TaskStatus.COMPLETED.value)
        counts = self.db.count_tasks_grouped_by_status(name_prefix="mft-camp-w1")
        self.assertEqual(counts, {"queued": 3, "completed": 1})
        all_counts = self.db.count_tasks_grouped_by_status()
        self.assertEqual(all_counts["completed"], 2)

    def test_storage_guard_blocks_attach_below_threshold(self) -> None:
        account = AccountConfig(
            "a", "host", 22, "a", "key", "/work", 4, 10, 10, storage_path="/work", storage_quota_gb=100.0
        )
        scheduler = Scheduler(
            self.db, [account], 30, client_factory=FakeClient, storage_guard_min_free_gb=5.0
        )
        scheduler._storage_cache["a"] = (time.time(), 97.0)
        self.assertTrue(scheduler.account_storage_blocked(account))
        scheduler._storage_cache["a"] = (time.time(), 50.0)
        self.assertFalse(scheduler.account_storage_blocked(account))
        # Guard disabled or quota unknown -> never blocks.
        scheduler._storage_cache["a"] = (time.time(), None)
        self.assertFalse(scheduler.account_storage_blocked(account))
        unguarded = Scheduler(self.db, [account], 30, client_factory=FakeClient)
        unguarded._storage_cache["a"] = (time.time(), 99.0)
        self.assertFalse(unguarded.account_storage_blocked(account))

    def test_workspace_prune_glob_validation(self) -> None:
        ok = Scheduler._workspace_prune_glob_ok
        self.assertTrue(ok("*.aedtresults"))
        self.assertTrue(ok("scratch_*"))
        self.assertFalse(ok("*"))
        self.assertFalse(ok("?"))
        self.assertFalse(ok("*.*"))
        self.assertFalse(ok("../evil"))
        self.assertFalse(ok("a/b"))
        self.assertFalse(ok(""))
        self.assertFalse(ok("  "))

    def test_workspace_prune_config_parses_from_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "app.yaml"
            path.write_text(
                "\n".join(
                    [
                        "cleanup:",
                        "  workspace_prune_globs: ['*.aedtresults', '*.asol']",
                        "  workspace_prune_min_age_seconds: 43200",
                    ]
                ),
                encoding="utf-8",
            )
            config = load_app_config(path)
        self.assertEqual(config.cleanup_workspace_prune_globs, ["*.aedtresults", "*.asol"])
        self.assertEqual(config.cleanup_workspace_prune_min_age_seconds, 43200)

    def test_reprioritized_task_moves_to_front_of_queue(self) -> None:
        first = self.db.create_task(
            TaskCreate("first", "~/case", "run", cpus=4, scheduling_profile=SchedulingProfile.FEA_BURSTY.value)
        )
        second = self.db.create_task(
            TaskCreate("second", "~/case", "run", cpus=4, scheduling_profile=SchedulingProfile.FEA_BURSTY.value)
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual([task["id"] for task in scheduler.queued_fea_tasks()], [first, second])
        self.db.update_task(second, priority=10)
        self.assertEqual([task["id"] for task in scheduler.queued_fea_tasks()], [second, first])

    def test_enforce_fea_node_cpu_cap_drains_newest_workers_gradually(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=48,
            total_memory_mb=380000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_ids = []
        for index in range(20):
            task_id = self.db.create_task(
                TaskCreate(
                    f"fea-{index}",
                    "~/case",
                    "run",
                    cpus=4,
                    memory_mb=4096,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                account_name="a",
                allocation_id=allocation_id,
                wrapper_pid=str(5000 + index),
                attached_at=f"2026-01-01 00:{index:02d}:00",
            )
            task_ids.append(task_id)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 48 4.0 380000 350000 busy\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        # 20 workers x 4 cpus = 80 requested vs cap 48 -> 8 newest drained (== per-tick limit).
        scheduler.enforce_fea_node_cpu_cap()
        newest_eight = task_ids[-8:]
        for task_id in newest_eight:
            task = self.db.get_task(task_id)
            self.assertEqual(task["status"], TaskStatus.QUEUED.value)
            self.assertEqual(task["attempt_count"], 0)
            self.assertIsNone(task["allocation_id"])
        for task_id in task_ids[:-8]:
            self.assertEqual(self.db.get_task(task_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(sorted(FakeClient.cancelled_tasks), sorted(newest_eight))
        # 12 workers x 4 = 48 == cap: a second pass drains nothing further.
        scheduler.enforce_fea_node_cpu_cap()
        self.assertEqual(len(FakeClient.cancelled_tasks), 8)

    def test_fea_stale_pestat_blocks_when_last_row_was_pressured(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 12.0 100000 35000 some_job\n"
            )
        )
        with self.db.connect() as conn:
            conn.execute("UPDATE pestat_nodes SET observed_at = datetime('now', '-300 seconds')")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        allocation = {"id": 1, "state": AllocationStatus.WARM.value, "node_name": "n001"}
        task = {"id": 10, "cpus": 4, "memory_mb": 8192}
        self.assertEqual(scheduler.fea_dynamic_extra_slots(allocation, task), 0)

    def test_fea_hard_memory_pressure_does_not_cancel_standard_task(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=100000,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        standard = self.db.create_task(TaskCreate("standard", "~/case", "run", cpus=4, memory_mb=8192))
        self.db.update_task(
            standard,
            status=TaskStatus.RUNNING.value,
            account_name="a",
            allocation_id=allocation_id,
            wrapper_pid="1234",
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "n001 cpu1 mix 8 64 12.0 100000 35000 some_job\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.handle_fea_memory_pressure()
        self.assertEqual(self.db.get_task(standard)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(FakeClient.cancelled_tasks, [])

    def test_gpu_task_does_not_attach_when_gpu_matches_but_cpu_is_exhausted(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="gpu-a6000",
            total_cpus=4,
            total_memory_mb=65536,
            total_gpus=1,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            free_cpus=0,
            free_gpus=1,
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        task = {"cpus": 4, "memory_mb": 2048, "gpus": 1, "gpu_model": "a6000", "partition": "auto", "node_name": ""}
        self.assertIsNone(scheduler.best_allocation_for_task(task))
        capacity = scheduler.task_fit_capacity(task)
        self.assertEqual(capacity["fit_slots"], 0)

        self.db.update_allocation(allocation_id, free_cpus=4)
        self.assertEqual(scheduler.best_allocation_for_task(task)["id"], allocation_id)
        self.assertEqual(scheduler.task_fit_capacity(task)["fit_slots"], 1)

    def test_demand_prewarm_counts_inflight_slots_instead_of_binary_capacity(self) -> None:
        for name in ("n001", "n002"):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu1",
                node_name=name,
                total_cpus=64,
                total_memory_mb=65536,
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=f"alloc-{name}",
                free_cpus=12,
                free_memory_mb=65536,
            )
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu1",
            node_name="g001",
            total_cpus=48,
            total_memory_mb=196608,
            total_gpus=2,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-gpu",
            free_cpus=48,
            free_memory_mb=196608,
            free_gpus=2,
        )
        task_ids = [
            self.db.create_task(TaskCreate(f"cpu-{index}", "~/case", "run", cpus=16, memory_mb=32768))
            for index in range(4)
        ]
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        task = scheduler.next_queued_task_without_inflight_capacity()
        self.assertIsNotNone(task)
        self.assertEqual(task["id"], task_ids[3])

    def test_near_drain_allocation_does_not_accept_new_tasks(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            started_at="2000-01-01 00:00:00",
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_drain_after_seconds=3600,
            allocation_attach_stop_before_drain_seconds=1800,
        )
        task = {"cpus": 1, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        self.assertIsNone(scheduler.best_allocation_for_task(task))

    def test_gpu_task_requires_matching_model(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="gpu-a6000",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=1,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        task = {"cpus": 8, "memory_mb": 2048, "gpus": 1, "gpu_model": "a6000ada", "partition": "auto", "node_name": ""}
        self.assertIsNone(scheduler.best_allocation_for_task(task))

    def test_allocation_usage_summary_excludes_pending_when_filtered(self) -> None:
        allocations = [
            {"state": "active", "total_cpus": 8, "free_cpus": 4, "total_gpus": 1, "free_gpus": 0, "total_memory_mb": 8192, "free_memory_mb": 4096},
            {"state": "pending", "total_cpus": 64, "free_cpus": 64, "total_gpus": 2, "free_gpus": 2, "total_memory_mb": 65536, "free_memory_mb": 65536},
        ]
        ready = [item for item in allocations if item["state"] in {"active", "warm", "draining", "closing"}]
        self.assertEqual(sum(item["total_cpus"] - item["free_cpus"] for item in ready), 4)
        self.assertEqual(sum(item["total_cpus"] for item in ready), 8)
        self.assertEqual(sum(item["total_gpus"] - item["free_gpus"] for item in ready), 1)
        self.assertEqual(sum(item["total_gpus"] for item in ready), 1)

    def test_gpu_capacity_summary_separates_cluster_and_scheduler_capacity(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-ada CPUTot=64 RealMemory=1024000 Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:2(IDX:0-1) State=MIXED Partitions=gpu3\n"
        )
        self.db.replace_node_inventory(inventory)
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu3",
            node_name="gpu-ada",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=1,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        summary = scheduler.gpu_capacity_summary()[0]
        self.assertEqual(summary["gpu_model"], "a6000ada")
        self.assertEqual(summary["cluster_total_gpus"], 4)
        self.assertEqual(summary["cluster_used_gpus"], 2)
        self.assertEqual(summary["cluster_free_gpus"], 2)
        self.assertEqual(summary["scheduler_owned_gpus"], 1)
        self.assertEqual(summary["scheduler_free_gpus"], 1)
        self.assertEqual(summary["single_node_max_free_gpus"], 2)
        self.assertEqual(summary["single_node_max_free_cpus"], 64)
        self.assertEqual(summary["single_node_max_free_gpu_node"], "gpu-ada")

    def test_gpu_capacity_summary_reports_best_single_node_gpu_and_cpu_fit(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu4-a CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:1 State=MIXED Partitions=gpu4\n"
            "NodeName=gpu4-b CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:1 State=MIXED Partitions=gpu4\n"
            "NodeName=gpu5-a CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu4-a gpu4 mix 46 56 10.0 1024000 900000 busy\n"
                "gpu4-b gpu4 mix 28 56 10.0 1024000 800000 busy\n"
                "gpu5-a gpu5 mix 4 64 1.0 1024000 950000 busy\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        summary = scheduler.gpu_capacity_summary()[0]
        self.assertEqual(summary["gpu_model"], "a6000")
        self.assertEqual(summary["cluster_free_gpus"], 8)
        self.assertEqual(summary["single_node_max_free_gpus"], 3)
        self.assertEqual(summary["single_node_max_free_cpus"], 28)
        self.assertEqual(summary["single_node_max_free_gpu_node"], "gpu4-b")
        self.assertEqual(summary["single_node_max_free_gpu_partition"], "gpu4")

    def test_gpu_capacity_summary_ignores_reserved_node_for_best_single_node_fit(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu4-resv CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu4\n"
            "NodeName=gpu4-fit CPUTot=56 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu4\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu4-resv gpu4 resv 0 56 0.0 1024000 900000 reservation\n"
                "gpu4-fit gpu4 mix 52 56 52.0 1024000 800000 busy\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        summary = scheduler.gpu_capacity_summary()[0]
        self.assertEqual(summary["cluster_free_gpus"], 8)
        self.assertEqual(summary["single_node_max_free_gpus"], 4)
        self.assertEqual(summary["single_node_max_free_cpus"], 4)
        self.assertEqual(summary["single_node_max_free_gpu_node"], "gpu4-fit")

    def test_gpu_capacity_summary_reports_pestat_sched_free_cpu_not_load_adjusted_cpu(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 mix 60 64 60.14 1024000 900000 busy\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        summary = scheduler.gpu_capacity_summary()[0]
        self.assertEqual(summary["single_node_max_free_gpus"], 4)
        self.assertEqual(summary["single_node_max_free_cpus"], 4)
        self.assertEqual(summary["single_node_max_free_gpu_node"], "gpu-a6000")


class BatchParserTests(unittest.TestCase):
    def test_parse_squeue_table(self) -> None:
        output = "\n".join(
            [
                "101|RUNNING|None|n001|gpu1",
                "102|PENDING|(Resources)|(None)|gpu1,gpu3",
                "garbage-line",
                "103|COMPLETING||n002",
            ]
        )
        table = parse_squeue_table(output)
        self.assertEqual(table["101"], ("RUNNING", "None", "n001", "gpu1"))
        self.assertEqual(table["102"], ("PENDING", "(Resources)", "", "gpu1,gpu3"))
        self.assertEqual(table["103"], ("COMPLETING", "", "n002", ""))
        self.assertNotIn("garbage-line", table)

    def test_parse_sacct_states_skips_substeps_and_keeps_first_word(self) -> None:
        output = "\n".join(
            [
                "201  COMPLETED",
                "201.batch  COMPLETED",
                "202  CANCELLED by 1000",
                "",
            ]
        )
        states = parse_sacct_states(output)
        self.assertEqual(states, {"201": "COMPLETED", "202": "CANCELLED"})

    def test_parse_task_probe_output(self) -> None:
        output = "17|0\n18|RUNNING\n\n19|2\n20|UNKNOWN\nnoise\n"
        probes = parse_task_probe_output(output)
        self.assertEqual(probes, {17: "0", 18: "RUNNING", 19: "2", 20: "UNKNOWN"})


class LegacyClientFallbackTests(unittest.TestCase):
    """Scheduler must keep working with clients that only implement the
    singular state/task_state methods (external fakes)."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_job_states_falls_back_to_singular_calls(self) -> None:
        class LegacyClient:
            def state(self, slurm_job_id: str) -> JobStatus:
                return JobStatus.SUBMITTED

            def pending_reason(self, slurm_job_id: str) -> str:
                return "(Priority)"

            def allocation_node_name(self, slurm_job_id: str) -> str:
                return ""

        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        states = scheduler._job_states(LegacyClient(), ["1", "2"])
        self.assertEqual(states["1"].status, JobStatus.SUBMITTED)
        self.assertEqual(states["1"].pending_reason, "(Priority)")

    def test_task_probes_falls_back_to_singular_calls(self) -> None:
        class LegacyClient:
            def task_state(self, task: dict) -> JobStatus:
                return JobStatus.FAILED

            def task_exit_code(self, task: dict) -> int | None:
                return 9

        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        probes = scheduler._task_probes(LegacyClient(), [{"id": 5}])
        self.assertEqual(probes[5], TaskProbe(status=JobStatus.FAILED, exit_code=9))


class _FakeSSHChannel:
    def __init__(self, ready: bool = False, exit_code: int = 0):
        self.ready = ready
        self.exit_code = exit_code
        self.closed = False

    def exit_status_ready(self) -> bool:
        return self.ready

    def recv_exit_status(self) -> int:
        return self.exit_code

    def close(self) -> None:
        self.closed = True


class _FakeSSHStream:
    def __init__(self, channel: _FakeSSHChannel):
        self.channel = channel

    def read(self) -> bytes:
        return b""


class _FakeParamikoClient:
    def __init__(self, channel: _FakeSSHChannel):
        self._channel = channel

    def exec_command(self, command: str, timeout: float | None = None):
        stream = _FakeSSHStream(self._channel)
        return stream, stream, stream


class SSHTimeoutTests(unittest.TestCase):
    def make_session(self, channel: _FakeSSHChannel, default_timeout: float | None) -> SSHSession:
        account = AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)
        session = SSHSession(account, default_timeout=default_timeout)
        session.client = _FakeParamikoClient(channel)
        return session

    def test_run_times_out_by_default_and_closes_channel(self) -> None:
        channel = _FakeSSHChannel(ready=False)
        session = self.make_session(channel, default_timeout=0.15)
        with self.assertRaises(RemoteCommandTimeout) as ctx:
            session.run("sleep forever")
        self.assertIsInstance(ctx.exception, TimeoutError)
        self.assertIsInstance(ctx.exception, RemoteExecutionError)
        self.assertTrue(channel.closed)
        self.assertIn("a", str(ctx.exception))

    def test_run_with_explicit_none_skips_deadline(self) -> None:
        channel = _FakeSSHChannel(ready=True, exit_code=3)
        session = self.make_session(channel, default_timeout=0.15)
        result = session.run("quick", timeout=None)
        self.assertEqual(result.exit_code, 3)
        self.assertFalse(channel.closed)

    def test_run_completes_within_deadline(self) -> None:
        channel = _FakeSSHChannel(ready=True, exit_code=0)
        session = self.make_session(channel, default_timeout=5)
        result = session.run("quick")
        self.assertEqual(result.exit_code, 0)


class WatchdogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_scheduler(self) -> Scheduler:
        scheduler = Scheduler(
            self.db, self.accounts, 30, client_factory=FakeClient, watchdog_stall_seconds=1
        )
        self.exits: list[int] = []
        self.force_closes: list[bool] = []
        scheduler._watchdog_exit = lambda code: self.exits.append(code)
        scheduler._tick_sessions_force_close = lambda: self.force_closes.append(True)
        return scheduler

    def test_watchdog_ignores_healthy_tick(self) -> None:
        scheduler = self.make_scheduler()
        scheduler._tick_seq = 5
        scheduler._tick_started_at = time.monotonic()
        self.assertEqual(scheduler._watchdog_check_once(-1), -1)
        self.assertEqual(self.force_closes, [])
        self.assertEqual(self.exits, [])

    def test_watchdog_ignores_idle_scheduler(self) -> None:
        scheduler = self.make_scheduler()
        scheduler._tick_started_at = None
        self.assertEqual(scheduler._watchdog_check_once(3), -1)
        self.assertEqual(self.exits, [])

    def test_watchdog_two_stage_escalation(self) -> None:
        scheduler = self.make_scheduler()
        scheduler._tick_seq = 7
        scheduler._tick_started_at = time.monotonic() - 10
        suspect = scheduler._watchdog_check_once(-1)
        self.assertEqual(suspect, 7)
        self.assertEqual(self.force_closes, [True])
        self.assertEqual(self.exits, [])
        suspect = scheduler._watchdog_check_once(suspect)
        self.assertEqual(self.exits, [70])

    def test_watchdog_resets_when_new_tick_stalls(self) -> None:
        scheduler = self.make_scheduler()
        scheduler._tick_seq = 7
        scheduler._tick_started_at = time.monotonic() - 10
        suspect = scheduler._watchdog_check_once(-1)
        scheduler._tick_seq = 8
        suspect = scheduler._watchdog_check_once(suspect)
        self.assertEqual(suspect, 8)
        self.assertEqual(self.exits, [])
        self.assertEqual(self.force_closes, [True, True])


class LicenseMonitorTests(unittest.TestCase):
    def test_parse_lmstat_features(self) -> None:
        from slurm_scheduler.scheduler import parse_lmstat_features

        output = "\n".join(
            [
                "License server status: 1055@172.16.10.81",
                "172.16.10.81: license server UP v11.19.9",
                "Users of ansys:  (Total of 550 licenses issued;  Total of 2 licenses in use)",
                "Users of electronics_desktop:  (Total of 550 licenses issued;  Total of 12 licenses in use)",
                "Users of single_lic:  (Total of 1 license issued;  Total of 1 license in use)",
                "garbage line",
            ]
        )
        features = parse_lmstat_features(output)
        self.assertEqual(
            features,
            [
                {"feature": "ansys", "total": 550, "used": 2},
                {"feature": "electronics_desktop", "total": 550, "used": 12},
                {"feature": "single_lic", "total": 1, "used": 1},
            ],
        )
        self.assertEqual(parse_lmstat_features(""), [])


class ObservabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_record_and_list_events(self) -> None:
        self.db.record_event("allocation_opened", "warm pool", entity_type="allocation", entity_id="7", account_name="a")
        self.db.record_event("task_failed", "boom", entity_type="task", entity_id="9")
        events = self.db.list_events(limit=10)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["kind"], "task_failed")
        self.assertEqual(events[1]["entity_id"], "7")

    def test_prune_old_rows_deletes_only_cleaned_terminal_rows(self) -> None:
        old_clean = self.db.create_task(TaskCreate("old-clean", "~/w", "run"))
        self.db.update_task(old_clean, status=TaskStatus.COMPLETED.value, remote_dir="")
        old_dirty = self.db.create_task(TaskCreate("old-dirty", "~/w", "run"))
        self.db.update_task(old_dirty, status=TaskStatus.COMPLETED.value, remote_dir="/work/task-2")
        live = self.db.create_task(TaskCreate("live", "~/w", "run"))
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE tasks SET finished_at = datetime('now', '-30 days'), updated_at = datetime('now', '-30 days')"
            )
        self.db.record_event("old_event", "stale")
        with self.db.connect() as conn:
            conn.execute("UPDATE scheduler_events SET created_at = datetime('now', '-30 days')")
        deleted = self.db.prune_old_rows(
            "2100-01-01 00:00:00",
            "2100-01-01 00:00:00",
        )
        self.assertEqual(deleted["tasks"], 1)
        self.assertEqual(deleted["events"], 1)
        self.assertIsNone(self.db.get_task(old_clean))
        self.assertIsNotNone(self.db.get_task(old_dirty))
        self.assertIsNotNone(self.db.get_task(live))

    def test_health_status_reports_thread_and_tick_state(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        health = scheduler.health_status()
        self.assertFalse(health["scheduler_thread_alive"])
        self.assertFalse(health["scheduler_stalled"])
        scheduler.tick()
        health = scheduler.health_status()
        self.assertIsNotNone(health["last_tick_duration_seconds"])
        self.assertTrue(health["last_tick_completed_at"])
        self.assertEqual(health["consecutive_tick_failures"], 0)


class CpuPoolPartitionSpreadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)]
        # cpu1 busy; several gpu partitions with idle capacity of differing CPU quality.
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=c01 CPUTot=48 RealMemory=192000 State=ALLOCATED Partitions=cpu1\n"
                "NodeName=g101 CPUTot=48 RealMemory=384000 State=IDLE Partitions=gpu1 Gres=gpu:rtx3090:4\n"
                "NodeName=g301 CPUTot=56 RealMemory=512000 State=IDLE Partitions=gpu3 Gres=gpu:a10:4\n"
                "NodeName=g501 CPUTot=64 RealMemory=512000 State=MIXED Partitions=gpu5 Gres=gpu:a6000:4\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "c01 cpu1 alloc 48 48 40.0 192000 20000 busy\n"
                "g101 gpu1 idle 0 48 0.1 384000 380000 \n"
                "g301 gpu3 idle 0 56 0.1 512000 500000 \n"
                "g501 gpu5 mix 20 64 10.0 512000 400000 other\n"
            )
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_spread_orders_partitions_by_cpu_score(self) -> None:
        scheduler = Scheduler(
            self.db, self.accounts, 30, client_factory=FakeClient, cpu_pool_partition_spread=True
        )
        shape = scheduler.choose_allocation_shape(resource_pool="cpu", requested_cpus=8)
        self.assertIsNotNone(shape)
        partitions = shape["partition"].split(",")
        self.assertGreater(len(partitions), 1)
        self.assertEqual(shape["node_name"], "")
        # gpu5 (score 300) must come before gpu3 (200) before gpu1 (100).
        self.assertLess(partitions.index("gpu5"), partitions.index("gpu3"))
        self.assertLess(partitions.index("gpu3"), partitions.index("gpu1"))

    def test_spread_disabled_keeps_single_partition_pin(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        shape = scheduler.choose_allocation_shape(resource_pool="cpu", requested_cpus=8)
        self.assertIsNotNone(shape)
        self.assertNotIn(",", shape["partition"])

    def test_started_spread_allocation_records_granted_partition(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5,gpu3,gpu1",
            node_name="",
            total_cpus=40,
            total_memory_mb=131072,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.PENDING.value, slurm_job_id="900")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        allocation = self.db.get_allocation(allocation_id)
        scheduler._apply_allocation_state(
            allocation,
            JobStateInfo(status=JobStatus.RUNNING, node_name="g301", partition="gpu3"),
        )
        updated = self.db.get_allocation(allocation_id)
        self.assertEqual(updated["state"], AllocationStatus.WARM.value)
        self.assertEqual(updated["partition"], "gpu3")
        self.assertEqual(updated["node_name"], "g301")


class PlacementDryRunTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)]
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=0, pending=0, max_running=10, max_pending=10, max_total=10),
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_dry_run_reports_allocation_rejections(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=16384,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            free_cpus=2,
            free_memory_mb=1024,
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        result = scheduler.placement_dry_run(
            {
                "cpus": 4,
                "memory_mb": 8192,
                "gpus": 0,
                "gpu_model": "",
                "partition": "auto",
                "node_name": "",
                "scheduling_profile": "standard",
                "required_capability": "",
                "env_profile": "",
                "account_name": "",
                "exclusive_node": 0,
                "max_workers_per_node": 0,
            }
        )
        self.assertEqual(len(result["accounts"]), 1)
        self.assertTrue(result["accounts"][0]["eligible"])
        candidate = next(item for item in result["allocations"] if item["id"] == allocation_id)
        self.assertEqual(candidate["fit_slots"], 0)
        self.assertTrue(any("free CPUs" in reason for reason in candidate["reasons"]))
        self.assertTrue(any("free memory" in reason for reason in candidate["reasons"]))
        self.assertIn(result["queue_state"], {"ready", "pending", "opening"})


class TransientStateRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_recovers_submitting_job_and_orphaned_attaching_task(self) -> None:
        job_id = self.db.create_job(JobCreate("https://example/repo.git", "main", "run.py"))
        self.db.update_job(job_id, status=JobStatus.SUBMITTING.value)
        orphan_id = self.db.create_task(TaskCreate("orphan", "~/work", "run"))
        self.db.update_task(orphan_id, status=TaskStatus.ATTACHING.value)
        tracked_id = self.db.create_task(TaskCreate("tracked", "~/work", "run"))
        self.db.update_task(
            tracked_id,
            status=TaskStatus.ATTACHING.value,
            exit_code_path="/remote/task/exit_code",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.recover_transient_states()
        self.assertEqual(self.db.get_job(job_id)["status"], JobStatus.QUEUED.value)
        self.assertEqual(self.db.get_task(orphan_id)["status"], TaskStatus.QUEUED.value)
        self.assertEqual(self.db.get_task(tracked_id)["status"], TaskStatus.ATTACHING.value)


if __name__ == "__main__":
    unittest.main()
