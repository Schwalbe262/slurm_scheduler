from __future__ import annotations

import re
from dataclasses import dataclass


GPU_PRIORITY = {
    "a6000ada": 400,
    "a6000": 300,
    "rtx3090": 200,
    "a10": 100,
}

CPU_PROFILES_BY_PARTITION = {
    "cpu1": {
        "cpu_model": "Intel(R) Xeon(R) Gold 6240R CPU @ 2.40GHz",
        "sockets": 2,
        "cores_per_socket": 24,
        "threads_per_core": 1,
        "cpu_score": 100,
    },
    "cpu2": {
        "cpu_model": "AMD EPYC 9755 128-Core Processor",
        "sockets": 2,
        "cores_per_socket": 128,
        "threads_per_core": 1,
        "cpu_score": 400,
    },
    "gpu1": {
        "cpu_model": "Intel(R) Xeon(R) Gold 6240R CPU @ 2.40GHz",
        "sockets": 2,
        "cores_per_socket": 24,
        "threads_per_core": 1,
        "cpu_score": 100,
    },
    "gpu2": {
        "cpu_model": "Intel(R) Xeon(R) Gold 6348 CPU @ 2.60GHz",
        "sockets": 2,
        "cores_per_socket": 28,
        "threads_per_core": 1,
        "cpu_score": 200,
    },
    "gpu3": {
        "cpu_model": "Intel(R) Xeon(R) Gold 6348 CPU @ 2.60GHz",
        "sockets": 2,
        "cores_per_socket": 28,
        "threads_per_core": 1,
        "cpu_score": 200,
    },
    "gpu5": {
        "cpu_model": "Intel(R) Xeon(R) Platinum 8358 CPU @ 2.60GHz",
        "sockets": 2,
        "cores_per_socket": 32,
        "threads_per_core": 1,
        "cpu_score": 300,
    },
    "gpu6": {
        "cpu_model": "Intel(R) Xeon(R) Gold 6240R CPU @ 2.40GHz",
        "sockets": 2,
        "cores_per_socket": 24,
        "threads_per_core": 1,
        "cpu_score": 100,
    },
}


@dataclass(frozen=True)
class NodeInventory:
    node_name: str
    partition: str
    cpus: int
    memory_mb: int
    gpu_model: str
    gpu_count: int
    state: str
    cpu_model: str = ""
    sockets: int = 0
    cores_per_socket: int = 0
    threads_per_core: int = 0
    cpu_score: int = 0


def parse_gres(gres: str) -> tuple[str, int]:
    if not gres or gres == "(null)":
        return "", 0
    match = re.search(r"gpu:([^:]+):(\d+)", gres)
    if not match:
        return "", 0
    return match.group(1), int(match.group(2))


def parse_sinfo_nodes(output: str) -> list[NodeInventory]:
    nodes = []
    for line in output.splitlines():
        parts = line.strip().split("|")
        if len(parts) != 6:
            continue
        node_name, partition, cpus, memory_mb, gres, state = parts
        gpu_model, gpu_count = parse_gres(gres)
        profile = CPU_PROFILES_BY_PARTITION.get(partition.rstrip("*"), {})
        nodes.append(
            NodeInventory(
                node_name=node_name,
                partition=partition.rstrip("*"),
                cpus=int(cpus),
                memory_mb=int(memory_mb),
                gpu_model=gpu_model,
                gpu_count=gpu_count,
                state=state.rstrip("*").lower(),
                cpu_model=str(profile.get("cpu_model", "")),
                sockets=int(profile.get("sockets", 0)),
                cores_per_socket=int(profile.get("cores_per_socket", 0)),
                threads_per_core=int(profile.get("threads_per_core", 0)),
                cpu_score=int(profile.get("cpu_score", 0)),
            )
        )
    return nodes


def partition_rank(rows: list[dict], needs_gpu: bool) -> list[dict]:
    summaries: dict[str, dict] = {}
    for row in rows:
        partition = row["partition"]
        summary = summaries.setdefault(
            partition,
            {
                "partition": partition,
                "nodes": 0,
                "available_nodes": 0,
                "max_cpus": 0,
                "max_memory_mb": 0,
                "gpu_model": "",
                "gpu_count_per_node": 0,
                "gpu_score": 0,
                "cpu_model": "",
                "cpu_score": 0,
                "sockets": 0,
                "cores_per_socket": 0,
                "threads_per_core": 0,
            },
        )
        summary["nodes"] += 1
        if row["state"] in {"idle", "mix", "mixed"}:
            summary["available_nodes"] += 1
        summary["max_cpus"] = max(summary["max_cpus"], int(row["cpus"]))
        summary["max_memory_mb"] = max(summary["max_memory_mb"], int(row["memory_mb"]))
        model = row.get("gpu_model") or ""
        gpu_score = GPU_PRIORITY.get(model, 0)
        if gpu_score > summary["gpu_score"]:
            summary["gpu_model"] = model
            summary["gpu_count_per_node"] = int(row.get("gpu_count") or 0)
            summary["gpu_score"] = gpu_score
        cpu_score = int(row.get("cpu_score") or 0)
        if cpu_score > summary["cpu_score"]:
            summary["cpu_score"] = cpu_score
            summary["cpu_model"] = row.get("cpu_model") or ""
            summary["sockets"] = int(row.get("sockets") or 0)
            summary["cores_per_socket"] = int(row.get("cores_per_socket") or 0)
            summary["threads_per_core"] = int(row.get("threads_per_core") or 0)
    candidates = list(summaries.values())
    if needs_gpu:
        candidates = [item for item in candidates if item["gpu_count_per_node"] > 0]
        return sorted(
            candidates,
            key=lambda item: (
                item["gpu_score"],
                item["gpu_count_per_node"],
                item["available_nodes"],
                item["max_cpus"],
                item["max_memory_mb"],
            ),
            reverse=True,
        )
    candidates = [item for item in candidates if item["gpu_count_per_node"] == 0]
    return sorted(
        candidates,
        key=lambda item: (item["cpu_score"], item["max_cpus"], item["max_memory_mb"], item["available_nodes"]),
        reverse=True,
    )
