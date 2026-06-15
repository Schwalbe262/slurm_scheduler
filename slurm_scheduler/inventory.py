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
    gpu_used_count: int
    state: str
    cpu_model: str = ""
    sockets: int = 0
    cores_per_socket: int = 0
    threads_per_core: int = 0
    cpu_score: int = 0


def normalize_gpu_model(model: str) -> str:
    raw = re.sub(r"[^a-z0-9]+", "", (model or "").strip().lower())
    if not raw:
        return ""
    if "6000" in raw and "ada" in raw:
        return "a6000ada"
    if "a6000" in raw or raw in {"rtxa6000", "nvidiaa6000"}:
        return "a6000"
    if "3090" in raw:
        return "rtx3090"
    if raw in {"a10", "rtxa10", "nvidiaa10"}:
        return "a10"
    return raw


def gpu_model_candidates(models: str) -> list[str]:
    candidates = []
    for part in re.split(r"[\s,;/|]+", models or ""):
        model = normalize_gpu_model(part)
        if model and model not in candidates:
            candidates.append(model)
    if not candidates:
        model = normalize_gpu_model(models or "")
        if model:
            candidates.append(model)
    return candidates


def parse_gres(gres: str) -> tuple[str, int]:
    if not gres or gres == "(null)":
        return "", 0
    best_model = ""
    total = 0
    for match in re.finditer(r"gpu(?::([^:,\(]+))?:(\d+)", gres):
        model = normalize_gpu_model(match.group(1) or "")
        count = int(match.group(2))
        total += count
        if GPU_PRIORITY.get(model, 0) > GPU_PRIORITY.get(best_model, 0):
            best_model = model
        elif not best_model:
            best_model = model
    return best_model, total


def parse_gres_used(gres_used: str, default_model: str = "") -> int:
    if not gres_used or gres_used == "(null)":
        return 0
    used = 0
    normalized_default = normalize_gpu_model(default_model)
    for match in re.finditer(r"gpu(?::([^:,\(]+))?:(\d+)", gres_used):
        model = normalize_gpu_model(match.group(1) or normalized_default)
        if normalized_default and model and model != normalized_default:
            continue
        used += int(match.group(2))
    return used


def parse_alloc_tres_gpus(alloc_tres: str, default_model: str = "") -> int:
    if not alloc_tres:
        return 0
    normalized_default = normalize_gpu_model(default_model)
    generic = 0
    specific = 0
    for item in alloc_tres.split(","):
        if "=" not in item:
            continue
        key, raw_value = item.split("=", 1)
        try:
            value = int(float(raw_value))
        except ValueError:
            continue
        if key == "gres/gpu":
            generic += value
            continue
        if key.startswith("gres/gpu:"):
            model = normalize_gpu_model(key.split(":", 1)[1])
            if not normalized_default or model == normalized_default:
                specific += value
    return specific or generic


def parse_sinfo_nodes(output: str) -> list[NodeInventory]:
    nodes = []
    for line in output.splitlines():
        parts = line.strip().split("|")
        if len(parts) not in {6, 7}:
            continue
        node_name, partition, cpus, memory_mb, gres, state = parts[:6]
        gres_used = parts[6] if len(parts) == 7 else ""
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
                gpu_used_count=parse_gres_used(gres_used, gpu_model),
                state=state.rstrip("*").lower(),
                cpu_model=str(profile.get("cpu_model", "")),
                sockets=int(profile.get("sockets", 0)),
                cores_per_socket=int(profile.get("cores_per_socket", 0)),
                threads_per_core=int(profile.get("threads_per_core", 0)),
                cpu_score=int(profile.get("cpu_score", 0)),
            )
        )
    return nodes


def parse_scontrol_nodes(output: str) -> list[NodeInventory]:
    nodes = []
    for raw in output.splitlines():
        fields = dict(re.findall(r"([A-Za-z][A-Za-z0-9_]*)=([^ ]*)", raw.strip()))
        if not fields.get("NodeName"):
            continue
        partitions = fields.get("Partitions") or fields.get("PartitionName") or ""
        partition = partitions.split(",")[0].rstrip("*")
        gpu_model, gpu_count = parse_gres(fields.get("Gres", ""))
        profile = CPU_PROFILES_BY_PARTITION.get(partition, {})
        nodes.append(
            NodeInventory(
                node_name=fields["NodeName"],
                partition=partition,
                cpus=int(fields.get("CPUTot") or fields.get("CPUs") or 0),
                memory_mb=int(fields.get("RealMemory") or fields.get("Memory") or 0),
                gpu_model=gpu_model,
                gpu_count=gpu_count,
                gpu_used_count=parse_gres_used(fields.get("GresUsed", ""), gpu_model)
                or parse_alloc_tres_gpus(fields.get("AllocTRES", ""), gpu_model),
                state=(fields.get("State") or "").split("+")[0].lower(),
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
                "total_gpus": 0,
                "used_gpus": 0,
                "free_gpus": 0,
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
        gpu_count = int(row.get("gpu_count") or 0)
        gpu_used = min(gpu_count, max(0, int(row.get("gpu_used_count") or 0)))
        summary["total_gpus"] += gpu_count
        summary["used_gpus"] += gpu_used
        if row["state"] in {"idle", "mix", "mixed"}:
            summary["free_gpus"] += max(0, gpu_count - gpu_used)
        gpu_score = GPU_PRIORITY.get(model, 0)
        if gpu_score > summary["gpu_score"]:
            summary["gpu_model"] = model
            summary["gpu_count_per_node"] = gpu_count
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
