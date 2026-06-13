from __future__ import annotations

from dataclasses import dataclass
from math import ceil


@dataclass(frozen=True)
class PestatNode:
    hostname: str
    partition: str
    state: str
    cpu_used: int
    cpu_total: int
    cpu_load: float
    memory_mb: int
    free_memory_mb: int

    @property
    def sched_free_cpus(self) -> int:
        return max(0, self.cpu_total - self.cpu_used)

    @property
    def load_free_cpus(self) -> int:
        return max(0, self.cpu_total - ceil(self.cpu_load))

    @property
    def effective_free_cpus(self) -> int:
        return min(self.sched_free_cpus, self.load_free_cpus)

    @property
    def usable(self) -> bool:
        return self.state in {"idle", "mix"} and self.effective_free_cpus > 0


@dataclass(frozen=True)
class AllocationPlan:
    partition: str
    node_name: str
    workers: int
    initial_workers: int
    cpus_per_worker: int
    total_cpus: int
    simulation_start: int
    simulation_count: int


def _clean_number(raw: str) -> str:
    return raw.rstrip("*")


def parse_pestat(output: str) -> list[PestatNode]:
    nodes = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 8:
            continue
        if parts[0] in {"Hostname", "State"} or parts[0].startswith("-"):
            continue
        try:
            nodes.append(
                PestatNode(
                    hostname=parts[0],
                    partition=parts[1],
                    state=parts[2].rstrip("*"),
                    cpu_used=int(_clean_number(parts[3])),
                    cpu_total=int(_clean_number(parts[4])),
                    cpu_load=float(_clean_number(parts[5])),
                    memory_mb=int(_clean_number(parts[6])),
                    free_memory_mb=int(_clean_number(parts[7])),
                )
            )
        except ValueError:
            continue
    return nodes


def plan_dynamic_allocations(
    nodes: list[PestatNode],
    total_simulations: int,
    cpus_per_simulation: int,
    mem_per_simulation_gb: float,
    max_workers_per_allocation: int,
    max_allocations: int,
    partition: str = "auto",
    start_index: int = 1,
    oversubscribe_factor: float = 1.5,
) -> list[AllocationPlan]:
    remaining = max(0, total_simulations)
    if remaining == 0:
        return []
    mem_per_sim_mb = max(1, int(mem_per_simulation_gb * 1024))
    candidates = []
    for node in nodes:
        if not node.usable:
            continue
        if partition and partition != "auto" and node.partition != partition:
            continue
        baseline_workers = node.effective_free_cpus // max(1, cpus_per_simulation)
        workers_by_cpu = int(baseline_workers * max(1.0, oversubscribe_factor))
        workers_by_mem = node.free_memory_mb // mem_per_sim_mb
        workers = min(workers_by_cpu, workers_by_mem, max_workers_per_allocation)
        if workers > 0 and baseline_workers > 0:
            candidates.append((node, workers, baseline_workers))
    candidates.sort(
        key=lambda item: (
            item[1],
            item[2],
            item[0].effective_free_cpus,
            item[0].free_memory_mb,
        ),
        reverse=True,
    )
    plans = []
    next_sim = start_index
    for node, workers, baseline_workers in candidates[:max_allocations]:
        if remaining <= 0:
            break
        count = min(workers, remaining)
        plans.append(
            AllocationPlan(
                partition=node.partition,
                node_name=node.hostname,
                workers=count,
                initial_workers=min(count, baseline_workers),
                cpus_per_worker=cpus_per_simulation,
                total_cpus=min(count, baseline_workers) * cpus_per_simulation,
                simulation_start=next_sim,
                simulation_count=count,
            )
        )
        next_sim += count
        remaining -= count
    return plans
