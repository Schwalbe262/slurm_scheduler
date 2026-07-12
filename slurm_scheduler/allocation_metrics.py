from __future__ import annotations


def annotate_allocation_node_metrics(allocations: list[dict], pestat_rows: list[dict]) -> list[dict]:
    pestat_by_node = {str(row.get("hostname") or ""): row for row in pestat_rows}
    for allocation in allocations:
        node = pestat_by_node.get(str(allocation.get("node_name") or ""))
        if not node:
            allocation.update(
                {
                    "node_pestat_state": "",
                    "node_cpu_used": None,
                    "node_cpu_total": None,
                    "node_cpu_load": None,
                    "node_cpu_load_percent": None,
                    "node_cpu_busy_percent": None,
                    "node_memory_used_mb": None,
                    "node_memory_free_mb": None,
                    "node_memory_total_mb": None,
                    "node_memory_used_gb": None,
                    "node_memory_total_gb": None,
                    "node_memory_used_percent": None,
                    "node_metrics_observed_at": "",
                }
            )
            continue
        cpu_total = int(node.get("cpu_total") or 0)
        cpu_load = float(node.get("cpu_load") or 0.0)
        memory_total_mb = int(node.get("memory_mb") or 0)
        memory_free_mb = int(node.get("free_memory_mb") or 0)
        memory_used_mb = max(0, memory_total_mb - memory_free_mb)
        allocation.update(
            {
                "node_pestat_state": node.get("state") or "",
                "node_cpu_used": int(node.get("cpu_used") or 0),
                "node_cpu_total": cpu_total,
                "node_cpu_load": round(cpu_load, 2),
                "node_cpu_load_percent": round((cpu_load / cpu_total) * 100.0, 1) if cpu_total > 0 else None,
                # Familiar 0-100% utilization view: loadavg approximates busy
                # cores while load <= cores; anything above is queueing, which
                # utilization-wise is still a fully busy node.
                "node_cpu_busy_percent": round(min(cpu_load, cpu_total) / cpu_total * 100.0, 1)
                if cpu_total > 0
                else None,
                "node_memory_used_mb": memory_used_mb,
                "node_memory_free_mb": memory_free_mb,
                "node_memory_total_mb": memory_total_mb,
                "node_memory_used_gb": round(memory_used_mb / 1024),
                "node_memory_total_gb": round(memory_total_mb / 1024),
                "node_memory_used_percent": round((memory_used_mb / memory_total_mb) * 100.0, 1)
                if memory_total_mb > 0
                else None,
                "node_metrics_observed_at": node.get("observed_at") or "",
            }
        )
    return allocations


def annotate_allocation_fea_pressure(allocations: list[dict], pressures: dict[int, dict[str, int]]) -> list[dict]:
    # pressures are keyed per allocation id (not per node): each Slurm allocation
    # reserves its own cores, so its FEA pressure is independent of co-tenant
    # allocations on the same physical node.
    for allocation in allocations:
        alloc_id = int(allocation.get("id") or 0)
        pressure = pressures.get(alloc_id) if alloc_id else None
        if not pressure:
            allocation.update(
                {
                    "node_fea_requested_cpus": None,
                    "node_fea_owned_cpus": None,
                    "node_fea_cpu_percent": None,
                }
            )
            continue
        requested_cpus = int(pressure.get("requested_cpus") or 0)
        owned_cpus = int(pressure.get("owned_cpus") or 0)
        allocation.update(
            {
                "node_fea_requested_cpus": requested_cpus,
                "node_fea_owned_cpus": owned_cpus,
                "node_fea_cpu_percent": round((requested_cpus / owned_cpus) * 100.0, 1)
                if owned_cpus > 0
                else None,
            }
        )
    return allocations
