"""Check the test machine before a run: its size, and its distance to the storage.

A run measures the storage only while the test machine is not the limit. Two
things make the machine the limit: too few cores or too little memory for the
tier, and a long network path to the storage. Every request waits for the
network at least once, so a test machine far from the storage adds that time
to every response time. AWS figures come from a machine about 1 ms from AWS
S3. This module measures both things, so the report can say when a result
describes the test machine, not the storage.

Network bandwidth is not measured here.
"""

from __future__ import annotations

import os
import shutil
import socket
import statistics
import time
from urllib.parse import urlsplit

from .run_store import physical_memory

GIB = 1024**3
# The system keeps part of the memory for itself: a machine sold with 8 GB
# reports about 7.6 GB. Accept the machine sizes the documentation recommends.
MEMORY_TOLERANCE = 0.9
# Linux reports a large number, not "max", when cgroup v1 sets no limit.
NO_LIMIT = 1 << 60


def _read(path):
    try:
        with open(path, encoding="ascii") as source:
            return source.read().strip()
    except OSError:
        return None


def available_cores():
    """The cores this process may use.

    A container can use fewer cores than the host has. os.cpu_count() shows
    the host, so read the processor affinity and the container's quota too.
    """
    try:
        cores = len(os.sched_getaffinity(0))
    except AttributeError:  # macOS has no processor affinity
        cores = os.cpu_count() or 1
    quota = None
    v2 = _read("/sys/fs/cgroup/cpu.max")
    if v2:
        limit, _, period = v2.partition(" ")
        if limit != "max" and period:
            quota = int(limit) / int(period)
    else:
        limit = _read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        period = _read("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        if limit and period and int(limit) > 0:
            quota = int(limit) / int(period)
    if quota:
        cores = min(cores, max(1, int(quota)))
    return cores


def available_memory():
    """The memory this process may use, in bytes, or None when unknown."""
    memory = physical_memory()
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        value = _read(path)
        if value and value.isdigit() and int(value) < NO_LIMIT:
            memory = min(memory, int(value)) if memory else int(value)
            break
    return memory


def file_limit():
    """The most open files this process can get. The tools raise to this limit."""
    try:
        import resource
    except ImportError:  # not a POSIX platform
        return None
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # macOS reports no hard limit, but refuses more than 10,240 per process.
    return max(soft, 10_240) if hard == resource.RLIM_INFINITY else max(soft, hard)


def round_trip(endpoint, attempts=20, timeout_s=5.0, pause_s=0.05):
    """Time new TCP connections to the endpoint.

    Opening a TCP connection takes one network round trip, and the storage
    software takes no part in it. So the fastest of several connections is
    the network's own share of every request. A new HTTPS connection needs
    two or three round trips before its first request: one for TCP, one or
    two for TLS (Transport Layer Security).
    """
    url = urlsplit(endpoint)
    port = url.port or (443 if url.scheme == "https" else 80)
    result = {"host": url.hostname, "port": port, "attempts": attempts}
    try:
        addresses = socket.getaddrinfo(url.hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror as error:
        result["error"] = f"Cannot resolve {url.hostname}: {error}"
        return result
    times, failures, error = [], 0, None
    for attempt in range(attempts):
        # Until one address answers, try them in turn, as a client would.
        candidates = addresses if not times else [chosen]
        for candidate in candidates:
            family, kind, protocol, _, address = candidate
            connection = socket.socket(family, kind, protocol)
            connection.settimeout(timeout_s)
            begin = time.perf_counter()
            try:
                connection.connect(address)
            except OSError as problem:
                error = f"{address[0]} port {port}: {problem}"
                continue
            else:
                times.append(time.perf_counter() - begin)
                chosen = candidate
                break
            finally:
                connection.close()
        else:
            failures += 1
        time.sleep(pause_s)
    result["failures"] = failures
    if not times:
        result["error"] = f"No connection opened. {error}"
        return result
    result.update(
        address=chosen[4][0],
        min_ms=round(min(times) * 1000, 3),
        median_ms=round(statistics.median(times) * 1000, 3),
        max_ms=round(max(times) * 1000, 3),
    )
    return result


def measure(endpoint, run_dir, files_needed=None):
    """Everything the machine check records. No secret is involved."""
    free = shutil.disk_usage(run_dir).free
    return {
        "cores": available_cores(),
        "memory_bytes": available_memory(),
        "free_disk_bytes": free,
        "file_limit": file_limit(),
        "files_needed": files_needed,
        "round_trip": round_trip(endpoint),
    }


def requirement(config, tier):
    """The smallest machine recommended for a tier, or None for no single machine."""
    return (config.get("test_machine") or {}).get("tiers", {}).get(tier)


def problems(facts, config, tier):
    """What on this machine may limit the run. An empty list means nothing.

    Each problem is (kind, message). The kinds are "size" and "distance".
    """
    found = []
    need = requirement(config, tier)
    limits = config.get("test_machine") or {}
    if limits and need is None:
        found.append(("size", f"No single test machine can send the {tier} workload."))
    if need:
        if facts.get("cores") and facts["cores"] < need["cores"]:
            found.append(
                ("size", f"{facts['cores']} cores, and the {tier} tier needs {need['cores']}.")
            )
        memory = facts.get("memory_bytes")
        if memory and memory < need["memory_gb"] * GIB * MEMORY_TOLERANCE:
            found.append(
                ("size", f"{memory / GIB:.1f} GB of memory, and the {tier} tier needs"
                 f" {need['memory_gb']} GB.")
            )
        disk = facts.get("free_disk_bytes")
        if disk is not None and disk < need["disk_gb"] * GIB:
            found.append(
                ("size", f"{disk / GIB:.1f} GB of free disk, and the {tier} tier needs"
                 f" {need['disk_gb']} GB.")
            )
    files, needed = facts.get("file_limit"), facts.get("files_needed")
    if files and needed and files < needed:
        found.append(
            ("size", f"{files:,} open files at most, and the run needs about {needed:,}.")
        )
    trip = facts.get("round_trip") or {}
    most = limits.get("max_round_trip_ms")
    if most and trip.get("min_ms") is not None and trip["min_ms"] > most:
        found.append(
            ("distance", f"The network round trip to the storage is {trip['min_ms']:.1f} ms,"
             f" and the limit is {most:g} ms.")
        )
    return found


def summary_lines(facts, config, tier):
    """What the machine check prints, one line per measurement."""
    need = requirement(config, tier) or {}

    def wanted(key, unit=""):
        return f" (the {tier} tier needs {need[key]}{unit})" if key in need else ""

    lines = [f"Cores: {facts['cores']}{wanted('cores')}"]
    if facts.get("memory_bytes"):
        lines.append(f"Memory: {facts['memory_bytes'] / GIB:.1f} GB{wanted('memory_gb', ' GB')}")
    lines.append(f"Free disk: {facts['free_disk_bytes'] / GIB:.1f} GB{wanted('disk_gb', ' GB')}")
    if facts.get("file_limit"):
        needed = facts.get("files_needed")
        lines.append(
            f"Open files: {facts['file_limit']:,}"
            + (f" (the run needs about {needed:,})" if needed else "")
        )
    trip = facts["round_trip"]
    most = (config.get("test_machine") or {}).get("max_round_trip_ms")
    if trip.get("min_ms") is not None:
        lines.append(
            f"Network round trip to {trip['host']}: {trip['min_ms']:.1f} ms at best,"
            f" {trip['median_ms']:.1f} ms median"
            + (f" (limit {most:g} ms)" if most else "")
        )
    else:
        lines.append(f"Network round trip: not measured. {trip.get('error', '')}".strip())
    return lines
