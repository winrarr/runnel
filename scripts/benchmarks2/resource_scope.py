#!/usr/bin/env python3
"""Run a native benchmark inside a bounded Linux systemd user scope."""

from __future__ import annotations

import re
import shutil


class ResourceScopeError(RuntimeError):
    """The local environment cannot provide the requested benchmark scope."""


_UNIT_COMPONENT = re.compile(r"^[A-Za-z0-9:_.@-]+$")


def _cpu_quota(cpus: str) -> str:
    try:
        value = float(cpus)
    except ValueError as error:
        raise ResourceScopeError(f"CPU limit must be a positive number, got {cpus!r}") from error
    if value <= 0:
        raise ResourceScopeError(f"CPU limit must be positive, got {cpus!r}")
    return f"{value * 100:g}%"


def _memory_limit(memory: str) -> str:
    value = memory.strip()
    if not value or value.startswith("-") or any(character.isspace() for character in value):
        raise ResourceScopeError("memory limit must be a non-empty systemd size such as 2g")
    # systemd's binary size suffixes are case-sensitive; accepting the
    # conventional CLI spelling keeps `2g` and `512m` pleasant to use.
    if value[-1].lower() in "kmgtpe":
        return value[:-1] + value[-1].upper()
    return value


def resource_scope_command(
    command: list[str],
    *,
    unit: str,
    cpus: str,
    memory: str,
) -> list[str]:
    """Return *command* wrapped in a user scope with explicit limits.

    The scope covers the benchmark client and all broker processes it starts.
    This keeps the current and baseline runs on the same resource budget while
    preserving the native process and network behavior of the clustered test.
    """
    if not command:
        raise ResourceScopeError("benchmark command cannot be empty")
    if not unit or not _UNIT_COMPONENT.fullmatch(unit):
        raise ResourceScopeError(f"invalid systemd scope unit name: {unit!r}")
    if shutil.which("systemd-run") is None:
        raise ResourceScopeError(
            "systemd-run is required for bounded local benchmarks; use a Linux system with a user systemd session"
        )
    return [
        "systemd-run",
        "--user",
        "--scope",
        "--collect",
        f"--unit={unit}",
        f"--property=CPUQuota={_cpu_quota(cpus)}",
        f"--property=MemoryMax={_memory_limit(memory)}",
        "--",
        *command,
    ]


def resource_limits(*, cpus: str, memory: str) -> dict[str, str]:
    """Return the provenance recorded in a bounded benchmark result."""
    _cpu_quota(cpus)
    normalized_memory = _memory_limit(memory)
    return {
        "processes": "systemd user scope; benchmark client and broker nodes",
        "cpu": cpus,
        "memory": normalized_memory,
    }
