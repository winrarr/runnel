"""Shared Docker lifecycle and resource measurement for native comparisons."""

from __future__ import annotations

import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from compare_adapters import ComparisonError
from runtime import DockerContainer, MeasuredContainer, inspect_image


COMMAND_TIMEOUT = 180
READINESS_TIMEOUT = 45
READINESS_COMMAND_TIMEOUT = 10
# Docker's stats endpoint commonly takes just over one second even for a
# healthy local container. Keep probes bounded while allowing useful samples.
RESOURCE_COMMAND_TIMEOUT = 2


def ensure_image(image: str) -> str:
    """Return a local image ID, pulling the pinned image when necessary."""
    image_id = inspect_image(image)
    if image_id is not None:
        return image_id

    try:
        subprocess.run(["docker", "pull", image], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as error:
        detail = f"{error.stdout or ''}{error.stderr or ''}"
        raise ComparisonError(f"could not pull benchmark image {image}:\n{detail}") from error

    image_id = inspect_image(image)
    if image_id is None:
        raise ComparisonError(f"Docker pulled benchmark image {image} but it could not be inspected")
    return image_id


class Service:
    """Own one measured broker container and its run-scoped data directory."""

    def __init__(
        self,
        *,
        name: str,
        image: str,
        network: str,
        cpus: str,
        memory: str,
        data_target: str,
        command: list[str] | None = None,
        environment: dict[str, str] | None = None,
        entrypoint: str | None = None,
    ) -> None:
        self.name = name
        self.image = image
        self.network = network
        self.cpus = cpus
        self.memory = memory
        self.container = MeasuredContainer(
            DockerContainer(
                name=name,
                image=image,
                network=network,
                cpus=cpus,
                memory=memory,
                data_dir=Path(tempfile.mkdtemp(prefix=f"{name}-")),
                data_target=data_target,
                command=command or [],
                environment=environment or {},
                entrypoint=entrypoint,
            ),
            probe_timeout_seconds=RESOURCE_COMMAND_TIMEOUT,
        )
        self.stats = self.container.stats

    @property
    def image_id(self) -> str | None:
        return self.container.image_id

    @property
    def startup_ns(self) -> int | None:
        return self.container.startup_ns

    def start(self) -> None:
        image_id = ensure_image(self.image)
        try:
            self.container.start(image_id=image_id)
        except subprocess.CalledProcessError as error:
            raise ComparisonError(
                f"failed to start {self.name}: {error}\n{self.container.logs()}"
            ) from error

    def close(self) -> dict[str, Any]:
        logs = self.container.close()
        summary = self.stats.summary()
        return {
            "image": self.image,
            "image_id": self.image_id,
            "cpu_limit": self.cpus,
            "memory_limit": self.memory,
            "startup_seconds": (self.startup_ns or 0) / 1_000_000_000,
            "resource_samples": summary,
            "log_tail": logs[-4000:],
        }


def combine_resource_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    """Return totals for a cluster while retaining per-node measurements."""
    if len(summaries) == 1:
        return summaries[0]

    combined: dict[str, Any] = {
        "nodes": summaries,
        "samples": min(summary.get("samples", 0) for summary in summaries),
    }
    for key in ("cpu_seconds", "memory_bytes_avg", "memory_bytes_max"):
        values = [summary.get(key) for summary in summaries]
        if all(isinstance(value, (int, float)) for value in values):
            combined[key] = sum(values)
    elapsed = [summary.get("elapsed_seconds") for summary in summaries]
    if all(isinstance(value, (int, float)) for value in elapsed):
        combined["elapsed_seconds"] = max(elapsed)
    return combined


def start_services(
    services: list[Service], ready: Callable[[], None], description: str
) -> list[Service]:
    """Start all services and close every created service if readiness fails."""
    try:
        for service in services:
            service.start()
        wait_for(ready, description)
    except BaseException:
        for service in services:
            service.close()
        raise
    return services


def run_tool(
    image: str,
    network: str,
    arguments: list[str],
    *,
    cpus: str,
    memory: str,
    timeout: int = COMMAND_TIMEOUT,
) -> str:
    """Run a bounded native benchmark or setup command in the network."""
    command = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "--cpus",
        cpus,
        "--memory",
        memory,
        image,
        *arguments,
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        if isinstance(error, subprocess.CalledProcessError):
            detail = f"{error.stdout or ''}{error.stderr or ''}"
        else:
            detail = "command timed out"
        raise ComparisonError(f"benchmark tool failed: {' '.join(command)}\n{detail}") from error
    return result.stdout + result.stderr


def run_measured_tool(
    services: Service | list[Service],
    image: str,
    network: str,
    arguments: list[str],
    *,
    cpus: str,
    memory: str,
    timeout: int = COMMAND_TIMEOUT,
) -> tuple[str, dict[str, Any]]:
    """Run a client command while sampling every participating broker."""
    service_list = services if isinstance(services, list) else [services]
    tokens = [service.stats.begin() for service in service_list]
    try:
        output = run_tool(
            image,
            network,
            arguments,
            cpus=cpus,
            memory=memory,
            timeout=timeout,
        )
    except BaseException:
        for service, token in zip(service_list, tokens):
            service.stats.end(token)
        raise
    resources = [service.stats.end(token) for service, token in zip(service_list, tokens)]
    return output, combine_resource_summaries(resources)


def wait_for(check: Callable[[], None], description: str) -> None:
    """Poll readiness with the comparison harness's bounded timeout."""
    deadline = time.monotonic() + READINESS_TIMEOUT
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            check()
            return
        except (ComparisonError, subprocess.CalledProcessError, OSError) as error:
            last_error = error
        time.sleep(0.5)
    raise ComparisonError(f"{description} did not become ready: {last_error}")


def close_services(services: list[Service]) -> dict[str, Any]:
    """Close services and aggregate their bounded resource summaries."""
    summaries = [service.close() for service in services]
    if len(summaries) == 1:
        return summaries[0]
    return {
        "image": summaries[0]["image"],
        "image_id": summaries[0]["image_id"],
        "image_ids": [summary["image_id"] for summary in summaries],
        "cpu_limit": summaries[0]["cpu_limit"],
        "memory_limit": summaries[0]["memory_limit"],
        "startup_seconds": max(summary["startup_seconds"] for summary in summaries),
        "resource_samples": combine_resource_summaries(
            [summary["resource_samples"] for summary in summaries]
        ),
        "nodes": summaries,
    }
