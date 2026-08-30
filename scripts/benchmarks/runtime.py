"""Small Docker lifecycle primitives shared by benchmark runners."""

from __future__ import annotations

import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from common import BenchmarkError
from resources import StatsSampler


def inspect_image(image: str) -> str | None:
    result = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image],
        capture_output=True,
        text=True,
        check=False,
    )
    image_id = result.stdout.strip()
    return image_id if result.returncode == 0 and image_id else None


def create_network(name: str) -> None:
    subprocess.run(
        ["docker", "network", "create", "--label", "runnel.benchmark=true", name],
        check=True,
        capture_output=True,
        text=True,
    )


def remove_network(name: str) -> None:
    for _ in range(3):
        result = subprocess.run(
            ["docker", "network", "rm", name],
            check=False,
            capture_output=True,
        )
        if result.returncode == 0:
            return
        time.sleep(0.2)

    inspect = subprocess.run(
        [
            "docker",
            "network",
            "inspect",
            name,
            "--format",
            "{{range $id, $container := .Containers}}{{$id}} {{end}}",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    for container in inspect.stdout.split():
        subprocess.run(
            ["docker", "network", "disconnect", "--force", name, container],
            check=False,
            capture_output=True,
        )
    subprocess.run(["docker", "network", "rm", name], check=False, capture_output=True)


@dataclass
class DockerContainer:
    name: str
    image: str
    network: str
    cpus: str
    memory: str
    data_dir: Path
    data_target: str
    command: list[str] = field(default_factory=list)
    environment: dict[str, str] = field(default_factory=dict)
    entrypoint: str | None = None
    published_ports: tuple[int, ...] = ()
    image_id: str | None = None
    startup_ns: int | None = None
    created: bool = False

    def start(self, *, image_id: str | None = None) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.chmod(0o777)
        self.image_id = image_id or inspect_image(self.image)
        if self.image_id is None:
            raise BenchmarkError(f"Docker image does not exist: {self.image}")

        command = self.run_command()
        started = time.perf_counter_ns()
        subprocess.run(command, check=True, capture_output=True, text=True)
        self.startup_ns = time.perf_counter_ns() - started
        self.created = True

    def restart(self) -> int:
        started = time.perf_counter_ns()
        subprocess.run(["docker", "restart", self.name], check=True, capture_output=True)
        return time.perf_counter_ns() - started

    def stop(self) -> None:
        if not self.created:
            return
        subprocess.run(
            ["docker", "rm", "--force", self.name],
            check=False,
            capture_output=True,
        )
        self.created = False

    def close(self) -> str:
        logs = self.logs()
        self.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)
        return logs

    def logs(self) -> str:
        if not self.created:
            return ""
        result = subprocess.run(
            ["docker", "logs", self.name],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout + result.stderr

    def published_port(self, container_port: int) -> int:
        result = subprocess.run(
            ["docker", "port", self.name, f"{container_port}/tcp"],
            check=True,
            capture_output=True,
            text=True,
        )
        value = result.stdout.rsplit(":", 1)[-1].strip()
        try:
            return int(value)
        except ValueError as error:
            raise BenchmarkError(
                f"could not parse published port for {container_port}: {result.stdout!r}"
            ) from error

    def run_command(self) -> list[str]:
        command = [
            "docker",
            "run",
            "--detach",
            "--name",
            self.name,
            "--label",
            "runnel.benchmark=true",
            "--network",
            self.network,
            "--cpus",
            self.cpus,
            "--memory",
            self.memory,
        ]
        for port in self.published_ports:
            command.extend(["--publish", f"127.0.0.1::{port}"])
        command.extend(["--volume", f"{self.data_dir}:{self.data_target}"])
        if self.entrypoint:
            command.extend(["--entrypoint", self.entrypoint])
        for key, value in self.environment.items():
            command.extend(["--env", f"{key}={value}"])
        command.extend([self.image, *self.command])
        return command


class MeasuredContainer:
    """Add scenario resource sampling to one Docker container."""

    def __init__(
        self,
        container: DockerContainer,
        *,
        probe_timeout_seconds: float = 2.0,
    ) -> None:
        self.container = container
        self.stats = StatsSampler(
            container.name,
            probe_timeout_seconds=probe_timeout_seconds,
        )
        self.startup_ns: int | None = None

    @property
    def image_id(self) -> str | None:
        return self.container.image_id

    @property
    def name(self) -> str:
        return self.container.name

    def start(self, *, image_id: str | None = None) -> None:
        started = time.perf_counter_ns()
        self.container.start(image_id=image_id)
        self.startup_ns = time.perf_counter_ns() - started
        self.stats.start()

    def restart(self) -> int:
        return self.container.restart()

    def logs(self) -> str:
        return self.container.logs()

    def published_port(self, container_port: int) -> int:
        return self.container.published_port(container_port)

    def close(self) -> str:
        self.stats.close()
        return self.container.close()
