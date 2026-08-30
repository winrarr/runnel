"""The small public surface of the clean-slate benchmark prototype.

The benchmark owns workload definition, measurement, and result shape. A
backend owns process/container lifecycle and protocol details.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol


@dataclass(frozen=True)
class Workload:
    """One repeatable workload shared by every backend."""

    messages: int
    payload_sizes: tuple[int, ...] = (100, 1024)
    warmup: int = 0
    concurrency: int = 1
    batch_size: int = 1
    slow_consumer_delay_ms: int = 0

    def __post_init__(self) -> None:
        if self.messages <= 0:
            raise ValueError("messages must be positive")
        if self.warmup < 0:
            raise ValueError("warmup must be non-negative")
        if self.concurrency <= 0:
            raise ValueError("concurrency must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.slow_consumer_delay_ms < 0:
            raise ValueError("slow_consumer_delay_ms must be non-negative")
        if not self.payload_sizes or any(size <= 0 for size in self.payload_sizes):
            raise ValueError("payload_sizes must contain positive values")


@dataclass(frozen=True)
class Limits:
    """Optional limits applied by a runtime adapter."""

    cpus: str | None = None
    memory: str | None = None


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int


@dataclass(frozen=True)
class ActionResult:
    """The only value a scenario must produce."""

    latencies_ns: tuple[int, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Measurement:
    elapsed_ns: int
    action: ActionResult
    resources: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScenarioResult:
    operation: str
    payload_size: int
    measurement: Measurement


@dataclass(frozen=True)
class RunResult:
    suite: str
    backend: str
    workload: Workload
    scenarios: tuple[ScenarioResult, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)


class Client(Protocol):
    def request(self, operation: str, **arguments: Any) -> Mapping[str, Any]: ...

    def close(self) -> None: ...


ClientFactory = Callable[[], Client]
Scenario = Callable[["Runtime", ClientFactory, Workload, bytes], ActionResult]
ResourceSample = Callable[[], Mapping[str, Any]]


class Runtime(Protocol):
    """Lifecycle and resource boundary shared by native and container runs."""

    endpoint: Endpoint | None

    def start(self) -> Endpoint: ...

    def restart(self) -> int: ...

    def stop(self) -> None: ...

    def sample(self) -> Mapping[str, Any]: ...


class Backend(Protocol):
    """Adapter boundary for Runnel, competitors, or another protocol later."""

    name: str

    def runtime(self, limits: Limits, nodes: int) -> Runtime: ...

    def client_factory(self, runtime: Runtime) -> ClientFactory: ...

    def scenarios(self) -> Mapping[str, Scenario]: ...
