"""Result shaping and semantic policy for native broker comparisons."""

from __future__ import annotations

from typing import Any, Callable

from compare_adapters import ComparisonError, KAFKA_IMAGE, NATS_BOX_IMAGE


DEFAULT_NODES = 1
THREE_NODE_COUNT = 3
NATIVE_COMPARISON_CLASSIFICATION = "native-tool-baseline"
NATIVE_COMPARISON_REASON = (
    "native clients and operation-specific acknowledgement, durability, replication, "
    "delivery, batching, client, and latency boundaries are not an apples-to-apples "
    "end-to-end ranking"
)
COMPARISON_MISMATCH_DIMENSIONS = (
    "acknowledgement",
    "durability",
    "replication",
    "delivery",
    "batching",
    "client",
    "latency",
)
SCENARIO_BOUNDARY_FIELDS = (
    "acknowledgement_boundary",
    "durability_boundary",
    "replication_topology",
    "delivery_boundary",
    "batching_boundary",
    "client_boundary",
    "latency_boundary",
)
SCENARIO_COMPARISON_CLASSES = {
    "publish": "publish-only",
    "consume_ack": "consume-with-ack",
    "consume": "consume-without-ack",
}


def scenario_operation(scenario: dict[str, Any]) -> str:
    """Read the current operation field while accepting older result artifacts."""
    operation = scenario.get("operation", scenario.get("name"))
    if not isinstance(operation, str):
        raise ComparisonError(f"benchmark scenario has no operation: {scenario!r}")
    return operation


def scenario_comparison_class(operation: Any) -> str:
    """Return the explicit comparison class for a measured operation."""
    if not isinstance(operation, str) or operation not in SCENARIO_COMPARISON_CLASSES:
        supported = ", ".join(sorted(SCENARIO_COMPARISON_CLASSES))
        raise ComparisonError(
            f"unsupported comparison scenario operation {operation!r}; expected one of {supported}"
        )
    return SCENARIO_COMPARISON_CLASSES[operation]


def record_tool_scenario(
    scenarios: list[dict[str, Any]],
    raw: dict[str, str],
    key: str,
    output: str,
    resources: dict[str, Any],
    parser: Callable[[str, int, int], dict[str, Any]],
    size: int,
    messages: int,
) -> None:
    """Add one parsed native-tool result and retain a bounded diagnostic tail."""
    scenario = parser(output, size, messages)
    scenario["resource_samples"] = resources
    scenarios.append(scenario)
    raw[key] = output[-12_000:]


def annotate_scenario_metadata(backend: dict[str, Any]) -> None:
    """Attach normalized comparison semantics to every measured scenario."""
    scenarios = backend.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ComparisonError("backend result must contain at least one scenario")
    semantic = backend.get("semantic_metadata")
    if not isinstance(semantic, dict):
        raise ComparisonError("backend result is missing semantic_metadata")
    scenario_boundaries = semantic.get("scenario_boundaries")
    if not isinstance(scenario_boundaries, dict):
        raise ComparisonError("backend result is missing scenario_boundaries metadata")
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            raise ComparisonError(f"backend scenario {index} is not an object")
        comparison_class = scenario_comparison_class(scenario_operation(scenario))
        boundaries = scenario_boundaries.get(comparison_class)
        if not isinstance(boundaries, dict):
            raise ComparisonError(
                f"backend scenario {index} is missing semantic boundaries for "
                f"{comparison_class!r}"
            )
        existing_metadata = scenario.get("metadata", {})
        if not isinstance(existing_metadata, dict):
            raise ComparisonError(f"backend scenario {index} metadata is not an object")
        existing_class = existing_metadata.get("comparison_class")
        if existing_class is not None and existing_class != comparison_class:
            raise ComparisonError(
                f"backend scenario {index} declares comparison class {existing_class!r}, "
                f"expected {comparison_class!r}"
            )
        existing_boundaries = existing_metadata.get("semantic_boundaries")
        if existing_boundaries is not None and existing_boundaries != boundaries:
            raise ComparisonError(
                f"backend scenario {index} declares semantic boundaries inconsistent "
                f"with {comparison_class!r}"
            )
        scenario["metadata"] = {
            **existing_metadata,
            "comparison_class": comparison_class,
            "semantic_boundaries": dict(boundaries),
        }


def _require_nonempty_text(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ComparisonError(f"comparison metadata is missing a non-empty {description}")
    return value


def validate_backend_record(name: str, backend: dict[str, Any]) -> None:
    """Reject a backend record that cannot be interpreted semantically."""
    if not isinstance(backend, dict):
        raise ComparisonError(f"backend {name!r} is not an object")

    acknowledgement = _require_nonempty_text(
        backend.get("acknowledgement"), f"acknowledgement boundary for {name!r}"
    )
    replication = _require_nonempty_text(
        backend.get("replication"), f"replication/topology for {name!r}"
    )
    measurement_boundary = _require_nonempty_text(
        backend.get("measurement_boundary"), f"measurement boundary for {name!r}"
    )
    measurement_client = _require_nonempty_text(
        backend.get("measurement_client"), f"client identity for {name!r}"
    )

    semantic = backend.get("semantic_metadata")
    if not isinstance(semantic, dict):
        raise ComparisonError(f"backend {name!r} is missing semantic_metadata")
    expected_boundaries = {
        "acknowledgement_boundary": acknowledgement,
        "replication_topology": replication,
        "measurement_boundary": measurement_boundary,
    }
    for field, expected in expected_boundaries.items():
        actual = _require_nonempty_text(semantic.get(field), f"{field} for {name!r}")
        if actual != expected:
            raise ComparisonError(
                f"backend {name!r} has inconsistent {field}: {actual!r} != {expected!r}"
            )

    declared_classes = semantic.get("scenario_classes")
    if (
        not isinstance(declared_classes, list)
        or not declared_classes
        or any(not isinstance(value, str) or not value for value in declared_classes)
        or len(set(declared_classes)) != len(declared_classes)
    ):
        raise ComparisonError(f"backend {name!r} has incomplete scenario_classes metadata")

    scenario_boundaries = semantic.get("scenario_boundaries")
    if not isinstance(scenario_boundaries, dict):
        raise ComparisonError(f"backend {name!r} is missing scenario_boundaries metadata")
    if set(scenario_boundaries) != set(declared_classes):
        raise ComparisonError(
            f"backend {name!r} scenario boundaries do not match declared scenario classes"
        )
    for comparison_class in declared_classes:
        boundaries = scenario_boundaries.get(comparison_class)
        if not isinstance(boundaries, dict):
            raise ComparisonError(
                f"backend {name!r} has no semantic boundaries for {comparison_class!r}"
            )
        for field in SCENARIO_BOUNDARY_FIELDS:
            boundary = _require_nonempty_text(
                boundaries.get(field),
                f"{field} for {name!r} {comparison_class!r}",
            )
            if field == "replication_topology" and boundary != replication:
                raise ComparisonError(
                    f"backend {name!r} {comparison_class!r} has inconsistent replication topology"
                )

    client_identity = semantic.get("client_identity")
    if not isinstance(client_identity, dict):
        raise ComparisonError(f"backend {name!r} is missing a client_identity object")
    client_name = _require_nonempty_text(
        client_identity.get("name"), f"client identity name for {name!r}"
    )
    client_image = _require_nonempty_text(
        client_identity.get("image"), f"client identity image for {name!r}"
    )
    if client_name != measurement_client:
        raise ComparisonError(
            f"backend {name!r} has inconsistent client identity: "
            f"{client_name!r} != {measurement_client!r}"
        )
    declared_client_image = _require_nonempty_text(
        backend.get("client_image"), f"client image for {name!r}"
    )
    if client_image != declared_client_image:
        raise ComparisonError(f"backend {name!r} has inconsistent client image metadata")

    comparison = semantic.get("comparison")
    if not isinstance(comparison, dict):
        raise ComparisonError(f"backend {name!r} is missing comparison metadata")
    if comparison.get("classification") != NATIVE_COMPARISON_CLASSIFICATION:
        raise ComparisonError(f"backend {name!r} has an unknown comparison classification")
    if comparison.get("apples_to_apples") is not False:
        raise ComparisonError(f"backend {name!r} must be marked non-equivalent")
    if comparison.get("ranking_eligible") is not False:
        raise ComparisonError(f"backend {name!r} must not be ranking eligible")
    if comparison.get("experimental") is not True:
        raise ComparisonError(f"backend {name!r} must be marked experimental")
    mismatch_dimensions = comparison.get("mismatch_dimensions")
    if mismatch_dimensions != list(COMPARISON_MISMATCH_DIMENSIONS):
        raise ComparisonError(
            f"backend {name!r} has incomplete comparison mismatch dimensions"
        )

    scenarios = backend.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ComparisonError(f"backend {name!r} must contain at least one scenario")
    observed_classes: set[str] = set()
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            raise ComparisonError(f"backend {name!r} scenario {index} is not an object")
        expected_class = scenario_comparison_class(scenario_operation(scenario))
        metadata = scenario.get("metadata")
        if not isinstance(metadata, dict) or metadata.get("comparison_class") != expected_class:
            raise ComparisonError(
                f"backend {name!r} scenario {index} is missing comparison class "
                f"{expected_class!r}"
            )
        if metadata.get("semantic_boundaries") != scenario_boundaries[expected_class]:
            raise ComparisonError(
                f"backend {name!r} scenario {index} is missing semantic metadata for "
                f"{expected_class!r}"
            )
        observed_classes.add(expected_class)
    if observed_classes != set(declared_classes):
        raise ComparisonError(
            f"backend {name!r} declares scenario classes {declared_classes!r}, "
            f"observed {sorted(observed_classes)!r}"
        )


def comparison_guardrail_metadata(nodes: int) -> dict[str, Any]:
    """Describe why this native comparison must not be treated as a ranking."""
    return {
        "classification": NATIVE_COMPARISON_CLASSIFICATION,
        "apples_to_apples": False,
        "ranking_eligible": False,
        "experimental": True,
        "mismatch_dimensions": list(COMPARISON_MISMATCH_DIMENSIONS),
        "scenario_scope": "publish-only" if nodes == THREE_NODE_COUNT else "publish and consume",
        "reason": NATIVE_COMPARISON_REASON,
    }


def validate_comparison_summary(summary: dict[str, Any]) -> None:
    """Validate the machine-readable guardrail on a complete raw result."""
    guardrail = summary.get("comparison_guardrail")
    if not isinstance(guardrail, dict):
        raise ComparisonError("comparison result is missing comparison_guardrail metadata")
    workload = summary.get("workload")
    nodes = workload.get("nodes") if isinstance(workload, dict) else None
    if not isinstance(nodes, int) or isinstance(nodes, bool) or nodes not in {
        DEFAULT_NODES,
        THREE_NODE_COUNT,
    }:
        raise ComparisonError("comparison result is missing a valid workload node count")
    expected_guardrail = comparison_guardrail_metadata(nodes)
    if any(guardrail.get(key) != value for key, value in expected_guardrail.items()):
        raise ComparisonError("comparison result has incomplete or inconsistent guardrail metadata")
    backends = summary.get("backends")
    if not isinstance(backends, dict) or not backends:
        raise ComparisonError("comparison result must contain backend records")
    for name, backend in backends.items():
        validate_backend_record(str(name), backend)


def benchmark_suite(nodes: int, backends: list[str]) -> str:
    """Identify the history series represented by a comparison workload."""
    if nodes == THREE_NODE_COUNT:
        return "cluster-comparison"
    if backends == ["runnel"]:
        return "runnel"
    return "native-comparison"


def backend_metadata(name: str, nodes: int) -> dict[str, Any]:
    """Declare operation-specific client, durability, and comparison boundaries."""
    if name == "runnel":
        acknowledgement = (
            "request response after the current local durable append; consume acknowledgement "
            "persists a consumer checkpoint"
        )
        replication = "single local broker engine"
        measurement_boundary = "Runnel's current line-delimited JSON protocol"
        client_image = "host Python runtime"
        client_name = "host Python socket client"
        scenario_classes = ["publish-only", "consume-with-ack"]
        scenario_boundaries = {
            "publish-only": {
                "acknowledgement_boundary": "publish response after the local durable append",
                "durability_boundary": (
                    "current local broker durable-append default; no replica quorum"
                ),
                "replication_topology": replication,
                "delivery_boundary": "not applicable to publish-only",
                "batching_boundary": (
                    "one in-flight publish request per message; concurrency=1; no client batching"
                ),
                "client_boundary": "host Python socket client using the host Python runtime",
                "latency_boundary": (
                    "per-message publish request send-to-response; p50/p99/p99.9/max are recorded"
                ),
            },
            "consume-with-ack": {
                "acknowledgement_boundary": (
                    "ack response after the local consumer checkpoint is persisted"
                ),
                "durability_boundary": "consumer checkpoint persistence on the single local broker",
                "replication_topology": replication,
                "delivery_boundary": (
                    "one poll followed by one acknowledgement per message; at-least-once delivery"
                ),
                "batching_boundary": (
                    "one poll-and-ack sequence per message; concurrency=1; no client batching"
                ),
                "client_boundary": "host Python socket client using the host Python runtime",
                "latency_boundary": (
                    "per-message poll-and-ack sequence; p50/p99/p99.9/max are recorded"
                ),
            },
        }
    elif name in {"kafka", "redpanda"}:
        client_image = KAFKA_IMAGE
        client_name = "Kafka producer/consumer performance clients"
        scenario_classes = ["publish-only", "consume-without-ack"]
        if nodes == THREE_NODE_COUNT:
            publish_acknowledgement = (
                "Kafka producer performance client with acks=all and idempotence enabled; "
                "topic min.insync.replicas=3"
            )
            acknowledgement = publish_acknowledgement
            replication = (
                "three broker nodes, one partition, replication factor three, "
                "min.insync.replicas three"
            )
            measurement_boundary = (
                "Kafka native producer performance client over the Kafka protocol; "
                "durable publish only"
            )
            client_name = "Kafka producer performance client"
            scenario_classes = ["publish-only"]
            publish_durability = (
                "one-partition broker log with replication factor three and min.insync.replicas=3; "
                "filesystem flush behavior is not asserted"
            )
        else:
            publish_acknowledgement = "Kafka producer performance client with acks=all"
            acknowledgement = (
                f"{publish_acknowledgement}; consumer perf client measures fetch throughput "
                "without per-record application acknowledgement"
            )
            replication = "single broker, one partition, replication factor one"
            measurement_boundary = (
                "Kafka producer/consumer performance clients over the Kafka protocol"
            )
            publish_durability = (
                "one-partition broker log with replication factor one and acks=all; "
                "filesystem flush behavior is not asserted"
            )
        scenario_boundaries = {
            "publish-only": {
                "acknowledgement_boundary": publish_acknowledgement,
                "durability_boundary": publish_durability,
                "replication_topology": replication,
                "delivery_boundary": "not applicable to publish-only",
                "batching_boundary": (
                    "Kafka producer perf client uses linger.ms=0 and the default batch.size; "
                    "native client batching remains possible"
                ),
                "client_boundary": f"{client_name} from {client_image}",
                "latency_boundary": (
                    "native Kafka producer latency includes client-side batching and reports "
                    "avg/p50/p95/p99/p99.9/max"
                ),
            },
        }
        if nodes == 1:
            scenario_boundaries["consume-without-ack"] = {
                "acknowledgement_boundary": (
                    "native consumer perf fetch throughput; no per-record application "
                    "acknowledgement"
                ),
                "durability_boundary": (
                    "reads the single broker's log with replication factor one; consumer fetch is "
                    "not a durability acknowledgement"
                ),
                "replication_topology": replication,
                "delivery_boundary": (
                    "native consumer performance client fetches records through a consumer group; "
                    "no application-level delivery acknowledgement"
                ),
                "batching_boundary": (
                    "native consumer fetch batching; application batch size and per-message "
                    "processing are not measured"
                ),
                "client_boundary": f"Kafka consumer performance client from {client_image}",
                "latency_boundary": (
                    "per-record consumer latency is unavailable; output reports elapsed "
                    "fetch throughput"
                ),
            }
    elif nodes == THREE_NODE_COUNT:
        acknowledgement = (
            "JetStream synchronous publish PubAck to a file-backed stream configured with "
            "three replicas"
        )
        replication = "three NATS servers, file storage, three stream replicas"
        measurement_boundary = "nats bench js native synchronous publisher; durable publish only"
        client_image = NATS_BOX_IMAGE
        client_name = "nats bench js native synchronous publisher"
        scenario_classes = ["publish-only"]
        scenario_boundaries = {
            "publish-only": {
                "acknowledgement_boundary": (
                    "synchronous JetStream PubAck after publishing to the three-replica stream"
                ),
                "durability_boundary": (
                    "file-backed stream with three replicas; synchronous PubAck; exact filesystem "
                    "flush behavior is not asserted"
                ),
                "replication_topology": replication,
                "delivery_boundary": "not applicable to publish-only",
                "batching_boundary": (
                    "nats bench js pub sync publishes one message at a time; no explicit "
                    "client batch"
                ),
                "client_boundary": (
                    f"nats bench js native synchronous publisher from {client_image}"
                ),
                "latency_boundary": (
                    "native synchronous publisher stats measure publish acknowledgement latency "
                    "and report min/avg/p50/p90/p99/p99.9/max"
                ),
            },
        }
    else:
        acknowledgement = (
            "JetStream synchronous publish PubAck; durable consumer explicit acknowledgement "
            "with synchronous double acknowledgement"
        )
        replication = "single NATS server, file storage, one replica"
        measurement_boundary = "nats bench js native benchmark client"
        client_image = NATS_BOX_IMAGE
        client_name = "nats bench js native benchmark client"
        scenario_classes = ["publish-only", "consume-with-ack"]
        scenario_boundaries = {
            "publish-only": {
                "acknowledgement_boundary": (
                    "synchronous JetStream PubAck after publishing to the file-backed stream"
                ),
                "durability_boundary": (
                    "file-backed stream with one replica; synchronous PubAck; exact filesystem "
                    "flush behavior is not asserted"
                ),
                "replication_topology": replication,
                "delivery_boundary": "not applicable to publish-only",
                "batching_boundary": (
                    "nats bench js pub sync publishes one message at a time; no explicit "
                    "client batch"
                ),
                "client_boundary": (
                    f"nats bench js native synchronous publisher from {client_image}"
                ),
                "latency_boundary": (
                    "native synchronous publisher stats measure publish acknowledgement latency "
                    "and report min/avg/p50/p90/p99/p99.9/max"
                ),
            },
            "consume-with-ack": {
                "acknowledgement_boundary": (
                    "explicit consumer acknowledgement with synchronous double acknowledgement"
                ),
                "durability_boundary": (
                    "file-backed consumer on a one-replica stream; the double acknowledgement "
                    "confirms the consumer acknowledgement"
                ),
                "replication_topology": replication,
                "delivery_boundary": (
                    "pull consumer with deliver=all and replay=instant; one explicit "
                    "acknowledgement per message"
                ),
                "batching_boundary": (
                    "nats bench js consume uses --batch=1 with explicit acknowledgement and "
                    "double acknowledgement"
                ),
                "client_boundary": f"nats bench js native consumer from {client_image}",
                "latency_boundary": (
                    "per-message consumer latency is unavailable; output reports acknowledged "
                    "consume throughput"
                ),
            },
        }

    return {
        "runtime": "container",
        "acknowledgement": acknowledgement,
        "replication": replication,
        "measurement_boundary": measurement_boundary,
        "measurement_client": client_name,
        "client_image": client_image,
        "semantic_metadata": {
            "acknowledgement_boundary": acknowledgement,
            "replication_topology": replication,
            "measurement_boundary": measurement_boundary,
            "client_identity": {"name": client_name, "image": client_image},
            "scenario_classes": scenario_classes,
            "scenario_boundaries": scenario_boundaries,
            "comparison": {
                "classification": NATIVE_COMPARISON_CLASSIFICATION,
                "apples_to_apples": False,
                "ranking_eligible": False,
                "experimental": True,
                "mismatch_dimensions": list(COMPARISON_MISMATCH_DIMENSIONS),
            },
        },
    }
