#!/usr/bin/env python3
"""Run a first-pass native-tool comparison of Runnel, Kafka, Redpanda, and JetStream.

The default comparison preserves each broker's native benchmark client and
single-node topology. ``--nodes 3`` adds a competitor-only, durable-publish
comparison with three broker nodes and replication factor three. It deliberately
does not include Runnel or a consumer result because those paths do not yet have
matching distributed semantics in this harness.

The implementation is split into private lifecycle, backend, result-policy, and
CLI modules. This file remains the stable executable and import facade for the
comparison harness.
"""

from __future__ import annotations

# Keep the standard-library modules imported by the former monolithic entrypoint
# available to existing tests and local benchmark tooling.
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from common import ROOT, parse_sizes, result_metadata, write_json_result
from compare_adapters import (
    ComparisonError,
    KAFKA_IMAGE,
    NATS_BOX_IMAGE,
    NATS_IMAGE,
    REDPANDA_IMAGE,
    kafka_environment,
    nats_server_command,
    parse_kafka_consume,
    parse_kafka_publish,
    parse_nats_consume,
    parse_nats_publish,
    parse_number,
    redpanda_command,
    require_redpanda_broker_count,
)
from compare_backends import (
    run_kafka_family,
    run_nats,
    run_runnel,
    start_kafka_services,
    start_nats_services,
    start_redpanda_services,
)
from compare_cli import DEFAULT_CPUS, DEFAULT_MEMORY, DEFAULT_MESSAGES, main, parse_args
from compare_lifecycle import (
    COMMAND_TIMEOUT,
    READINESS_COMMAND_TIMEOUT,
    READINESS_TIMEOUT,
    RESOURCE_COMMAND_TIMEOUT,
    Service,
    close_services,
    combine_resource_summaries,
    ensure_image,
    run_measured_tool,
    run_tool,
    start_services,
    wait_for,
)
from compare_results import (
    COMPARISON_MISMATCH_DIMENSIONS,
    DEFAULT_NODES,
    NATIVE_COMPARISON_CLASSIFICATION,
    NATIVE_COMPARISON_REASON,
    SCENARIO_BOUNDARY_FIELDS,
    SCENARIO_COMPARISON_CLASSES,
    THREE_NODE_COUNT,
    annotate_scenario_metadata,
    backend_metadata,
    benchmark_suite,
    comparison_guardrail_metadata,
    record_tool_scenario,
    _require_nonempty_text,
    scenario_comparison_class,
    scenario_operation,
    validate_backend_record,
    validate_comparison_summary,
)
from runtime import DockerContainer, MeasuredContainer, create_network, inspect_image, remove_network


if __name__ == "__main__":
    raise SystemExit(main())
