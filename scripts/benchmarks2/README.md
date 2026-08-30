# Benchmark 2

This is a clean-slate prototype of the benchmark framework. It keeps the
benchmark model small:

```text
backend -> runtime + client factory + scenarios
workload + scenarios -> measurement -> JSON result
repeated JSON results -> aggregate or compare
```

The public signatures are in `api.py`. Shared execution and result behavior is
in `core.py`; `runnel.py` contains only the current protocol and one runtime
adapter. A future binary protocol, native process runtime, cluster runtime, or
competitor can implement the same backend boundary.

The implemented slice includes workload validation, durable publish,
concurrent publish, consume/acknowledge, round-trip, restart recovery,
bounded Docker lifecycle, resource snapshots, JSON results, repeated-run
aggregation, and paired metric comparison. Native process, three-node,
competitor, history/dashboard, profiling, and isolation adapters are still
missing; their common seam is represented by the backend/runtime interfaces.

Run the prototype with:

```text
python3 -m scripts.benchmarks2.run --help
```

It is deliberately not wired into `just`, CI, history, or the dashboard yet.
The existing framework remains the authoritative benchmark implementation while
this prototype is evaluated for feature parity and size.
