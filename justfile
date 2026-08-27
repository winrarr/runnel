set shell := ["bash", "-euo", "pipefail", "-c"]

benchmark_lock := "/tmp/runnel-benchmark.lock"

default: verify

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

test:
    # Test-only recovery experiments are run explicitly by cluster-replacement-test.
    RUST_TEST_THREADS=1 cargo test --locked --workspace --all-targets

doc-test:
    cargo test --locked --workspace --doc

shellcheck:
    shellcheck scripts/*.sh

build:
    cargo build --locked --workspace

release:
    cargo build --locked --workspace --release

verify: fmt-check lint test doc-test shellcheck bench-test build

run:
    cargo run -p runnel-server -- --data-dir ./data

smoke:
    ./scripts/smoke.sh

isolated workflow="test":
    python3 scripts/isolated.py {{workflow}}

cluster-test:
    cargo test --locked -p runnel-server --test cluster_smoke -- --nocapture --test-threads=1

cluster-replacement-test:
    cargo test --locked -p runnel-server --features test-replacement-recovery --test cluster_smoke -- --nocapture --test-threads=1

bench:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- cargo bench --locked --workspace

bench-container:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- python3 scripts/benchmarks/run.py --build

bench-container-smoke:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode shared -- python3 scripts/benchmarks/run.py --image runnel:dev --messages 20 --warmup 2 --concurrency 2 --payload-sizes 100

bench-cluster:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- python3 scripts/benchmarks/cluster.py --build

bench-cluster-smoke:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode shared -- python3 scripts/benchmarks/cluster.py --build --messages 20 --warmup 2 --payload-sizes 100 --skip-recovery

bench-pr-local *args:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- python3 scripts/benchmarks/pr_local.py {{args}}

bench-pr-local-until-stable *args:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- python3 scripts/benchmarks/pr_local_until_stable.py {{args}}

bench-pr-local-quick *args:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode shared -- python3 scripts/benchmarks/pr_local.py --repetitions 1 --allow-inconclusive {{args}}

profile-cluster:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- python3 scripts/benchmarks/profile.py --build

profile-cluster-instrumented:
    RUST_LOG=runnel::timing=trace python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- python3 scripts/benchmarks/profile.py --build --features instrumentation --skip-perf

bench-compare:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- python3 scripts/benchmarks/compare.py --build-runnel

bench-compare-cluster:
    python3 scripts/benchmarks/lock.py --path {{benchmark_lock}} --mode exclusive -- python3 scripts/benchmarks/compare.py --nodes 3 --backends kafka,redpanda,nats --messages 1000 --payload-sizes 100,1024 --cpus 2 --memory 2g --client-cpus 1 --client-memory 512m

bench-dashboard:
    python3 scripts/benchmarks/build_history.py --runs benchmark-results --output benchmark-results/site

bench-test:
    python3 -m unittest discover --start-directory scripts/benchmarks --pattern 'test_*.py'

audit:
    command -v cargo-audit >/dev/null || { echo "cargo-audit is required; install it with: cargo install --locked cargo-audit" >&2; exit 1; }
    cargo audit

docker-build:
    docker build --tag runnel:dev .

integration:
    RUNNEL_TEST_CAPTURE_LOGS=1 python3 scripts/isolated.py smoke
    RUNNEL_TEST_CAPTURE_LOGS=1 python3 scripts/isolated.py cluster-test
    if [ "${RUNNEL_INTEGRATION_IMAGE_READY:-0}" != "1" ]; then just docker-build; fi
    RUNNEL_TEST_CAPTURE_LOGS=1 python3 scripts/isolated.py bench-container-smoke

ci: verify integration
