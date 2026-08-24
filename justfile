set shell := ["bash", "-euo", "pipefail", "-c"]

default: verify

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

test:
    RUST_TEST_THREADS=1 cargo test --locked --workspace --all-targets --all-features

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
    RUST_TEST_THREADS=1 cargo test --locked -p runnel-server --test cluster_smoke -- --nocapture

bench:
    cargo bench --locked --workspace

bench-container:
    python3 scripts/benchmarks/run.py --build

bench-container-smoke:
    python3 scripts/benchmarks/run.py --image runnel:dev --messages 20 --warmup 2 --concurrency 2 --payload-sizes 100

bench-cluster:
    python3 scripts/benchmarks/cluster.py --build

bench-cluster-smoke:
    python3 scripts/benchmarks/cluster.py --build --messages 20 --warmup 2 --payload-sizes 100 --skip-recovery

profile-cluster:
    python3 scripts/benchmarks/profile.py --build

profile-cluster-instrumented:
    RUST_LOG=runnel::timing=trace python3 scripts/benchmarks/profile.py --build --features instrumentation --skip-perf

bench-compare:
    python3 scripts/benchmarks/compare.py --build-runnel

bench-compare-cluster:
    python3 scripts/benchmarks/compare.py --nodes 3 --backends kafka,redpanda,nats --messages 1000 --payload-sizes 100,1024 --cpus 2 --memory 2g --client-cpus 1 --client-memory 512m

bench-dashboard:
    python3 scripts/benchmarks/build_history.py --runs benchmark-results --output benchmark-results/site

bench-test:
    python3 -m unittest discover --start-directory scripts/benchmarks --pattern 'test_*.py'

audit:
    command -v cargo-audit >/dev/null || { echo "cargo-audit is required; install it with: cargo install --locked cargo-audit" >&2; exit 1; }
    cargo audit

docker-build:
    docker build --tag runnel:dev .

ci: verify docker-build
    python3 scripts/isolated.py smoke
    python3 scripts/isolated.py bench-container-smoke
