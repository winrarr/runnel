set shell := ["bash", "-euo", "pipefail", "-c"]

default: verify

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

test:
    cargo test --locked --workspace --all-targets --all-features

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

cluster-test:
    cargo test --locked -p runnel-server --test cluster_smoke -- --nocapture

bench:
    cargo bench --locked --workspace

bench-container:
    python3 scripts/benchmarks/run.py --build

bench-container-smoke:
    python3 scripts/benchmarks/run.py --image runnel:dev --messages 20 --warmup 2 --concurrency 2 --payload-sizes 100

bench-compare:
    python3 scripts/benchmarks/compare.py --build-runnel

bench-dashboard:
    python3 scripts/benchmarks/build_history.py --runs benchmark-results --output benchmark-results/site

bench-test:
    python3 -m unittest discover --start-directory scripts/benchmarks --pattern 'test_*.py'

audit:
    command -v cargo-audit >/dev/null || { echo "cargo-audit is required; install it with: cargo install --locked cargo-audit" >&2; exit 1; }
    cargo audit

docker-build:
    docker build --tag runnel:dev .

ci: verify smoke docker-build bench-container-smoke
