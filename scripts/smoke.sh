#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

data_dir=$(mktemp -d /tmp/runnel-smoke.XXXXXX)
log_file="$data_dir/server.log"
server_pid=
broker_port=
http_port=
broker_addr=

cleanup() {
    if [ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    if [ -d "$data_dir" ]; then
        rm -rf "$data_dir"
    fi
}

stop_server() {
    if [ -n "$server_pid" ]; then
        kill -TERM "$server_pid"
        wait "$server_pid"
        server_pid=
    fi
}

allocate_ports() {
    read -r broker_port http_port < <(python3 - <<'PY'
import socket

ports = []
for _ in range(2):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        ports.append(str(sock.getsockname()[1]))
print(" ".join(ports))
PY
)
    broker_addr="127.0.0.1:$broker_port"
}

start_server() {
    target/debug/runnel \
        --data-dir "$data_dir" \
        --listen "$broker_addr" \
        --http-listen "127.0.0.1:$http_port" \
        >"$log_file" 2>&1 &
    server_pid=$!

    for _ in $(seq 1 100); do
        if curl -fsS "http://127.0.0.1:$http_port/health/ready" >/dev/null; then
            return
        fi
        sleep 0.05
    done

    cat "$log_file" >&2
    printf '%s\n' 'broker did not become ready' >&2
    exit 1
}

assert_contains() {
    local output=$1
    local pattern=$2
    printf '%s' "$output" | grep -Eq "$pattern"
}

trap cleanup EXIT INT TERM

allocate_ports
cargo build -q --locked -p runnel-server -p runnel-cli

start_server

"$PWD/target/debug/runnelctl" --server "$broker_addr" create-stream events
"$PWD/target/debug/runnelctl" --server "$broker_addr" publish events hello
output=$("$PWD/target/debug/runnelctl" --server "$broker_addr" consume events worker)
assert_contains "$output" '"offset": 0'
"$PWD/target/debug/runnelctl" --server "$broker_addr" ack events worker 0
output=$("$PWD/target/debug/runnelctl" --server "$broker_addr" consume events worker)
assert_contains "$output" '"type": "empty"'

"$PWD/target/debug/runnelctl" --server "$broker_addr" publish events recover-me
"$PWD/target/debug/runnelctl" --server "$broker_addr" consume events recovery-worker
"$PWD/target/debug/runnelctl" --server "$broker_addr" ack events recovery-worker 0
output=$("$PWD/target/debug/runnelctl" --server "$broker_addr" consume events recovery-worker)
assert_contains "$output" '"offset": 1'

stop_server
start_server
output=$("$PWD/target/debug/runnelctl" --server "$broker_addr" consume events recovery-worker)
assert_contains "$output" '"offset": 1'
"$PWD/target/debug/runnelctl" --server "$broker_addr" ack events recovery-worker 1
curl -fsS "http://127.0.0.1:$http_port/health/ready" >/dev/null
curl -fsS "http://127.0.0.1:$http_port/metrics" | grep -Eq 'runnel_streams 1'

printf '%s\n' 'Runnel smoke test passed'
