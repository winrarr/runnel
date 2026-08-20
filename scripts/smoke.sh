#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

target_dir=${CARGO_TARGET_DIR:-$PWD/target}
data_dir=$(mktemp -d "${TMPDIR:-/tmp}/runnel-smoke.XXXXXX")
log_file="$data_dir/server.log"
server_pid=
broker_port=
http_port=
broker_addr=
server_binary="$target_dir/debug/runnel"
cli_binary="$target_dir/debug/runnelctl"

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
    "$server_binary" \
        --data-dir "$data_dir" \
        --listen "$broker_addr" \
        --http-listen "127.0.0.1:$http_port" \
        --ack-timeout-ms 50 \
        --max-delivery-attempts 2 \
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

json_field() {
    local field=$1
    python3 -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$field"
}

trap cleanup EXIT INT TERM

allocate_ports
cargo build -q --locked -p runnel-server -p runnel-cli

start_server

"$cli_binary" --server "$broker_addr" create-stream events
"$cli_binary" --server "$broker_addr" publish events hello
output=$("$cli_binary" --server "$broker_addr" consume events worker)
assert_contains "$output" '"offset": 0'
"$cli_binary" --server "$broker_addr" ack events worker 0
output=$("$cli_binary" --server "$broker_addr" consume events worker)
assert_contains "$output" '"type": "empty"'

"$cli_binary" --server "$broker_addr" publish events recover-me
"$cli_binary" --server "$broker_addr" consume events recovery-worker
"$cli_binary" --server "$broker_addr" ack events recovery-worker 0
output=$("$cli_binary" --server "$broker_addr" consume events recovery-worker)
assert_contains "$output" '"offset": 1'

"$cli_binary" --server "$broker_addr" create-stream jobs
"$cli_binary" --server "$broker_addr" publish jobs group-first
"$cli_binary" --server "$broker_addr" publish jobs group-second
group_a_output=$("$cli_binary" --server "$broker_addr" consume jobs workers --member worker-a)
group_b_output=$("$cli_binary" --server "$broker_addr" consume jobs workers --member worker-b)
group_a_offset=$(printf '%s' "$group_a_output" | json_field offset)
group_a_token=$(printf '%s' "$group_a_output" | json_field delivery_token)
group_b_offset=$(printf '%s' "$group_b_output" | json_field offset)
group_b_token=$(printf '%s' "$group_b_output" | json_field delivery_token)
if [ "$group_a_offset" = "$group_b_offset" ]; then
    printf '%s\n' 'group members received the same record' >&2
    exit 1
fi
"$cli_binary" --server "$broker_addr" ack jobs workers "$group_a_offset" \
    --member worker-a --delivery-token "$group_a_token"
"$cli_binary" --server "$broker_addr" ack jobs workers "$group_b_offset" \
    --member worker-b --delivery-token "$group_b_token"

"$cli_binary" --server "$broker_addr" publish poison poison
output=$("$cli_binary" --server "$broker_addr" consume poison poison-worker)
assert_contains "$output" '"delivery_attempt": 1'
sleep 0.08
output=$("$cli_binary" --server "$broker_addr" consume poison poison-worker)
assert_contains "$output" '"delivery_attempt": 2'
sleep 0.08
output=$("$cli_binary" --server "$broker_addr" consume poison poison-worker)
assert_contains "$output" '"type": "empty"'
output=$("$cli_binary" --server "$broker_addr" consume poison.dead-letter poison-inspector)
assert_contains "$output" '"payload": "poison"'
"$cli_binary" --server "$broker_addr" ack poison.dead-letter poison-inspector 0
curl -fsS "http://127.0.0.1:$http_port/metrics" | grep -Eq 'runnel_dead_letters_total 1'

stop_server
start_server
output=$("$cli_binary" --server "$broker_addr" consume events recovery-worker)
assert_contains "$output" '"offset": 1'
"$cli_binary" --server "$broker_addr" ack events recovery-worker 1
curl -fsS "http://127.0.0.1:$http_port/health/ready" >/dev/null
curl -fsS "http://127.0.0.1:$http_port/metrics" | grep -Eq 'runnel_streams 4'

printf '%s\n' 'Runnel smoke test passed'
