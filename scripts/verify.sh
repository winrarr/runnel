#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v just >/dev/null 2>&1; then
    printf '%s\n' 'just is required; install it with: cargo install --locked just' >&2
    exit 1
fi

exec just verify
