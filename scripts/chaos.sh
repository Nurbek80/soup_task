#!/usr/bin/env bash
set -uo pipefail

mkdir -p artifacts/chaos
run_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_file="artifacts/chaos/run-${run_stamp}.log"

echo "raw log: ${log_file}"
docker compose up -d --build --scale worker="${WORKERS:-8}" 2>&1 | tee "${log_file}"
compose_status=${PIPESTATUS[0]}
if [ "${compose_status}" != "0" ]; then
  echo "compose startup failed" | tee -a "${log_file}"
  exit 1
fi

GOCACHE="${PWD}/.cache/go-build" GOPATH="${PWD}/.cache/go" \
  go run ./cmd/chaos \
    -jobs="${JOBS:-10000}" \
    -workers="${WORKERS:-8}" \
    -rate="${RATE:-100}" \
    -duration-ms="${DURATION_MS:-100}" \
    -kill-interval="${KILL_INTERVAL:-2s}" \
    -timeout="${TIMEOUT:-10m}" 2>&1 | tee -a "${log_file}"
result=${PIPESTATUS[0]}
echo "exit_code=${result}" | tee -a "${log_file}"
exit "${result}"
