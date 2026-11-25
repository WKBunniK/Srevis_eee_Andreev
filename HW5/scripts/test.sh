#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."
export COMPOSE_PROJECT_NAME="hw5-test-$$-${RANDOM}"
export PRODUCER_PORT=0 AGGREGATION_PORT=0
export GENERATOR_ENABLED=false AGGREGATION_INTERVAL_SECONDS=3

cleanup() {
    result=$?
    trap - EXIT
    if [ "$result" -ne 0 ]; then
        docker compose logs --no-color --tail=80
    fi
    docker compose down --volumes --remove-orphans
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker compose --progress plain build bootstrap
docker compose up -d --wait --wait-timeout 240
docker compose run --rm --no-deps tests
