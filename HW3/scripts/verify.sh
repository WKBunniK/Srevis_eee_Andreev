#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if docker --context default compose version >/dev/null 2>&1; then
    compose() { docker --context default compose "$@"; }
elif command -v docker-compose >/dev/null 2>&1; then
    compose() { DOCKER_CONTEXT=default docker-compose "$@"; }
else
    echo 'Docker Compose is required.' >&2
    exit 1
fi
stopped_master=""
finish() {
    status=$?
    if [ "$status" -ne 0 ]; then compose logs --tail=100; fi
    if [ -n "$stopped_master" ]; then compose start "$stopped_master"; fi
    exit "$status"
}
trap finish EXIT
compose config --quiet
compose up --build -d --wait --wait-timeout 180
compose exec -T booking python -m tests.integration
compose exec -T booking python -m tests.retry
compose exec -T flight python -m tests.flight_transactions
compose exec -T flight python -m tests.cache
compose exec -T flight python -m common.migrate flight
compose exec -T booking python -m common.migrate booking
master=$(compose exec -T flight python -m tests.failover master)
case "$master" in redis-master|redis-replica) ;; *) exit 1 ;; esac
stopped_master=$master
compose stop "$master"
compose exec -T flight python -m tests.failover promoted "$master"
compose start "$master"
compose exec -T flight python -m tests.failover recovered
stopped_master=""
compose restart redis-master redis-replica
compose exec -T flight python -m tests.failover recovered
compose ps
printf '\nAll integration, cache, retry and failover checks passed.\n'
