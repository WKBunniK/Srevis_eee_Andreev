#!/bin/sh
set -eu
config=/data/redis.conf
if [ ! -s "$config" ]; then
    cat > "$config" <<CONF
port 6379
bind 0.0.0.0
protected-mode no
dir /data
appendonly yes
replica-announce-ip ${REDIS_HOSTNAME}
CONF
    if [ -n "${REDIS_INITIAL_MASTER:-}" ]; then
        printf 'replicaof %s 6379\n' "$REDIS_INITIAL_MASTER" >> "$config"
    fi
fi
exec redis-server "$config"
