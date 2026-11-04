#!/bin/sh
set -eu
config=/data/sentinel.conf
if [ ! -s "$config" ]; then
    cat > "$config" <<CONF
port 26379
bind 0.0.0.0
protected-mode no
dir /data
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip ${SENTINEL_HOSTNAME}
sentinel monitor flights redis-master 6379 2
sentinel down-after-milliseconds flights 3000
sentinel failover-timeout flights 10000
sentinel parallel-syncs flights 1
CONF
fi
exec redis-server "$config" --sentinel
