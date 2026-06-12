#!/bin/sh
# Assign unique BATC port per replica (shared flock volume).
# Advertise CUPIDMQ_ADVERTISE_HOST:PORT; bind 0.0.0.0:PORT locally.
set -eu

ADVERTISE_HOST="${CUPIDMQ_ADVERTISE_HOST:?set CUPIDMQ_ADVERTISE_HOST — host IP reachable by producers (not Docker DNS)}"
BASE="${CUPIDMQ_DATA_PORT_BASE:-9760}"
LOCKDIR="${CUPIDMQ_ALLOC_DIR:-/var/lib/cupidmq}"
mkdir -p "$LOCKDIR"

exec 9>"${LOCKDIR}/alloc.lock"
flock -x 9
n=0
if [ -f "${LOCKDIR}/next" ]; then
    read -r n < "${LOCKDIR}/next"
fi
PORT=$((BASE + n))
echo $((n + 1)) > "${LOCKDIR}/next"
flock -u 9

export CUPIDMQ_DATA_ADDR="${ADVERTISE_HOST}:${PORT}"
export CUPIDMQ_BIND_ADDR="0.0.0.0:${PORT}"
TAG_PREFIX="${CUPIDMQ_CONSUMER_TAG_PREFIX:-consumer}"
export CUPIDMQ_CONSUMER_TAG="${CUPIDMQ_CONSUMER_TAG:-${TAG_PREFIX}-${PORT}}"
CONSUMER_BIN="${CUPIDMQ_CONSUMER_BIN:-/usr/local/bin/docker-consumer}"

echo "entrypoint-consumer: advertise=${CUPIDMQ_DATA_ADDR} bind=${CUPIDMQ_BIND_ADDR} tag=${CUPIDMQ_CONSUMER_TAG} bin=${CONSUMER_BIN}" >&2
exec "${CONSUMER_BIN}"
