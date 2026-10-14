#!/usr/bin/env bash
# Start a single-node PD + TiKV cluster in Docker and wait until it serves.
#
#   ci/start-cluster.sh [version]      # e.g. v8.5.8 (default)
#   ci/start-cluster.sh stop
#
# PD listens on 127.0.0.1:2379 and TiKV on 127.0.0.1:20160 (host networking).
set -euo pipefail

VERSION="${1:-v8.5.8}"
HERE="$(cd "$(dirname "$0")" && pwd)"

if [[ "$VERSION" == "stop" ]]; then
  docker rm -f tikv-ci-pd tikv-ci-tikv >/dev/null 2>&1 || true
  exit 0
fi

docker rm -f tikv-ci-pd tikv-ci-tikv >/dev/null 2>&1 || true

docker run -d --name tikv-ci-pd --network host "pingcap/pd:${VERSION}" \
  --name=pd \
  --data-dir=/data/pd \
  --client-urls=http://127.0.0.1:2379 \
  --peer-urls=http://127.0.0.1:2380 \
  --advertise-client-urls=http://127.0.0.1:2379 \
  --advertise-peer-urls=http://127.0.0.1:2380 >/dev/null

# TiKV refuses to start with fewer than ~82k open file descriptors.
docker run -d --name tikv-ci-tikv --network host --ulimit nofile=1048576:1048576 \
  -v "$HERE/tikv.toml:/etc/tikv.toml:ro" "pingcap/tikv:${VERSION}" \
  --addr=127.0.0.1:20160 \
  --advertise-addr=127.0.0.1:20160 \
  --status-addr=127.0.0.1:20180 \
  --data-dir=/data/tikv \
  --pd=127.0.0.1:2379 \
  --config=/etc/tikv.toml >/dev/null

echo "waiting for TiKV ${VERSION} to come up..."
for _ in $(seq 1 120); do
  if curl -fs http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null | grep -q '"state_name": "Up"'; then
    echo "cluster is up"
    exit 0
  fi
  sleep 1
done

echo "cluster did not come up in time" >&2
docker logs --tail 50 tikv-ci-pd >&2 || true
docker logs --tail 50 tikv-ci-tikv >&2 || true
exit 1
