#!/usr/bin/env bash
# Build redis 7.2.7 from source into /mnt/projects/jtian15/redis (no root needed).
# Usage: run on mcgill-rr-storage as jtian15.
set -euo pipefail

REDIS_VERSION=7.2.7
PREFIX=/mnt/projects/jtian15/redis

mkdir -p "$PREFIX" /mnt/projects/jtian15/redis-data
cd /mnt/projects/jtian15/redis
curl -fsSLO "https://download.redis.io/releases/redis-${REDIS_VERSION}.tar.gz"
tar xzf "redis-${REDIS_VERSION}.tar.gz"
cd "redis-${REDIS_VERSION}"
make -j4
make PREFIX="$PREFIX" install

"$PREFIX/bin/redis-server" --version
