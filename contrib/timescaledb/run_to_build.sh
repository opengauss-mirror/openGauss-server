#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)

find_pg_config()
{
    if [ -n "${PG_CONFIG:-}" ] && [ -x "${PG_CONFIG}" ]; then
        echo "${PG_CONFIG}"
        return
    fi

    if [ -x "${ROOT_DIR}/mppdb_temp_install/bin/pg_config" ]; then
        echo "${ROOT_DIR}/mppdb_temp_install/bin/pg_config"
        return
    fi

    if [ -n "${GAUSSHOME:-}" ] && [ -x "${GAUSSHOME}/bin/pg_config" ]; then
        echo "${GAUSSHOME}/bin/pg_config"
        return
    fi

    if command -v pg_config >/dev/null 2>&1; then
        command -v pg_config
        return
    fi

    echo "could not find pg_config" >&2
    exit 1
}

PG_CONFIG=$(find_pg_config)
PG_BINDIR=$(cd "$(dirname "${PG_CONFIG}")" && pwd)

export PG_CONFIG
export GAUSSHOME=$(cd "${PG_BINDIR}/.." && pwd)
export PATH="${PG_BINDIR}:${PATH}"

cd "${ROOT_DIR}"

"${SCRIPT_DIR}/bootstrap" \
    -DPG_CONFIG="${PG_CONFIG}" \
    -DUSE_OPENSSL=0 \
    -DREGRESS_CHECKS=OFF \
    -DENABLE_BBOX=ON \
    -DENABLE_MOT=ON \
    -DENABLE_HTAP=ON \
    --prefix=contrib/timescaledb
