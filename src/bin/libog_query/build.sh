#!/bin/bash

mode="B"
for i in "$@"
do
case $i in
    --mode=*)
    mode="${i#*=}"
    shift
    ;;
    *)
    ;;
esac
done

if [ "$mode" != "A" ] && [ "$mode" != "B" ]; then
  echo "Error: Invalid value for --mode. Allowed values are 'A' or 'B'."
  exit 1
fi

# Preextracted sources need ENABLE_GSS/USE_SSL headers from binarylibs.
EXTRA_INC=""
_3rd_root="${THIRD_BIN_PATH:-${binarylib_dir:-}}"
if [ -n "${_3rd_root}" ] && [ -d "${_3rd_root}" ]; then
    _dep="${_3rd_root}/kernel/dependency"
    for _d in kerberos/comm kerberos/llt; do
        if [ -f "${_dep}/${_d}/include/gssapi/gssapi.h" ] || [ -f "${_dep}/${_d}/include/gssapi.h" ]; then
            EXTRA_INC="${EXTRA_INC} -I${_dep}/${_d}/include"
            break
        fi
    done
    for _d in openssl/comm openssl/llt tassl/comm tassl/llt; do
        if [ -d "${_dep}/${_d}/include/openssl" ]; then
            EXTRA_INC="${EXTRA_INC} -I${_dep}/${_d}/include"
            break
        fi
    done
fi

# Prefer the active compiler's libstdc++ so GAUSSHOME/lib (older) does not break cc1plus.
_saved_ld="${LD_LIBRARY_PATH}"
_comp=$(echo "${CXX:-${CC:-}}" | sed 's/^ccache[[:space:]]*//' | awk '{print $NF}')
if [ -n "${_comp}" ] && [ -x "${_comp}" ]; then
    _lib64=$(cd "$(dirname "${_comp}")/../lib64" 2>/dev/null && pwd)
    if [ -n "${_lib64}" ] && [ -d "${_lib64}" ]; then
        export LD_LIBRARY_PATH="${_lib64}:${LD_LIBRARY_PATH}"
    fi
fi

build_one()
{
    local src_dir=$1
    local flags=$2
    mkdir -p "${src_dir}"
    tar -zxf "${src_dir}.tar.gz" -C "${src_dir}"
    cp src_mock.cpp "${src_dir}/source/src_mock.cpp"
    cp Makefile "${src_dir}/source/Makefile"
    cd "${src_dir}/source" || exit 1
    make CC="${CC:-gcc}" CXX="${CXX:-g++}" \
        CFLAGS="${flags} ${EXTRA_INC}" CXXFLAGS="${flags} ${EXTRA_INC}" \
        build_shared -sj || {
        echo "Error: libog_query build_shared failed in ${src_dir}/source"
        exit 1
    }
    [ -f libog_query.so ] || {
        echo "Error: libog_query.so was not generated"
        exit 1
    }
    cp libog_query.so ../../libog_query.so
}

if [ "$mode" == "A" ]; then
    build_one source_server "-fstack-protector-strong"
elif [ "$mode" == "B" ]; then
    build_one source_dolphin "-DDOLPHIN -fstack-protector-strong"
fi

export LD_LIBRARY_PATH="${_saved_ld}"
