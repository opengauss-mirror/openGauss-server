#This is the main CMAKE for set 3rd path components.
set(3RD_PATH $ENV{THIRD_BIN_PATH})
set(VERSION_TYPE $ENV{DEBUG_TYPE})
option(ENABLE_LLT "enable llt, current value is --enable-llt" OFF)
option(ENABLE_UT "enable ut, current value is --enable-ut" OFF)
option(WITH_OPENEULER_OS "Build openGauss rpm package on openEuler os" OFF)

#############################################################################
# get the depend lib path
#    1. libedit, event, libcgroup, kerberos, zlib1.2.11, boost,
#       libxml are support parameter --enable-llt and --enable-ut;
#       $(LIB_SUPPORT_LLT)
#    2. Huawei_Secure_C, gtest, mockcpp, unixodbc, libstd
#       and openssl not support parameter --enable-llt and --enable-ut;
#       $(LIB_UNIFIED_SUPPORT)
#############################################################################
set(SUPPORT_LLT "")
set(JEMALLOC_SUPPORT_LLT "")
if("${ENABLE_LLT}" STREQUAL "ON")
    set(SUPPORT_LLT llt)
    set(JEMALLOC_SUPPORT_LLT _llt)
else()
    if("${ENABLE_UT}" STREQUAL "ON")
        set(SUPPORT_LLT llt)
        set(JEMALLOC_SUPPORT_LLT _llt)
    else()
        set(SUPPORT_LLT comm)
    endif()
endif()
if(${BUILD_TUPLE} STREQUAL "x86_64")
    set(HOST_TUPLE x86_64-pc-linux-gnu)
else()
    set(HOST_TUPLE aarch64-unknown-linux-gnu)
endif()

set(LIB_UNIFIED_SUPPORT comm)
set(MEMCHECK_BUILD_TYPE debug)
set(DEPENDENCY_PATH ${3RD_PATH}/kernel/dependency)
set(PLATFORM_PATH ${3RD_PATH}/kernel/platform)
set(BUILDTOOLS_PATH ${3RD_PATH}/buildtools)
set(COMPONENT_PATH ${3RD_PATH}/kernel/component)

set(CJSON_HOME ${DEPENDENCY_PATH}/cjson/${SUPPORT_LLT})
set(ETCD_HOME ${DEPENDENCY_PATH}/etcd/${LIB_UNIFIED_SUPPORT})
set(EVENT_HOME ${DEPENDENCY_PATH}/event/${LIB_UNIFIED_SUPPORT})
set(FIO_HOME ${DEPENDENCY_PATH}/fio/${SUPPORT_LLT})
set(IPERF_HOME ${DEPENDENCY_PATH}/iperf/${LIB_UNIFIED_SUPPORT})
if("${VERSION_TYPE}" STREQUAL "debug" OR "${VERSION_TYPE}" STREQUAL "memcheck")
    set(JEMALLOC_HOME ${DEPENDENCY_PATH}/jemalloc/debug${JEMALLOC_SUPPORT_LLT})
else()
    set(JEMALLOC_HOME ${DEPENDENCY_PATH}/jemalloc/${VERSION_TYPE}${JEMALLOC_SUPPORT_LLT})
endif()
set(KERBEROS_HOME ${DEPENDENCY_PATH}/kerberos/${SUPPORT_LLT})
set(KMC_HOME ${PLATFORM_PATH}/kmc/${LIB_UNIFIED_SUPPORT})
set(CGROUP_HOME ${DEPENDENCY_PATH}/libcgroup/${SUPPORT_LLT})
set(CURL_HOME ${DEPENDENCY_PATH}/libcurl/${SUPPORT_LLT})
set(EDIT_HOME ${DEPENDENCY_PATH}/libedit/${SUPPORT_LLT})
set(OBS_HOME ${DEPENDENCY_PATH}/libobs/${LIB_UNIFIED_SUPPORT})
set(XML2_HOME ${DEPENDENCY_PATH}/libxml2/${SUPPORT_LLT})
set(LLVM_HOME ${DEPENDENCY_PATH}/llvm/${LIB_UNIFIED_SUPPORT})
set(LZ4_HOME ${DEPENDENCY_PATH}/lz4/${SUPPORT_LLT})
set(NANOMSG_HOME ${DEPENDENCY_PATH}/nng/${LIB_UNIFIED_SUPPORT})
set(NCURSES_HOME ${DEPENDENCY_PATH}/ncurses/${SUPPORT_LLT})
if(($ENV{WITH_TASSL}) STREQUAL "YES")
    set(OPENSSL_HOME ${DEPENDENCY_PATH}/tassl/${LIB_UNIFIED_SUPPORT})
else()
    set(OPENSSL_HOME ${DEPENDENCY_PATH}/openssl/${LIB_UNIFIED_SUPPORT})
endif()

set(PLJAVA_HOME ${DEPENDENCY_PATH}/pljava/${LIB_UNIFIED_SUPPORT})
if (EXISTS "${PLATFORM_PATH}/openjdk8/${BUILD_TUPLE}/jdk")
  set(JAVA_HOME ${PLATFORM_PATH}/openjdk8/${BUILD_TUPLE}/jdk)
else()
  set(JAVA_HOME ${PLATFORM_PATH}/huaweijdk8/${BUILD_TUPLE}/jdk)
endif()
set(ZLIB_HOME ${DEPENDENCY_PATH}/zlib1.2.11/${SUPPORT_LLT})
set(XGBOOST_HOME ${DEPENDENCY_PATH}/xgboost/${SUPPORT_LLT})
set(ZSTD_HOME ${DEPENDENCY_PATH}/zstd)
set(LICENSE_HOME ${PLATFORM_PATH}/AdaptiveLM_C_V100R005C01SPC002/${SUPPORT_LLT})
set(HOTPATCH_HOME ${PLATFORM_PATH}/hotpatch)
set(SECURE_HOME ${PLATFORM_PATH}/Huawei_Secure_C/${LIB_UNIFIED_SUPPORT})
set(SECUREDYNAMICLIB_HOME ${PLATFORM_PATH}/Huawei_Secure_C/Dynamic_Lib)
set(DCF_HOME ${COMPONENT_PATH}/dcf)
set(DMS_HOME ${COMPONENT_PATH}/dms)
set(DSS_HOME ${COMPONENT_PATH}/dss)

set(MOCKCPP_HOME ${BUILDTOOLS_PATH}/mockcpp/${LIB_UNIFIED_SUPPORT})
set(GTEST_HOME ${BUILDTOOLS_PATH}/gtest/${LIB_UNIFIED_SUPPORT})
set(MASSTREE_HOME ${BUILDTOOLS_PATH}/masstree/${LIB_UNIFIED_SUPPORT})
set(NUMA_HOME ${DEPENDENCY_PATH}/numactl/${SUPPORT_LLT})
set(BOOST_HOME ${DEPENDENCY_PATH}/boost/${SUPPORT_LLT})
set(ODBC_HOME ${DEPENDENCY_PATH}/unixodbc)
set(MASSTREE_HOME ${DEPENDENCY_PATH}/masstree/${LIB_UNIFIED_SUPPORT})
set(LCOV_HOME ${BUILDTOOLS_PATH}/gcc${GCC_VERSION_LIT}/gcc/lib/gcc/${HOST_TUPLE})
set(GCC_LIB_PATH $ENV{GCC_INSTALL_HOME})
set(MEMCHECK_LIB_PATH $ENV{GCC_INSTALL_HOME}/lib64/)
if("${GCC_LIB_PATH}" STREQUAL "")
    set(GCC_LIB_PATH ${BUILDTOOLS_PATH}/gcc${GCC_VERSION_LIT}/gcc)
    set(MEMCHECK_HOME ${DEPENDENCY_PATH}/memcheck/${MEMCHECK_BUILD_TYPE})
    set(MEMCHECK_LIB_PATH ${MEMCHECK_HOME}/gcc${GCC_VERSION}/lib/)
endif()

#############################################################################
# lcov
#############################################################################
set(LCOV_LIB_PATH ${LCOV_HOME}/${GCC_VERSION})

#############################################################################
# boost component
#############################################################################
set(BOOST_INCLUDE_PATH ${BOOST_HOME}/include)
set(BOOST_LIB_PATH ${BOOST_HOME}/lib)

#############################################################################
# cjson component
#############################################################################
set(CJSON_INCLUDE_PATH ${CJSON_HOME}/include)
set(CJSON_LIB_PATH ${CJSON_HOME}/lib)

#############################################################################
# etcd component
#############################################################################
set(ETCD_INCLUDE_PATH ${ETCD_HOME}/include)
set(ETCD_LIB_PATH ${ETCD_HOME}/lib)
set(ETCD_BIN_PATH ${ETCD_HOME}/bin)

#############################################################################
# event component
#############################################################################
set(EVENT_INCLUDE_PATH ${EVENT_HOME}/include)
set(EVENT_LIB_PATH ${EVENT_HOME}/lib)

#############################################################################
# fio component
#############################################################################
set(LIBFIO_INCLUDE_PATH ${FIO_HOME}/include)
set(LIBFIO_LIB_PATH ${FIO_HOME}/lib)
set(LIBFIO_BIN_PATH ${FIO_HOME}/bin)

#############################################################################
# iperf component
#############################################################################
set(IPERF_INCLUDE_PATH ${IPERF_HOME}/include)
set(IPERF_LIB_PATH ${IPERF_HOME}/lib)
set(IPERF_BIN_PATH ${IPERF_HOME}/bin)

#############################################################################
# jemalloc component
#############################################################################
set(JEMALLOC_INCLUDE_PATH ${JEMALLOC_HOME}/include)
set(JEMALLOC_LIB_PATH ${JEMALLOC_HOME}/lib)
set(JEMALLOC_BIN_PATH ${JEMALLOC_HOME}/bin)
set(JEMALLOC_SHARE_PATH ${JEMALLOC_HOME}/share)
if(${ENABLE_LLT} STREQUAL "ON")
    set(JEMALLOC_LIB_NAME jemalloc_pic)
else()
    if("${ENABLE_UT}" STREQUAL "ON")
        set(JEMALLOC_LIB_NAME jemalloc_pic)
    else()
        set(JEMALLOC_LIB_NAME jemalloc)
    endif()
endif()

if(${WITH_OPENEULER_OS} STREQUAL "ON")
    set(SECURE_C_CHECK boundscheck)
else()
    set(SECURE_C_CHECK securec)
endif()

#############################################################################
# kerberos component
#############################################################################
set(KERBEROS_BIN_PATH ${KERBEROS_HOME}/bin)
set(KERBEROS_SBIN_PATH ${KERBEROS_HOME}/sbin)
set(KERBEROS_INCLUDE_PATH ${KERBEROS_HOME}/include)
set(KERBEROS_LIB_PATH ${KERBEROS_HOME}/lib)

#############################################################################
# kmc component
#############################################################################
set(KMC_INCLUDE_PATH ${KMC_HOME}/include)
set(KMC_LIB_PATH ${KMC_HOME}/lib)

#############################################################################
# cgroup component
#############################################################################
set(LIBCGROUP_INCLUDE_PATH ${CGROUP_HOME}/include)
set(LIBCGROUP_LIB_PATH ${CGROUP_HOME}/lib)

#############################################################################
# curl component
#############################################################################
set(LIBCURL_INCLUDE_PATH ${CURL_HOME}/include)
set(LIBCURL_LIB_PATH ${CURL_HOME}/lib)

#############################################################################
# edit component
#############################################################################
set(LIBEDIT_INCLUDE_PATH ${EDIT_HOME}/include)
set(LIBEDIT_LIB_PATH ${EDIT_HOME}/lib)

#############################################################################
# obs component
#############################################################################
set(LIBOBS_INCLUDE_PATH ${OBS_HOME}/include)
set(LIBOBS_LIB_PATH ${OBS_HOME}/lib)

#############################################################################
# xml2 component
#############################################################################
set(LIBXML_INCLUDE_PATH ${XML2_HOME}/include)
set(LIBXML_LIB_PATH ${XML2_HOME}/lib)

#############################################################################
# llvm component
#############################################################################
set(LIBLLVM_BIN_PATH ${LLVM_HOME}/bin)
set(LIBLLVM_INCLUDE_PATH ${LLVM_HOME}/include)
set(LIBLLVM_LIB_PATH ${LLVM_HOME}/lib)
set(LLVM_CONFIG ${LIBLLVM_BIN_PATH}/llvm-config)

#############################################################################
# lz4 component
#############################################################################
set(LZ4_INCLUDE_PATH ${LZ4_HOME}/include)
set(LZ4_LIB_PATH ${LZ4_HOME}/lib)
set(LZ4_BIN_PATH ${LZ4_HOME}/bin)

#############################################################################
# nanomsg component
#############################################################################
set(LIBNANOMSG_INCLUDE_PATH ${NANOMSG_HOME}/include)
set(LIBNANOMSG_LIB_PATH ${NANOMSG_HOME}/lib)

#############################################################################
# ncurses component
#############################################################################
set(NCURSES_INCLUDE_PATH ${NCURSES_HOME}/include)
set(NCURSES_LIB_PATH ${NCURSES_HOME}/lib)

#############################################################################
# openssl component
#############################################################################
set(LIBOPENSSL_BIN_PATH ${OPENSSL_HOME}/bin)
set(LIBOPENSSL_LIB_PATH ${OPENSSL_HOME}/lib)
set(LIBOPENSSL_SSL_PATH ${OPENSSL_HOME}/ssl)
set(LIBOPENSSL_INCLUDE_PATH ${OPENSSL_HOME}/include)

#############################################################################
# zlib component
#############################################################################
set(ZLIB_INCLUDE_PATH ${ZLIB_HOME}/include)
set(ZLIB_LIB_PATH ${ZLIB_HOME}/lib)

#############################################################################
# xgboost component
#############################################################################
set(XGBOOST_INCLUDE_PATH ${XGBOOST_HOME}/include)
set(XGBOOST_LIB_PATH ${XGBOOST_HOME}/lib64)

#############################################################################
# zstd component
#############################################################################
set(ZSTD_INCLUDE_PATH ${ZSTD_HOME}/include)
set(ZSTD_LIB_PATH ${ZSTD_HOME}/lib)

#############################################################################
# dcf component
#############################################################################
set(DCF_INCLUDE_PATH ${DCF_HOME}/include)
set(DCF_LIB_PATH ${DCF_HOME}/lib)

#############################################################################
# dms component
#############################################################################
set(DMS_LIB_PATH ${DMS_HOME}/lib)

#############################################################################
# dss component
#############################################################################
set(DSS_LIB_PATH ${DSS_HOME}/lib)
set(DSS_BIN_PATH ${DSS_HOME}/bin)

#############################################################################
# license manager compnent
#############################################################################
set(LICENSE_INCLUDE_PATH ${LICENSE_HOME}/include)
set(LICENSE_LIB_PATH ${LICENSE_HOME}/lib)

#############################################################################
# hotpatch component
#############################################################################
set(LIBHOTPATCH_INCLUDE_PATH ${HOTPATCH_HOME}/include)
set(LIBHOTPATCH_LIB_PATH ${HOTPATCH_HOME}/lib)
set(LIBHOTPATCH_TOOL_PATH ${HOTPATCH_HOME}/tool)
set(LIBHOTPATCH_CONFIG_PATH ${HOTPATCH_HOME}/config)

#############################################################################
# secure component
#############################################################################
set(SECURE_INCLUDE_PATH ${SECURE_HOME}/include)
set(SECURE_LIB_PATH ${SECURE_HOME}/lib)

#############################################################################
# numa component
#############################################################################
set(NUMA_INCLUDE_PATH ${NUMA_HOME}/include)
set(NUMA_LIB_PATH ${NUMA_HOME}/lib)

#############################################################################
# odbc component
#############################################################################
set(LIBODBC_INCLUDE_PATH ${ODBC_HOME}/include)
set(LIBODBC_LIB_PATH ${ODBC_HOME}/lib)
set(LIBODBC_BIN_PATH ${ODBC_HOME}/bin)
set(LIBODBC_SHARE_PATH ${ODBC_HOME}/share)

############################################################################
# gtest component
############################################################################
set(GTEST_INCLUDE_PATH ${GTEST_HOME}/include)
set(GTEST_LIB_PATH ${GTEST_HOME}/lib)

############################################################################
# mockcpp component
############################################################################
set(MOCKCPP_INCLUDE_PATH ${MOCKCPP_HOME}/include)
set(MOCKCPP_LIB_PATH ${MOCKCPP_HOME}/lib)
set(MOCKCPP_3RDPARTY_PATH ${MOCKCPP_HOME}/3rdparty)

#############################################################################
# masstree component
#############################################################################
set(MASSTREE_INCLUDE_PATH ${MASSTREE_HOME}/include)
set(MASSTREE_LIB_PATH ${MASSTREE_HOME}/lib)

############################################################################
# gtest component
############################################################################
set(GTEST_INCLUDE_PATH ${GTEST_HOME}/include)
set(GTEST_LIB_PATH ${GTEST_HOME}/lib)

############################################################################
# mockcpp component
############################################################################
set(MOCKCPP_INCLUDE_PATH ${MOCKCPP_HOME}/include)
set(MOCKCPP_LIB_PATH ${MOCKCPP_HOME}/lib)
set(MOCKCPP_3RDPARTY_PATH ${MOCKCPP_HOME}/3rdparty)
