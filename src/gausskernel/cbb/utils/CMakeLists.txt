#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/aes
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/biginteger
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/bloomfilter
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/debug
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/gssignal
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/hll
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/partition
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/ssl
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/utils/zfiles
)

add_subdirectory(aes)
add_subdirectory(biginteger)
add_subdirectory(bloomfilter)
add_subdirectory(debug)
add_subdirectory(gssignal)
add_subdirectory(hll)
add_subdirectory(partition)
add_subdirectory(ssl)
add_subdirectory(zfiles)
