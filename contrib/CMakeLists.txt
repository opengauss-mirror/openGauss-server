#This is the main CMAKE for build contrib.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

INCLUDE_DIRECTORIES(${CJSON_INCLUDE_PATH} ${BOOST_INCLUDE_PATH})

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/gsredistribute
    ${CMAKE_CURRENT_SOURCE_DIR}/hstore
    ${CMAKE_CURRENT_SOURCE_DIR}/test_decoding
    ${CMAKE_CURRENT_SOURCE_DIR}/mppdb_decoding
    ${CMAKE_CURRENT_SOURCE_DIR}/sql_decoding
    ${CMAKE_CURRENT_SOURCE_DIR}/spi
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_upgrade_support
    ${CMAKE_CURRENT_SOURCE_DIR}/postgres_fdw
    ${CMAKE_CURRENT_SOURCE_DIR}/security_plugin
    ${CMAKE_CURRENT_SOURCE_DIR}/dummy_seclabel
    ${CMAKE_CURRENT_SOURCE_DIR}/pagehack
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_xlogdump
    ${CMAKE_CURRENT_SOURCE_DIR}/file_fdw
    ${CMAKE_CURRENT_SOURCE_DIR}/log_fdw
    ${CMAKE_CURRENT_SOURCE_DIR}/gc_fdw
    ${CMAKE_CURRENT_SOURCE_DIR}/ndpplugin
    ${CMAKE_CURRENT_SOURCE_DIR}/spq_plugin
)

add_subdirectory(hstore)
add_subdirectory(test_decoding)
add_subdirectory(mppdb_decoding)
add_subdirectory(sql_decoding)
add_subdirectory(spi)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON" OR "${ENABLE_PRIVATEGAUSS}" STREQUAL "ON")
    add_subdirectory(pg_upgrade_support)
endif()
add_subdirectory(postgres_fdw)
add_subdirectory(security_plugin)
add_subdirectory(dummy_seclabel)
add_subdirectory(pagehack)
add_subdirectory(pg_xlogdump)
add_subdirectory(file_fdw)
add_subdirectory(log_fdw)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    add_subdirectory(gc_fdw)
endif()
add_subdirectory(ndpplugin)
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/spq_plugin)
    add_subdirectory(spq_plugin)
endif()
