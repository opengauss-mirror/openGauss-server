#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

INCLUDE_DIRECTORIES(${LIBTHRIFT_INCLUDE_PATH} ${SNAPPY_INCLUDE_PATH} ${CJSON_INCLUDE_PATH} ${BOOST_INCLUDE_PATH})

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_cgroup
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_guc
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_log
    ${CMAKE_CURRENT_SOURCE_DIR}/gsqlerr
    ${CMAKE_CURRENT_SOURCE_DIR}/initdb
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_basebackup
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_config
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_controldata
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_ctl
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_dump
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_probackup
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_resetxlog
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_upgrade
    ${CMAKE_CURRENT_SOURCE_DIR}/pgxc_clean
    ${CMAKE_CURRENT_SOURCE_DIR}/psql
    ${CMAKE_CURRENT_SOURCE_DIR}/scripts
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_rewind
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_plan_simulator
    ${CMAKE_CURRENT_SOURCE_DIR}/gs_loader
)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
    add_subdirectory(gs_cgroup)
    add_subdirectory(gs_log)
    add_subdirectory(pg_upgrade)
    add_subdirectory(pgxc_clean)
    add_subdirectory(scripts)
    add_subdirectory(gs_plan_simulator)
endif()

add_subdirectory(initdb)
add_subdirectory(pg_ctl)
add_subdirectory(pg_rewind)
add_subdirectory(pg_dump)
add_subdirectory(psql)
add_subdirectory(pg_config)
add_subdirectory(pg_controldata)
add_subdirectory(pg_resetxlog)
add_subdirectory(gs_guc)
add_subdirectory(gsqlerr)
add_subdirectory(pg_basebackup)
add_subdirectory(pg_probackup)
add_subdirectory(gs_loader)
