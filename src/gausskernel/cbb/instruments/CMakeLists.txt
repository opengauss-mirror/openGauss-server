#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

INCLUDE_DIRECTORIES(${LIBTHRIFT_INCLUDE_PATH} ${SNAPPY_INCLUDE_PATH} ${CJSON_INCLUDE_PATH} ${BOOST_INCLUDE_PATH})

set(CMAKE_MODULE_PATH 
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/ash
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/capture_view
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/event
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/slow_query
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/statement
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/unique_sql
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/user
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/utils
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/wdr
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/workload
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/percentile
    ${PROJECT_OPENGS_DIR}/src/gausskernel/cbb/instruments/func_control

)

add_subdirectory(ash)
add_subdirectory(capture_view)
add_subdirectory(event)
add_subdirectory(slow_query)
add_subdirectory(statement)
add_subdirectory(unique_sql)
add_subdirectory(user)
add_subdirectory(utils)
add_subdirectory(wdr)
add_subdirectory(workload)
add_subdirectory(percentile)
add_subdirectory(func_control)

