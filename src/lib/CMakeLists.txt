#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

INCLUDE_DIRECTORIES(${LIBTHRIFT_INCLUDE_PATH} ${SNAPPY_INCLUDE_PATH} ${CJSON_INCLUDE_PATH} ${BOOST_INCLUDE_PATH})

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/gstrace
    ${CMAKE_CURRENT_SOURCE_DIR}/elog
    ${CMAKE_CURRENT_SOURCE_DIR}/hotpatch
    ${CMAKE_CURRENT_SOURCE_DIR}/alarm
    ${CMAKE_CURRENT_SOURCE_DIR}/build_query
    ${CMAKE_CURRENT_SOURCE_DIR}/config
    ${CMAKE_CURRENT_SOURCE_DIR}/page_compression
    ${CMAKE_CURRENT_SOURCE_DIR}/pgcommon
    ${CMAKE_CURRENT_SOURCE_DIR}/cm_common
    ${CMAKE_CURRENT_SOURCE_DIR}/cm_communication
)

add_subdirectory(gstrace)
add_subdirectory(elog)
add_subdirectory(hotpatch)
add_subdirectory(alarm)
add_subdirectory(build_query)
add_subdirectory(config)
add_subdirectory(page_compression)
add_subdirectory(pgcommon)
add_subdirectory(cm_common)
add_subdirectory(cm_communication)

