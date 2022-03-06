#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)


set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/barrier
    ${CMAKE_CURRENT_SOURCE_DIR}/copy
    ${CMAKE_CURRENT_SOURCE_DIR}/locator
    ${CMAKE_CURRENT_SOURCE_DIR}/nodemgr
    ${CMAKE_CURRENT_SOURCE_DIR}/pool
)

add_subdirectory(barrier)
add_subdirectory(copy)

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    add_subdirectory(locator)
    add_subdirectory(nodemgr)
    add_subdirectory(pool)
endif()

