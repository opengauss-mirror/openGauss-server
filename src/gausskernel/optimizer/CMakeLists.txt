#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/commands
    ${CMAKE_CURRENT_SOURCE_DIR}/geqo
    ${CMAKE_CURRENT_SOURCE_DIR}/path
    ${CMAKE_CURRENT_SOURCE_DIR}/plan
    ${CMAKE_CURRENT_SOURCE_DIR}/prep
    ${CMAKE_CURRENT_SOURCE_DIR}/rewrite
    ${CMAKE_CURRENT_SOURCE_DIR}/util
    ${CMAKE_CURRENT_SOURCE_DIR}/sqladvisor
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlpatch
    ${CMAKE_CURRENT_SOURCE_DIR}/statistics
    ${CMAKE_CURRENT_SOURCE_DIR}/aioptimizer
    ${CMAKE_CURRENT_SOURCE_DIR}/gplanmgr
)

add_subdirectory(commands)
add_subdirectory(geqo)
add_subdirectory(path)
add_subdirectory(plan)
add_subdirectory(prep)
add_subdirectory(rewrite)
add_subdirectory(util)
add_subdirectory(sqladvisor)
add_subdirectory(sqlpatch)
if("${ENABLE_MULTIPLE_NODES}" STREQUAL "OFF")
    add_subdirectory(statistics)
endif()
add_subdirectory(aioptimizer)
add_subdirectory(gplanmgr) 
