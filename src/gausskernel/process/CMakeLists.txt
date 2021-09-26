#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH 
    ${CMAKE_CURRENT_SOURCE_DIR}/datasource
    ${CMAKE_CURRENT_SOURCE_DIR}/globalplancache
    ${CMAKE_CURRENT_SOURCE_DIR}/job
    ${CMAKE_CURRENT_SOURCE_DIR}/main
    ${CMAKE_CURRENT_SOURCE_DIR}/postmaster
    ${CMAKE_CURRENT_SOURCE_DIR}/stream
    ${CMAKE_CURRENT_SOURCE_DIR}/tcop
    ${CMAKE_CURRENT_SOURCE_DIR}/threadpool
)

add_subdirectory(datasource)
add_subdirectory(globalplancache)
add_subdirectory(job)
add_subdirectory(main)
add_subdirectory(postmaster)
add_subdirectory(stream)
add_subdirectory(tcop)
add_subdirectory(threadpool)
