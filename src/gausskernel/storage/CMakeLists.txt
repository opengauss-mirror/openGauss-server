#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/access 
    ${CMAKE_CURRENT_SOURCE_DIR}/bulkload
    ${CMAKE_CURRENT_SOURCE_DIR}/buffer
    ${CMAKE_CURRENT_SOURCE_DIR}/cmgr
    ${CMAKE_CURRENT_SOURCE_DIR}/cstore
    ${CMAKE_CURRENT_SOURCE_DIR}/replication
    ${CMAKE_CURRENT_SOURCE_DIR}/sync
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs
    ${CMAKE_CURRENT_SOURCE_DIR}/file
    ${CMAKE_CURRENT_SOURCE_DIR}/freespace
    ${CMAKE_CURRENT_SOURCE_DIR}/ipc
    ${CMAKE_CURRENT_SOURCE_DIR}/large_object
    ${CMAKE_CURRENT_SOURCE_DIR}/lmgr
    ${CMAKE_CURRENT_SOURCE_DIR}/page
    ${CMAKE_CURRENT_SOURCE_DIR}/remote
    ${CMAKE_CURRENT_SOURCE_DIR}/smgr
    ${CMAKE_CURRENT_SOURCE_DIR}/tcap
    ${CMAKE_CURRENT_SOURCE_DIR}/xlog_share_storage
    ${CMAKE_CURRENT_SOURCE_DIR}/dorado_operation
)

if("${ENABLE_MOT}" STREQUAL "ON")
    list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/mot)
endif()

if("${ENABLE_UT}" STREQUAL "ON")
    list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()

add_subdirectory(access)
add_subdirectory(bulkload)
add_subdirectory(buffer)
add_subdirectory(cmgr)
add_subdirectory(cstore)
add_subdirectory(replication)
add_subdirectory(sync)
add_subdirectory(dfs)
add_subdirectory(file)
add_subdirectory(freespace)
add_subdirectory(ipc)
add_subdirectory(large_object)
add_subdirectory(lmgr)
add_subdirectory(page)
add_subdirectory(remote)
add_subdirectory(smgr)
add_subdirectory(tcap)
add_subdirectory(xlog_share_storage)
add_subdirectory(dorado_operation)

if("${ENABLE_MOT}" STREQUAL "ON")
    add_subdirectory(mot)
endif()

if("${ENABLE_UT}" STREQUAL "ON")
    add_subdirectory(test)
endif()

