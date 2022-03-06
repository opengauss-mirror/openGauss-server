#This is the main CMAKE for build bin.

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RULE_MESSAGES OFF)
set(CMAKE_SKIP_RPATH TRUE)

set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/cbtree 
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs
    ${CMAKE_CURRENT_SOURCE_DIR}/gin
    ${CMAKE_CURRENT_SOURCE_DIR}/gist
    ${CMAKE_CURRENT_SOURCE_DIR}/hash
    ${CMAKE_CURRENT_SOURCE_DIR}/hbstore
    ${CMAKE_CURRENT_SOURCE_DIR}/heap
    ${CMAKE_CURRENT_SOURCE_DIR}/index
    ${CMAKE_CURRENT_SOURCE_DIR}/nbtree 
    ${CMAKE_CURRENT_SOURCE_DIR}/obs
    ${CMAKE_CURRENT_SOURCE_DIR}/psort 
    ${CMAKE_CURRENT_SOURCE_DIR}/redo
    ${CMAKE_CURRENT_SOURCE_DIR}/rmgrdesc
    ${CMAKE_CURRENT_SOURCE_DIR}/spgist 
    ${CMAKE_CURRENT_SOURCE_DIR}/table
    ${CMAKE_CURRENT_SOURCE_DIR}/transam 
    ${CMAKE_CURRENT_SOURCE_DIR}/ubtree 
    ${CMAKE_CURRENT_SOURCE_DIR}/ustore 
)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND CMAKE_MODULE_PATH
        ${CMAKE_CURRENT_SOURCE_DIR}/archive
    )
    add_subdirectory(archive)
endif()

add_subdirectory(cbtree)
add_subdirectory(common)
add_subdirectory(dfs)
add_subdirectory(gin)
add_subdirectory(gist)
add_subdirectory(hash)
add_subdirectory(hbstore)
add_subdirectory(heap)
add_subdirectory(index)
add_subdirectory(nbtree)
add_subdirectory(obs)
add_subdirectory(psort)
add_subdirectory(redo)
add_subdirectory(rmgrdesc)
add_subdirectory(spgist)
add_subdirectory(table)
add_subdirectory(transam)
add_subdirectory(ubtree)
add_subdirectory(ustore)
