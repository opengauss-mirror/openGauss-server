#This is the main CMAKE for build bin.
list(APPEND TGT_dfs_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/common_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_am.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_delete.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfsdesc.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_insert.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_parinsert.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_query_check.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_query.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_query_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_stream_factory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_update.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dfs_uri.cpp
)

set(TGT_dfs_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH} 
)

set(dfs_DEF_OPTIONS ${MACRO_OPTIONS})
set(dfs_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(dfs_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_dfs TGT_dfs_SRC TGT_dfs_INC "${dfs_DEF_OPTIONS}" "${dfs_COMPILE_OPTIONS}" "${dfs_LINK_OPTIONS}")


set(CMAKE_MODULE_PATH
    ${CMAKE_CURRENT_SOURCE_DIR}/carbondata 
    ${CMAKE_CURRENT_SOURCE_DIR}/csv
    ${CMAKE_CURRENT_SOURCE_DIR}/text
)

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    list(APPEND CMAKE_MODULE_PATH
        ${CMAKE_CURRENT_SOURCE_DIR}/orc
        ${CMAKE_CURRENT_SOURCE_DIR}/parquet
    )
    add_subdirectory(orc)
    add_subdirectory(parquet)
endif()

if("${ENABLE_MULTIPLE_NODES}" STREQUAL "ON")
    add_subdirectory(carbondata)
endif()
add_subdirectory(csv)
add_subdirectory(text)

install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/region_map
    DESTINATION etc/
)
