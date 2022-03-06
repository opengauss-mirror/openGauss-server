#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_dfs_SRC)

set(TGT_dfs_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(dfs_DEF_OPTIONS ${MACRO_OPTIONS})
set(dfs_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(dfs_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_dfs TGT_dfs_SRC TGT_dfs_INC "${dfs_DEF_OPTIONS}" "${dfs_COMPILE_OPTIONS}" "${dfs_LINK_OPTIONS}")

if(NOT "${ENABLE_LITE_MODE}" STREQUAL "ON")
    set(CMAKE_MODULE_PATH
        ${CMAKE_CURRENT_SOURCE_DIR}/obs
    )

    add_subdirectory(obs)
endif()
