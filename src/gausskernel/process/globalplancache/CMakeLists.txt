#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_globalplancache_SRC)

set(TGT_globalplancache_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(globalplancache_DEF_OPTIONS ${MACRO_OPTIONS})
set(globalplancache_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(globalplancache_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_process_globalplancache TGT_globalplancache_SRC TGT_globalplancache_INC "${globalplancache_DEF_OPTIONS}" "${globalplancache_COMPILE_OPTIONS}" "${globalplancache_LINK_OPTIONS}")

