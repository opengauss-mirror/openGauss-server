#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_nbtree_SRC)

set(TGT_nbtree_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH} 
)

set(nbtree_DEF_OPTIONS ${MACRO_OPTIONS})
set(nbtree_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(nbtree_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_nbtree TGT_nbtree_SRC TGT_nbtree_INC "${nbtree_DEF_OPTIONS}" "${nbtree_COMPILE_OPTIONS}" "${nbtree_LINK_OPTIONS}")

