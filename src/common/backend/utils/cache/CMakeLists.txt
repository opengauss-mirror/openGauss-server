#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_cache_SRC)

set(TGT_cache_INC 
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

set(cache_DEF_OPTIONS ${MACRO_OPTIONS})
set(cache_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(cache_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_utils_cache TGT_cache_SRC TGT_cache_INC "${cache_DEF_OPTIONS}" "${cache_COMPILE_OPTIONS}" "${cache_LINK_OPTIONS}")
