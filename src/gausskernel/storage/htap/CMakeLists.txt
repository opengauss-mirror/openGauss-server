#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_htap_SRC)

set(TGT_htap_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_OPENGS_DIR}/contrib/ndpplugin
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${PROJECT_SRC_DIR}/lib/page_compression
    ${PROJECT_SRC_DIR}/bin/pg_rewind
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(htap_DEF_OPTIONS ${MACRO_OPTIONS})
set(htap_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(htap_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_htap TGT_htap_SRC TGT_htap_INC "${htap_DEF_OPTIONS}" "${htap_COMPILE_OPTIONS}" "${htap_LINK_OPTIONS}")