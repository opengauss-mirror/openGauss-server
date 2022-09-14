#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gplanmgr_SRC)

set(TGT_gplanmgr_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBOPENSSL_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
)

set(gplanmgr_DEF_OPTIONS ${MACRO_OPTIONS} -DOBS_SERVER)
set(gplanmgr_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gplanmgr_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_optimizer_gplanmgr TGT_gplanmgr_SRC TGT_gplanmgr_INC "${gplanmgr_DEF_OPTIONS}" "${gplanmgr_COMPILE_OPTIONS}" "${gplanmgr_LINK_OPTIONS}")
