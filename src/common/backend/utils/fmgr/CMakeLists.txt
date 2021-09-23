#This is the main CMAKE for build all components.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_fmgr_SRC)

set(TGT_fmgr_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_OPENGS_DIR}/contrib/gc_fdw
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

set(fmgr_DEF_OPTIONS ${MACRO_OPTIONS} -DDLSUFFIX=\".so\")
set(fmgr_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(fmgr_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_utils_fmgr TGT_fmgr_SRC TGT_fmgr_INC "${fmgr_DEF_OPTIONS}" "${fmgr_COMPILE_OPTIONS}" "${fmgr_LINK_OPTIONS}")
