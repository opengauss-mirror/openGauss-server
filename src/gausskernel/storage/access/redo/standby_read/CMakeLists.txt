#This is the main CMAKE for build bin.
 
 
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_standby_read_SRC)
 
set(TGT_standby_read_INC 
    ${PROJECT_SRC_DIR}/tools/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH} 
    ${LZ4_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
)
 
set(standby_read_DEF_OPTIONS ${MACRO_OPTIONS})
set(standby_read_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(standby_read_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_redo_standby_read TGT_standby_read_SRC TGT_standby_read_INC "${standby_read_DEF_OPTIONS}" "${standby_read_COMPILE_OPTIONS}" "${standby_read_LINK_OPTIONS}")
