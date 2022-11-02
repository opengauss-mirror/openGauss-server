#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_ipc_SRC)

set(TGT_ipc_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${DCF_INCLUDE_PATH}
)

set(ipc_DEF_OPTIONS ${MACRO_OPTIONS})
set(ipc_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(ipc_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_ipc TGT_ipc_SRC TGT_ipc_INC "${ipc_DEF_OPTIONS}" "${ipc_COMPILE_OPTIONS}" "${ipc_LINK_OPTIONS}")

