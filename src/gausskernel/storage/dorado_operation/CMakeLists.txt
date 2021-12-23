#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_dorado_operation_SRC)

set(TGT_dorado_operation_INC 
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
    ${LIBOPENSSL_INCLUDE_PATH}
)

set(dorado_operation_DEF_OPTIONS ${MACRO_OPTIONS})
set(dorado_operation_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(dorado_operation_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_dorado_operation TGT_dorado_operation_SRC TGT_dorado_operation_INC "${dorado_operation_DEF_OPTIONS}" "${dorado_operation_COMPILE_OPTIONS}" "${dorado_operation_LINK_OPTIONS}")

