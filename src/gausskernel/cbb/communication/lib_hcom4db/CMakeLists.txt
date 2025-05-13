#This is the main CMAKE for build object.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_hcom_4db_SRC)

set(TGT_hcom_4db_INC
        ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
        ${PROJECT_SRC_DIR}/include/libcomm
        ${PROJECT_SRC_DIR}/include
        ${PROJECT_SRC_DIR}/lib/gstrace
        ${LIBCGROUP_INCLUDE_PATH}
        ${ZLIB_INCLUDE_PATH}
        ${LIBOPENSSL_INCLUDE_PATH}
)

set(hcom_4db_DEF_OPTIONS ${MACRO_OPTIONS})
set(hcom_4db_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(hcom_4db_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_cbb_communication_lib_hcom4db TGT_hcom_4db_SRC TGT_hcom_4db_INC "${hcom_4db_DEF_OPTIONS}" "${hcom_4db_COMPILE_OPTIONS}" "${hcom_4db_LINK_OPTIONS}")

