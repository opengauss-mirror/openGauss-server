#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_tde_key_management_SRC)

set(TGT_tde_key_management_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH} 
    ${LIBOPENSSL_INCLUDE_PATH}
)

set(tde_key_management_DEF_OPTIONS ${MACRO_OPTIONS})
set(tde_key_management_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(tde_key_management_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_security_tde_key_management TGT_tde_key_management_SRC TGT_tde_key_management_INC "${tde_key_management_DEF_OPTIONS}" "${tde_key_management_COMPILE_OPTIONS}" "${tde_key_management_LINK_OPTIONS}")
