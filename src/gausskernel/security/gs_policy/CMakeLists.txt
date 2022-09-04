#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gs_policy_SRC)

set(TGT_gs_policy_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH} 
)

set(gs_policy_DEF_OPTIONS ${MACRO_OPTIONS})
set(gs_policy_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gs_policy_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_security_gs_policy TGT_gs_policy_SRC TGT_gs_policy_INC "${gs_policy_DEF_OPTIONS}" "${gs_policy_COMPILE_OPTIONS}" "${gs_policy_LINK_OPTIONS}")

