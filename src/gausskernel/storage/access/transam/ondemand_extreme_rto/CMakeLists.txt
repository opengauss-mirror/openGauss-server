#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_ondemand_extreme_rto_SRC)

set(TGT_ondemand_extreme_rto_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/iprange
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${ZLIB_INCLUDE_PATH}
    ${LIBCURL_INCLUDE_PATH}
    ${DCF_INCLUDE_PATH}
)

set(ondemand_extreme_rto_DEF_OPTIONS ${MACRO_OPTIONS})
set(ondemand_extreme_rto_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(ondemand_extreme_rto_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_transam_ondemand_extreme_rto TGT_ondemand_extreme_rto_SRC TGT_ondemand_extreme_rto_INC "${ondemand_extreme_rto_DEF_OPTIONS}" 
    "${ondemand_extreme_rto_COMPILE_OPTIONS}" "${ondemand_extreme_rto_LINK_OPTIONS}")

