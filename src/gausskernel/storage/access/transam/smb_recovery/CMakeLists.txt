#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_smb_recovery_SRC)

set(TGT_smb_recovery_INC 
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

set(smb_recovery_DEF_OPTIONS ${MACRO_OPTIONS})
set(smb_recovery_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(smb_recovery_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_storage_access_transam_smb_recovery TGT_smb_recovery_SRC TGT_smb_recovery_INC "${smb_recovery_DEF_OPTIONS}" 
    "${smb_recovery_COMPILE_OPTIONS}" "${smb_recovery_LINK_OPTIONS}")