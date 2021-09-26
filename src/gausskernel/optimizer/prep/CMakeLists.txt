#This is the main CMAKE for build bin.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_prep_SRC)

set(TGT_prep_INC 
    ${PROJECT_SRC_DIR}/gausskernel/cbb/communication
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LIBCGROUP_INCLUDE_PATH}
    ${PROJECT_SRC_DIR}/include/libcomm
    ${LIBORC_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${LIBPARQUET_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(prep_DEF_OPTIONS ${MACRO_OPTIONS})
set(prep_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(prep_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(gausskernel_optimizer_prep TGT_prep_SRC TGT_prep_INC "${prep_DEF_OPTIONS}" "${prep_COMPILE_OPTIONS}" "${prep_LINK_OPTIONS}")

